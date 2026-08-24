const express = require('express');
const crypto = require('crypto');
const path = require('path');
const fs = require('fs');
const http = require('http');
const multer = require('multer');
const { WebSocketServer } = require('ws');
const { EventEmitter } = require('events');
const { JsonDB } = require('./db');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });
const PORT = process.env.PORT || 3001;
const DB_PATH = process.env.DB_PATH || path.join(__dirname, '..', 'data', 'app.db');
const ATTACH_DIR = path.join(__dirname, '..', 'data', 'attachments');
const DMS_DIR = path.join(__dirname, '..', 'data', 'dms');
const db = new JsonDB(DB_PATH);

// Ensure DMS directories exist
['uploads', 'versions', 'thumbnails'].forEach(sub => {
  const dir = path.join(DMS_DIR, sub);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
});

const upload = multer({
  storage: multer.diskStorage({
    destination: (req, file, cb) => {
      const folder = req.params.folder || 'settings';
      const dest = path.join(ATTACH_DIR, folder);
      if (!fs.existsSync(dest)) fs.mkdirSync(dest, { recursive: true });
      cb(null, dest);
    },
    filename: (req, file, cb) => {
      const ext = path.extname(file.originalname);
      cb(null, `${Date.now()}-${crypto.randomBytes(8).toString('hex')}${ext}`);
    }
  }),
  limits: { fileSize: 25 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    const allowed = /jpeg|jpg|png|gif|pdf|doc|docx|xls|xlsx|csv|txt|zip/;
    const ok = allowed.test(path.extname(file.originalname).toLowerCase()) && allowed.test(file.mimetype.split('/').pop());
    cb(null, !!ok);
  }
});

const dmsUpload = multer({
  storage: multer.diskStorage({
    destination: (req, file, cb) => {
      const dest = path.join(DMS_DIR, 'uploads');
      if (!fs.existsSync(dest)) fs.mkdirSync(dest, { recursive: true });
      cb(null, dest);
    },
    filename: (req, file, cb) => {
      const ext = path.extname(file.originalname);
      cb(null, `${Date.now()}-${crypto.randomBytes(8).toString('hex')}${ext}`);
    }
  }),
  limits: { fileSize: 50 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    const allowed = /jpeg|jpg|png|gif|pdf|doc|docx|xls|xlsx|csv|txt|zip|ppt|pptx|dwg|step|iges/;
    const ok = allowed.test(path.extname(file.originalname).toLowerCase());
    cb(null, ok);
  }
});

app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname, 'public')));
app.get('/', (req, res) => res.redirect('/dashboard.html'));
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET,POST,PUT,PATCH,DELETE,OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

// WebSocket
wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', (msg) => {
    try { const d = JSON.parse(msg); if (d.type === 'ping') ws.send(JSON.stringify({ type: 'pong', ts: Date.now() })); } catch(e) {}
  });
});
setInterval(() => { wss.clients.forEach(ws => { if (!ws.isAlive) return ws.terminate(); ws.isAlive = false; ws.ping(); }); }, 30000);

// Helpers
function genId() { return crypto.randomUUID(); }
function now() { return new Date().toISOString(); }
function hashPassword(pw) { const s = crypto.randomBytes(16).toString('hex'); const h = crypto.pbkdf2Sync(pw, s, 100000, 64, 'sha512').toString('hex'); return `${s}:${h}`; }
function verifyPassword(pw, stored) { const [s, h] = stored.split(':'); return h === crypto.pbkdf2Sync(pw, s, 100000, 64, 'sha512').toString('hex'); }
function genToken() { return crypto.randomBytes(32).toString('hex'); }

// =================== EVENT BUS ===================
class EventBus extends EventEmitter {
  constructor() {
    super();
    this.setMaxListeners(50);
  }

  emitEvent(type, data) {
    const event = {
      id: genId(),
      type,
      module: data.module || 'system',
      entityId: data.entityId || null,
      userId: data.userId || null,
      username: data.username || 'system',
      data: data.data || {},
      timestamp: now(),
      processed: false
    };
    if (!db.data.events) db.data.events = [];
    db.data.events.push(event);
    if (db.data.events.length > 2000) db.data.events = db.data.events.slice(-1500);
    this.emit(type, event);
    this.broadcast(event);
    return event;
  }

  broadcast(event) {
    const msg = JSON.stringify({ type: 'event', payload: event });
    wss.clients.forEach(client => {
      if (client.readyState === 1) {
        try { client.send(msg); } catch(e) {}
      }
    });
  }

  getEvents(filters = {}) {
    let events = db.data.events || [];
    if (filters.module) events = events.filter(e => e.module === filters.module);
    if (filters.type) events = events.filter(e => e.type === filters.type);
    if (filters.entityId) events = events.filter(e => e.entityId === filters.entityId);
    if (filters.userId) events = events.filter(e => e.userId === filters.userId);
    if (filters.since) events = events.filter(e => new Date(e.timestamp) >= new Date(filters.since));
    events.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
    return filters.limit ? events.slice(0, filters.limit) : events;
  }
}
const eventBus = new EventBus();

// =================== WORKFLOW ENGINE ===================
const WorkflowEngine = {
  startInstance(workflowId, entityId, module, startedBy) {
    const wf = (db.data.workflows || []).find(w => w.id === workflowId);
    if (!wf || !wf.active) return null;
    const instance = {
      id: genId(),
      workflowId,
      workflowName: wf.name,
      entityId,
      module,
      currentStep: 0,
      status: 'pending',
      steps: wf.steps.map((s, i) => ({ ...s, stepIndex: i, status: i === 0 ? 'pending' : 'waiting', approvedBy: null, approvedAt: null, comment: null })),
      history: [{ step: 0, action: 'started', by: startedBy, at: now() }],
      startedBy,
      createdAt: now(),
      updatedAt: now()
    };
    if (!db.data.workflow_instances) db.data.workflow_instances = [];
    db.data.workflow_instances.push(instance);
    eventBus.emitEvent('workflow.started', { module, entityId, userId: startedBy, username: startedBy, data: { workflowId, workflowName: wf.name, instanceId: instance.id } });
    return instance;
  },

  approveStep(instanceId, userId, username, comment) {
    const inst = (db.data.workflow_instances || []).find(i => i.id === instanceId);
    if (!inst || inst.status !== 'pending') return null;
    const step = inst.steps[inst.currentStep];
    if (!step) return null;
    step.status = 'approved';
    step.approvedBy = username;
    step.approvedAt = now();
    step.comment = comment || '';
    inst.history.push({ step: inst.currentStep, action: 'approved', by: username, at: now(), comment });
    if (inst.currentStep >= inst.steps.length - 1) {
      inst.status = 'approved';
      inst.completedAt = now();
      eventBus.emitEvent('workflow.completed', { module: inst.module, entityId: inst.entityId, userId, username, data: { workflowName: inst.workflowName, instanceId } });
    } else {
      inst.currentStep++;
      inst.steps[inst.currentStep].status = 'pending';
      eventBus.emitEvent('workflow.step_approved', { module: inst.module, entityId: inst.entityId, userId, username, data: { workflowName: inst.workflowName, step: step.name } });
    }
    inst.updatedAt = now();
    return inst;
  },

  rejectStep(instanceId, userId, username, comment) {
    const inst = (db.data.workflow_instances || []).find(i => i.id === instanceId);
    if (!inst || inst.status !== 'pending') return null;
    const step = inst.steps[inst.currentStep];
    step.status = 'rejected';
    step.approvedBy = username;
    step.approvedAt = now();
    step.comment = comment || '';
    inst.status = 'rejected';
    inst.completedAt = now();
    inst.history.push({ step: inst.currentStep, action: 'rejected', by: username, at: now(), comment });
    eventBus.emitEvent('workflow.rejected', { module: inst.module, entityId: inst.entityId, userId, username, data: { workflowName: inst.workflowName, step: step.name } });
    inst.updatedAt = now();
    return inst;
  },

  getPendingForUser(userId) {
    return (db.data.workflow_instances || []).filter(i => i.status === 'pending' && i.steps[i.currentStep]?.assignee === userId);
  }
};

function auth(req, res, next) {
  let tokenVal = null;
  const a = req.headers.authorization;
  if (a?.startsWith('Bearer ')) {
    tokenVal = a.slice(7);
  } else if (req.query.token) {
    tokenVal = req.query.token;
  }
  if (!tokenVal) return res.status(401).json({ error: 'No token' });
  const session = db.findOne('sessions', s => s.token === tokenVal && new Date(s.expiresAt) > new Date());
  if (!session) return res.status(401).json({ error: 'Invalid/expired token' });
  const user = db.findById('users', session.userId);
  if (!user) return res.status(401).json({ error: 'User not found' });
  req.user = user;
  req.token = tokenVal;
  next();
}

function requireAdmin(req, res, next) {
  if (req.user.role !== 'admin') return res.status(403).json({ error: 'Admin only' });
  next();
}

function paginatedResults(table, req) {
  let results = db.findAll(table);
  const { search, sort, order, page = 1, limit = 50, ...filters } = req.query;
  Object.entries(filters).forEach(([k, v]) => {
    if (v !== undefined && v !== '') results = results.filter(r => String(r[k]).toLowerCase().includes(String(v).toLowerCase()));
  });
  if (search) {
    const q = search.toLowerCase();
    results = results.filter(r => Object.values(r).some(v => String(v).toLowerCase().includes(q)));
  }
  if (sort) results.sort((a, b) => { const va = a[sort] || '', vb = b[sort] || ''; return order === 'desc' ? (vb > va ? 1 : -1) : (va > vb ? 1 : -1); });
  const total = results.length;
  const start = (parseInt(page) - 1) * parseInt(limit);
  results = results.slice(start, start + parseInt(limit));
  return { data: results, total, page: parseInt(page), limit: parseInt(limit), pages: Math.ceil(total / parseInt(limit)) };
}

// Seed data
function initDefaultData() {
  if (!db.findOne('users', u => u.username === 'admin')) {
    db.insert('users', { id: 'u1', username: 'admin', email: 'admin@carbonerp.local', passwordHash: hashPassword('admin123'), name: 'Administrator', role: 'admin', department: 'Management', jobTitle: 'System Administrator', phone: '+1-555-0001', status: 'active' });
    db.insert('users', { id: 'u2', username: 'sales', email: 'sales@carbonerp.local', passwordHash: hashPassword('sales123'), name: 'Sales Manager', role: 'user', department: 'Sales', jobTitle: 'Sales Manager', phone: '+1-555-0002', status: 'active' });
    db.insert('users', { id: 'u3', username: 'inventory', email: 'inventory@carbonerp.local', passwordHash: hashPassword('inv123'), name: 'Inventory Manager', role: 'user', department: 'Warehouse', jobTitle: 'Inventory Manager', phone: '+1-555-0003', status: 'active' });
    db.insert('users', { id: 'u4', username: 'accounting', email: 'accounting@carbonerp.local', passwordHash: hashPassword('acc123'), name: 'Accountant', role: 'user', department: 'Accounting', jobTitle: 'Senior Accountant', phone: '+1-555-0004', status: 'active' });
  }
  if (db.findAll('departments').length === 0) {
    const depts = ['Management', 'Sales', 'Marketing', 'Accounting', 'Warehouse', 'Production', 'Quality', 'HR', 'IT', 'Purchasing', 'Customer Support', 'R&D'];
    depts.forEach(d => db.insert('departments', { id: genId(), name: d, description: '', status: 'active', managerId: null }));
  }
  if (db.findAll('categories').length === 0) {
    const cats = ['Raw Materials', 'Chemicals', 'Consumables', 'Tools', 'Spare Parts', 'Finished Goods', 'Packaging'];
    cats.forEach(c => db.insert('categories', { id: genId(), name: c, description: '', status: 'active' }));
  }
  if (db.findAll('warehouses').length === 0) {
    db.insert('warehouses', { id: 'w1', name: 'Main Warehouse', code: 'WH-001', address: '100 Industrial Park', status: 'active', capacity: 10000 });
    db.insert('warehouses', { id: 'w2', name: 'Warehouse B', code: 'WH-002', address: '102 Industrial Park', status: 'active', capacity: 5000 });
    db.insert('warehouses', { id: 'w3', name: 'Warehouse C', code: 'WH-003', address: '104 Industrial Park', status: 'active', capacity: 3000 });
  }
  if (db.findAll('products').length === 0) {
    const products = [
      { id: 'p1', sku: 'SKU-001', name: 'Carbon Fiber Sheet 1mm', category: 'Raw Materials', stock: 150, minStock: 20, unit: 'pcs', cost: 30, price: 45, supplierId: 's1', warehouseId: 'w1', description: 'Standard 1mm carbon fiber sheet', status: 'active', weight: 0.5, barcode: 'CF001' },
      { id: 'p2', sku: 'SKU-002', name: 'Epoxy Resin 5L', category: 'Chemicals', stock: 75, minStock: 15, unit: 'cans', cost: 60, price: 89.50, supplierId: 's2', warehouseId: 'w2', description: 'Industrial epoxy resin', status: 'active', weight: 5.5, barcode: 'EP002' },
      { id: 'p3', sku: 'SKU-003', name: 'Vacuum Bagging Film', category: 'Consumables', stock: 200, minStock: 30, unit: 'rolls', cost: 80, price: 125, supplierId: 's3', warehouseId: 'w1', description: 'Nylon vacuum bagging film', status: 'active', weight: 1.2, barcode: 'VB003' },
      { id: 'p4', sku: 'SKU-004', name: 'Release Agent Spray', category: 'Chemicals', stock: 8, minStock: 10, unit: 'cans', cost: 20, price: 32, supplierId: 's2', warehouseId: 'w3', description: 'Semi-permanent release agent', status: 'active', weight: 0.8, barcode: 'RA004' },
      { id: 'p5', sku: 'SKU-005', name: 'Carbon Fiber Tape 50mm', category: 'Raw Materials', stock: 300, minStock: 50, unit: 'rolls', cost: 12, price: 18.75, supplierId: 's1', warehouseId: 'w1', description: 'Unidirectional carbon fiber tape', status: 'active', weight: 0.3, barcode: 'CT005' },
      { id: 'p6', sku: 'SKU-006', name: 'Peel Ply Fabric', category: 'Consumables', stock: 120, minStock: 25, unit: 'pcs', cost: 15, price: 22.50, supplierId: 's4', warehouseId: 'w2', description: 'Nylon peel ply fabric', status: 'active', weight: 0.1, barcode: 'PP006' },
      { id: 'p7', sku: 'SKU-007', name: 'Breather Fabric', category: 'Consumables', stock: 5, minStock: 15, unit: 'pcs', cost: 10, price: 15, supplierId: 's4', warehouseId: 'w3', description: 'Polyester breather fabric', status: 'active', weight: 0.05, barcode: 'BF007' },
      { id: 'p8', sku: 'SKU-008', name: 'Mixing Cups 1L', category: 'Tools', stock: 500, minStock: 100, unit: 'pcs', cost: 0.50, price: 0.85, supplierId: 's5', warehouseId: 'w2', description: 'Disposable mixing cups', status: 'active', weight: 0.05, barcode: 'MC008' },
      { id: 'p9', sku: 'SKU-009', name: 'Fiberglass Cloth', category: 'Raw Materials', stock: 0, minStock: 30, unit: 'meters', cost: 8, price: 14, supplierId: 's1', warehouseId: 'w1', description: 'E-glass woven fabric 200gsm', status: 'active', weight: 0.2, barcode: 'FG009' },
      { id: 'p10', sku: 'SKU-010', name: 'Carbon Fiber Tube 25mm', category: 'Raw Materials', stock: 80, minStock: 20, unit: 'pcs', cost: 25, price: 42, supplierId: 's1', warehouseId: 'w1', description: 'Pultruded carbon fiber tube', status: 'active', weight: 0.15, barcode: 'CT010' },
    ];
    products.forEach(p => db.insert('products', p));
  }
  if (db.findAll('suppliers').length === 0) {
    const suppliers = [
      { id: 's1', name: 'CarbonTech Inc', email: 'orders@carbontech.com', phone: '+27-11-1001', address: '500 Carbon Way, Johannesburg', contactPerson: 'James Wilson', leadTime: 7, rating: 5, status: 'active', paymentTerms: 'Net 30', bankDetails: 'First National Bank' },
      { id: 's2', name: 'ChemSupply Co', email: 'supply@chemsupply.com', phone: '+27-21-1002', address: '600 Chemical Blvd, Cape Town', contactPerson: 'Maria Garcia', leadTime: 5, rating: 4, status: 'active', paymentTerms: 'Net 30', bankDetails: 'First National Bank' },
      { id: 's3', name: 'VacuumTech Ltd', email: 'sales@vacuumtech.com', phone: '+27-31-1003', address: '700 Vacuum Ave, Durban', contactPerson: 'Robert Brown', leadTime: 10, rating: 4, status: 'active', paymentTerms: 'Net 30', bankDetails: 'First National Bank' },
      { id: 's4', name: 'CompositeMaterials', email: 'info@compositematerials.com', phone: '+27-12-1004', address: '800 Composite Dr, Pretoria', contactPerson: 'Sarah Davis', leadTime: 6, rating: 3, status: 'active', paymentTerms: 'Net 30', bankDetails: 'First National Bank' },
      { id: 's5', name: 'LabSupply Co', email: 'orders@labsupply.com', phone: '+27-41-1005', address: '900 Lab Road, Port Elizabeth', contactPerson: 'Mike Johnson', leadTime: 3, rating: 4, status: 'active', paymentTerms: 'Net 30', bankDetails: 'First National Bank' },
    ];
    suppliers.forEach(s => db.insert('suppliers', s));
  }
  if (db.findAll('customers').length === 0) {
    const customers = [
      { id: 'c1', name: 'AeroSpace Dynamics', email: 'procurement@aerospacedyn.com', phone: '+27-21-2001', address: '100 Aviation Blvd, Cape Town', contactPerson: 'Tom Anderson', creditLimit: 500000, balance: 0, type: 'OEM', industry: 'Aerospace', status: 'active', paymentTerms: 'Net 30', dunningLevel: 0, creditUsed: 0 },
      { id: 'c2', name: 'Marine Composites Ltd', email: 'orders@marinecomp.com', phone: '+27-31-2002', address: '200 Harbor Dr, Durban', contactPerson: 'Lisa Chen', creditLimit: 250000, balance: 0, type: 'Distributor', industry: 'Marine', status: 'active', paymentTerms: 'Net 30', dunningLevel: 0, creditUsed: 0 },
      { id: 'c3', name: 'Wind Energy Systems', email: 'supply@windenergy.com', phone: '+27-11-2003', address: '300 Turbine Way, Johannesburg', contactPerson: 'David Miller', creditLimit: 750000, balance: 0, type: 'OEM', industry: 'Energy', status: 'active', paymentTerms: 'Net 30', dunningLevel: 0, creditUsed: 0 },
      { id: 'c4', name: 'AutoRace Engineering', email: 'purchasing@autorace.com', phone: '+27-12-2004', address: '400 Speed Lane, Pretoria', contactPerson: 'Kevin Taylor', creditLimit: 400000, balance: 0, type: 'OEM', industry: 'Automotive', status: 'active', paymentTerms: 'Net 30', dunningLevel: 0, creditUsed: 0 },
      { id: 'c5', name: 'DroneTech Innovations', email: 'procurement@dronetech.io', phone: '+27-41-2005', address: '500 Flight Path, Port Elizabeth', contactPerson: 'Amy White', creditLimit: 150000, balance: 0, type: 'Startup', industry: 'Defense', status: 'active', paymentTerms: 'Net 30', dunningLevel: 0, creditUsed: 0 },
      { id: 'c6', name: 'SportsTech Racing', email: 'orders@sportstech.com', phone: '+27-18-2006', address: '600 Track Rd, Bloemfontein', contactPerson: 'Chris Martin', creditLimit: 300000, balance: 0, type: 'OEM', industry: 'Sports', status: 'active', paymentTerms: 'Net 30', dunningLevel: 0, creditUsed: 0 },
    ];
    customers.forEach(c => db.insert('customers', c));
  }
  if (db.findAll('sales_orders').length === 0) {
    const orders = [
      { id: 'so1', number: 'SO-2026-001', customerId: 'c1', customerName: 'AeroSpace Dynamics', orderDate: '2026-01-15', deliveryDate: '2026-02-15', status: 'shipped', subtotal: 900, tax: 135, total: 1035, notes: 'Urgent order', paymentTerms: 'Net 30', salesRep: 'u2', currency: 'ZAR' },
      { id: 'so2', number: 'SO-2026-002', customerId: 'c2', customerName: 'Marine Composites Ltd', orderDate: '2026-02-10', deliveryDate: '2026-03-10', status: 'shipped', subtotal: 895, tax: 134.25, total: 1029.25, notes: '', paymentTerms: 'Net 30', salesRep: 'u2', currency: 'ZAR' },
      { id: 'so3', number: 'SO-2026-003', customerId: 'c3', customerName: 'Wind Energy Systems', orderDate: '2026-03-05', deliveryDate: '2026-04-05', status: 'shipped', subtotal: 625, tax: 93.75, total: 718.75, notes: 'Check stock availability', paymentTerms: 'Net 30', salesRep: 'u2', currency: 'ZAR' },
      { id: 'so4', number: 'SO-2026-004', customerId: 'c4', customerName: 'AutoRace Engineering', orderDate: '2026-04-20', deliveryDate: '2026-05-20', status: 'shipped', subtotal: 675, tax: 101.25, total: 776.25, notes: '', paymentTerms: 'Net 30', salesRep: 'u2', currency: 'ZAR' },
      { id: 'so5', number: 'SO-2026-005', customerId: 'c5', customerName: 'DroneTech Innovations', orderDate: '2026-05-15', deliveryDate: '2026-06-15', status: 'processing', subtotal: 937.50, tax: 140.63, total: 1078.13, notes: 'New customer order', paymentTerms: 'Net 30', salesRep: 'u2', currency: 'ZAR' },
    ];
    orders.forEach(o => db.insert('sales_orders', o));
    const lines = [
      { id: 'sol1', orderId: 'so1', productId: 'p1', productName: 'Carbon Fiber Sheet 1mm', sku: 'SKU-001', qty: 20, unitPrice: 45, subtotal: 900, vatRate: 15, vatAmount: 135, total: 1035 },
      { id: 'sol2', orderId: 'so2', productId: 'p2', productName: 'Epoxy Resin 5L', sku: 'SKU-002', qty: 10, unitPrice: 89.50, subtotal: 895, vatRate: 15, vatAmount: 134.25, total: 1029.25 },
      { id: 'sol3', orderId: 'so3', productId: 'p3', productName: 'Vacuum Bagging Film', sku: 'SKU-003', qty: 5, unitPrice: 125, subtotal: 625, vatRate: 15, vatAmount: 93.75, total: 718.75 },
      { id: 'sol4', orderId: 'so4', productId: 'p1', productName: 'Carbon Fiber Sheet 1mm', sku: 'SKU-001', qty: 15, unitPrice: 45, subtotal: 675, vatRate: 15, vatAmount: 101.25, total: 776.25 },
      { id: 'sol5', orderId: 'so5', productId: 'p5', productName: 'Carbon Fiber Tape 50mm', sku: 'SKU-005', qty: 50, unitPrice: 18.75, subtotal: 937.50, vatRate: 15, vatAmount: 140.63, total: 1078.13 },
    ];
    lines.forEach(l => db.insert('sales_order_lines', l));
  }
  if (db.findAll('purchase_orders').length === 0) {
    const pos = [
      { id: 'po1', number: 'PO-2026-001', supplierId: 's1', supplierName: 'CarbonTech Inc', orderDate: '2026-01-10', expectedDate: '2026-02-10', status: 'received', subtotal: 1500, tax: 225, total: 1725, notes: 'Restock raw materials', paymentTerms: 'Net 30', currency: 'ZAR' },
      { id: 'po2', number: 'PO-2026-002', supplierId: 's2', supplierName: 'ChemSupply Co', orderDate: '2026-02-05', expectedDate: '2026-03-05', status: 'received', subtotal: 900, tax: 135, total: 1035, notes: '', paymentTerms: 'Net 30', currency: 'ZAR' },
      { id: 'po3', number: 'PO-2026-003', supplierId: 's3', supplierName: 'VacuumTech Ltd', orderDate: '2026-03-01', expectedDate: '2026-04-01', status: 'received', subtotal: 2000, tax: 300, total: 2300, notes: 'Quarterly stock replenishment', paymentTerms: 'Net 30', currency: 'ZAR' },
    ];
    pos.forEach(p => db.insert('purchase_orders', p));
    const poLines = [
      { id: 'pol1', orderId: 'po1', productId: 'p1', productName: 'Carbon Fiber Sheet 1mm', sku: 'SKU-001', qty: 50, unitPrice: 30, subtotal: 1500, vatRate: 15, vatAmount: 225, total: 1725 },
      { id: 'pol2', orderId: 'po2', productId: 'p2', productName: 'Epoxy Resin 5L', sku: 'SKU-002', qty: 15, unitPrice: 60, subtotal: 900, vatRate: 15, vatAmount: 135, total: 1035 },
      { id: 'pol3', orderId: 'po3', productId: 'p3', productName: 'Vacuum Bagging Film', sku: 'SKU-003', qty: 25, unitPrice: 80, subtotal: 2000, vatRate: 15, vatAmount: 300, total: 2300 },
    ];
    poLines.forEach(l => db.insert('purchase_order_lines', l));
  }
  if (db.findAll('accounts').length === 0) {
    const accts = [
      // ASSETS - Current (debit-normal: positive balance)
      { id: 'a1', code: '1000', name: 'Cash - Main Operating', type: 'asset', subtype: 'bank', balance: 250000, status: 'active', ifrs_element: 'ifrs-full_CashAndCashEquivalents', ifrs_category: 'operating', current_non_current: 'current' },
      { id: 'a1b', code: '1010', name: 'Cash - Savings', type: 'asset', subtype: 'bank', balance: 150000, status: 'active', ifrs_element: 'ifrs-full_CashAndCashEquivalents', ifrs_category: 'operating', current_non_current: 'current' },
      { id: 'a2', code: '1100', name: 'Accounts Receivable', type: 'asset', subtype: 'receivable', balance: 0, status: 'active', ifrs_element: 'ifrs-full_TradeAndOtherCurrentReceivables', ifrs_category: 'operating', current_non_current: 'current' },
      { id: 'a3', code: '1200', name: 'Inventory - Raw Materials', type: 'asset', subtype: 'inventory', balance: 28000, status: 'active', ifrs_element: 'ifrs-full_Inventories', ifrs_category: 'operating', current_non_current: 'current' },
      { id: 'a3b', code: '1210', name: 'Inventory - Finished Goods', type: 'asset', subtype: 'inventory', balance: 17000, status: 'active', ifrs_element: 'ifrs-full_Inventories', ifrs_category: 'operating', current_non_current: 'current' },
      { id: 'a3c', code: '1220', name: 'Inventory - WIP', type: 'asset', subtype: 'inventory', balance: 8500, status: 'active', ifrs_element: 'ifrs-full_Inventories', ifrs_category: 'operating', current_non_current: 'current' },
      { id: 'a11', code: '1300', name: 'Prepaid Expenses', type: 'asset', subtype: 'prepaid', balance: 5000, status: 'active', ifrs_element: 'ifrs-full_OtherCurrentNonfinancialAssets', ifrs_category: 'operating', current_non_current: 'current' },
      { id: 'a12', code: '1310', name: 'VAT Input Tax', type: 'asset', subtype: 'tax', balance: 0, status: 'active', ifrs_element: 'ifrs-full_CurrentTaxAssetsCurrent', ifrs_category: 'operating', current_non_current: 'current' },
      // ASSETS - Non-Current
      { id: 'a9', code: '1500', name: 'Property & Equipment', type: 'asset', subtype: 'fixed', balance: 180000, status: 'active', ifrs_element: 'ifrs-full_PropertyPlantAndEquipment', ifrs_category: 'investing', current_non_current: 'non-current' },
      { id: 'a9b', code: '1510', name: 'Vehicles', type: 'asset', subtype: 'fixed', balance: 95000, status: 'active', ifrs_element: 'ifrs-full_PropertyPlantAndEquipment', ifrs_category: 'investing', current_non_current: 'non-current' },
      { id: 'a9c', code: '1520', name: 'Computer Equipment', type: 'asset', subtype: 'fixed', balance: 35000, status: 'active', ifrs_element: 'ifrs-full_PropertyPlantAndEquipment', ifrs_category: 'investing', current_non_current: 'non-current' },
      { id: 'a9d', code: '1590', name: 'Accumulated Depreciation', type: 'asset', subtype: 'contra_asset', balance: -68000, status: 'active', ifrs_element: 'ifrs-full_PropertyPlantAndEquipment', ifrs_category: 'investing', current_non_current: 'non-current' },
      // LIABILITIES - Current (credit-normal: negative balance)
      { id: 'a4', code: '2000', name: 'Accounts Payable', type: 'liability', subtype: 'payable', balance: 0, status: 'active', ifrs_element: 'ifrs-full_TradeAndOtherPayables', ifrs_category: 'operating', current_non_current: 'current' },
      { id: 'a13', code: '2010', name: 'Output VAT', type: 'liability', subtype: 'tax', balance: 0, status: 'active', ifrs_element: 'ifrs-full_TradeAndOtherPayables', ifrs_category: 'operating', current_non_current: 'current' },
      { id: 'a14', code: '2020', name: 'Accrued Expenses', type: 'liability', subtype: 'accrued', balance: 0, status: 'active', ifrs_element: 'ifrs-full_TradeAndOtherPayables', ifrs_category: 'operating', current_non_current: 'current' },
      { id: 'a10', code: '2100', name: 'Income Tax Payable', type: 'liability', subtype: 'tax', balance: 0, status: 'active', ifrs_element: 'ifrs-full_CurrentTaxLiabilitiesCurrent', ifrs_category: 'operating', current_non_current: 'current' },
      { id: 'a15', code: '2110', name: 'PAYE Payable', type: 'liability', subtype: 'tax', balance: 0, status: 'active', ifrs_element: 'ifrs-full_ProvisionsForEmployeeBenefits', ifrs_category: 'operating', current_non_current: 'current' },
      { id: 'a16', code: '2120', name: 'UIF Payable', type: 'liability', subtype: 'tax', balance: 0, status: 'active', ifrs_element: 'ifrs-full_ProvisionsForEmployeeBenefits', ifrs_category: 'operating', current_non_current: 'current' },
      { id: 'a17', code: '2200', name: 'Short-term Loan', type: 'liability', subtype: 'loan', balance: -25000, status: 'active', ifrs_element: 'ifrs-full_OtherFinancialLiabilities', ifrs_category: 'financing', current_non_current: 'current' },
      // LIABILITIES - Non-Current
      { id: 'a18', code: '2500', name: 'Long-term Loan', type: 'liability', subtype: 'loan', balance: -120000, status: 'active', ifrs_element: 'ifrs-full_OtherFinancialLiabilities', ifrs_category: 'financing', current_non_current: 'non-current' },
      // EQUITY (credit-normal: negative balance)
      { id: 'a5', code: '3000', name: 'Share Capital', type: 'equity', subtype: 'capital', balance: -300000, status: 'active', ifrs_element: 'ifrs-full_IssuedCapital', ifrs_category: 'equity', current_non_current: 'equity' },
      { id: 'a19', code: '3100', name: 'Retained Earnings', type: 'equity', subtype: 'retained', balance: -292500, status: 'active', ifrs_element: 'ifrs-full_RetainedEarnings', ifrs_category: 'equity', current_non_current: 'equity' },
      { id: 'a20', code: '3200', name: 'Current Year Earnings', type: 'equity', subtype: 'current', balance: 0, status: 'active', ifrs_element: 'ifrs-full_RetainedEarnings', ifrs_category: 'equity', current_non_current: 'equity' },
      // INCOME (credit-normal: negative balance, start at 0 for new period)
      { id: 'a6', code: '4000', name: 'Sales Revenue', type: 'income', subtype: 'revenue', balance: 0, status: 'active', ifrs_element: 'ifrs-full_Revenue', ifrs_category: 'operating', current_non_current: 'pnl' },
      { id: 'a21', code: '4100', name: 'Service Income', type: 'income', subtype: 'revenue', balance: 0, status: 'active', ifrs_element: 'ifrs-full_Revenue', ifrs_category: 'operating', current_non_current: 'pnl' },
      { id: 'a22', code: '4200', name: 'Interest Income', type: 'income', subtype: 'other', balance: 0, status: 'active', ifrs_element: 'ifrs-full_InterestRevenueCalculatedUsingEffectiveInterestMethodInvesting', ifrs_category: 'investing', current_non_current: 'pnl' },
      { id: 'a23', code: '4300', name: 'Discount Received', type: 'income', subtype: 'other', balance: 0, status: 'active', ifrs_element: 'ifrs-full_OtherOperatingIncomeExpense', ifrs_category: 'operating', current_non_current: 'pnl' },
      // EXPENSES - Cost of Sales (debit-normal: positive balance, start at 0)
      { id: 'a7', code: '5000', name: 'Cost of Goods Sold', type: 'expense', subtype: 'cogs', balance: 0, status: 'active', ifrs_element: 'ifrs-full_CostOfSales', ifrs_category: 'operating', current_non_current: 'pnl' },
      { id: 'a24', code: '5010', name: 'Direct Labour', type: 'expense', subtype: 'cogs', balance: 0, status: 'active', ifrs_element: 'ifrs-full_CostOfSales', ifrs_category: 'operating', current_non_current: 'pnl' },
      { id: 'a25', code: '5020', name: 'Manufacturing Overhead', type: 'expense', subtype: 'cogs', balance: 0, status: 'active', ifrs_element: 'ifrs-full_CostOfSales', ifrs_category: 'operating', current_non_current: 'pnl' },
      // EXPENSES - Operating
      { id: 'a8', code: '6000', name: 'Salaries & Wages', type: 'expense', subtype: 'operating', balance: 0, status: 'active', ifrs_element: 'ifrs-full_EmployeeBenefitsExpense', ifrs_category: 'operating', current_non_current: 'pnl' },
      { id: 'a26', code: '6100', name: 'Rent Expense', type: 'expense', subtype: 'operating', balance: 0, status: 'active', ifrs_element: 'ifrs-full_OtherOperatingIncomeExpense', ifrs_category: 'operating', current_non_current: 'pnl' },
      { id: 'a27', code: '6200', name: 'Utilities', type: 'expense', subtype: 'operating', balance: 0, status: 'active', ifrs_element: 'ifrs-full_OtherOperatingIncomeExpense', ifrs_category: 'operating', current_non_current: 'pnl' },
      { id: 'a28', code: '6300', name: 'Insurance', type: 'expense', subtype: 'operating', balance: 0, status: 'active', ifrs_element: 'ifrs-full_OtherOperatingIncomeExpense', ifrs_category: 'operating', current_non_current: 'pnl' },
      { id: 'a29', code: '6400', name: 'Depreciation', type: 'expense', subtype: 'operating', balance: 0, status: 'active', ifrs_element: 'ifrs-full_DepreciationAndAmortisationExpense', ifrs_category: 'operating', current_non_current: 'pnl' },
      { id: 'a30', code: '6500', name: 'Repairs & Maintenance', type: 'expense', subtype: 'operating', balance: 0, status: 'active', ifrs_element: 'ifrs-full_OtherOperatingIncomeExpense', ifrs_category: 'operating', current_non_current: 'pnl' },
      { id: 'a31', code: '6600', name: 'Marketing & Advertising', type: 'expense', subtype: 'operating', balance: 0, status: 'active', ifrs_element: 'ifrs-full_SellingExpense', ifrs_category: 'operating', current_non_current: 'pnl' },
      { id: 'a32', code: '6700', name: 'Professional Fees', type: 'expense', subtype: 'operating', balance: 0, status: 'active', ifrs_element: 'ifrs-full_AdministrativeExpense', ifrs_category: 'operating', current_non_current: 'pnl' },
      { id: 'a33', code: '6800', name: 'Office Supplies', type: 'expense', subtype: 'operating', balance: 0, status: 'active', ifrs_element: 'ifrs-full_AdministrativeExpense', ifrs_category: 'operating', current_non_current: 'pnl' },
      { id: 'a34', code: '6900', name: 'Telephone & Internet', type: 'expense', subtype: 'operating', balance: 0, status: 'active', ifrs_element: 'ifrs-full_AdministrativeExpense', ifrs_category: 'operating', current_non_current: 'pnl' },
      { id: 'a35', code: '7000', name: 'Bank Charges', type: 'expense', subtype: 'financial', balance: 0, status: 'active', ifrs_element: 'ifrs-full_FinanceCosts', ifrs_category: 'financing', current_non_current: 'pnl' },
      { id: 'a36', code: '7100', name: 'Interest Expense', type: 'expense', subtype: 'financial', balance: 0, status: 'active', ifrs_element: 'ifrs-full_InterestExpenseFinancing', ifrs_category: 'financing', current_non_current: 'pnl' },
      { id: 'a37', code: '7200', name: 'Bad Debts', type: 'expense', subtype: 'operating', balance: 0, status: 'active', ifrs_element: 'ifrs-full_ImpairmentLossRecognisedInProfitOrLoss', ifrs_category: 'operating', current_non_current: 'pnl' },
      { id: 'a38', code: '8000', name: 'Income Tax Expense', type: 'expense', subtype: 'tax', balance: 0, status: 'active', ifrs_element: 'ifrs-full_IncomeTaxExpenseContinuingOperations', ifrs_category: 'income_taxes', current_non_current: 'pnl' },
    ];
    accts.forEach(a => db.insert('accounts', a));
  }
  // Seed Fiscal Periods
  if (db.findAll('fiscal_periods').length === 0) {
    const months = ['January','February','March','April','May','June','July','August','September','October','November','December'];
    months.forEach((m, i) => {
      const mm = String(i + 1).padStart(2, '0');
      const lastDay = new Date(2026, i + 1, 0).getDate();
      db.insert('fiscal_periods', {
        id: `fp-2026-${mm}`,
        name: `${m} 2026`,
        year: 2026,
        month: i + 1,
        startDate: `2026-${mm}-01`,
        endDate: `2026-${mm}-${String(lastDay).padStart(2, '0')}`,
        status: i < 7 ? 'closed' : 'open',
        closedBy: i < 7 ? 'admin' : null,
        closedAt: i < 7 ? new Date().toISOString() : null
      });
    });
  }
  // Seed cost centers
  if (db.findAll('cost_centers').length === 0) {
    const costCenters = [
      { id: 'cc1', code: 'CC-001', name: 'Sales Department', description: 'Sales and customer relationship management', status: 'active' },
      { id: 'cc2', code: 'CC-002', name: 'Production Department', description: 'Manufacturing and production operations', status: 'active' },
      { id: 'cc3', code: 'CC-003', name: 'Finance Department', description: 'Financial management and accounting', status: 'active' },
      { id: 'cc4', code: 'CC-004', name: 'Administration', description: 'General administration and management', status: 'active' },
      { id: 'cc5', code: 'CC-005', name: 'R&D', description: 'Research and development', status: 'active' },
    ];
    costCenters.forEach(cc => db.insert('cost_centers', cc));
  }
  // Seed profit centers
  if (db.findAll('profit_centers').length === 0) {
    const profitCenters = [
      { id: 'pc1', code: 'PC-001', name: 'Composite Products', description: 'Core business - composite product manufacturing and sales', status: 'active' },
      { id: 'pc2', code: 'PC-002', name: 'Services', description: 'Consulting and training services', status: 'active' },
      { id: 'pc3', code: 'PC-003', name: 'Spare Parts', description: 'Spare parts sales and distribution', status: 'active' },
    ];
    profitCenters.forEach(pc => db.insert('profit_centers', pc));
  }
  // Seed AR invoices linked to SOs with GL postings
  if (db.findAll('invoices').length === 0) {
    const arAcct = (db.data.accounts || []).find(a => a.subtype === 'receivable');
    const revAcct = (db.data.accounts || []).find(a => a.code === '4000');
    const vatOutAcct = (db.data.accounts || []).find(a => a.code === '2010');
    const invs = [
      { id: 'inv-d1', number: 'INV-2026-001', customerId: 'c1', customerName: 'AeroSpace Dynamics', orderId: 'so1', date: '2026-01-15', dueDate: '2026-02-14', amount: 1035, subtotal: 900, tax: 135, currency: 'ZAR', status: 'paid', notes: 'Auto-generated from SO-2026-001' },
      { id: 'inv-d2', number: 'INV-2026-002', customerId: 'c2', customerName: 'Marine Composites Ltd', orderId: 'so2', date: '2026-02-10', dueDate: '2026-03-12', amount: 1029.25, subtotal: 895, tax: 134.25, currency: 'ZAR', status: 'paid', notes: 'Auto-generated from SO-2026-002' },
      { id: 'inv-d3', number: 'INV-2026-003', customerId: 'c3', customerName: 'Wind Energy Systems', orderId: 'so3', date: '2026-03-05', dueDate: '2026-04-04', amount: 718.75, subtotal: 625, tax: 93.75, currency: 'ZAR', status: 'pending', notes: 'Auto-generated from SO-2026-003' },
      { id: 'inv-d4', number: 'INV-2026-004', customerId: 'c4', customerName: 'AutoRace Engineering', orderId: 'so4', date: '2026-04-20', dueDate: '2026-05-20', amount: 776.25, subtotal: 675, tax: 101.25, currency: 'ZAR', status: 'pending', notes: 'Auto-generated from SO-2026-004' },
      { id: 'inv-d5', number: 'INV-2026-005', customerId: 'c5', customerName: 'DroneTech Innovations', orderId: 'so5', date: '2026-05-15', dueDate: '2026-06-14', amount: 1078.13, subtotal: 937.50, tax: 140.63, currency: 'ZAR', status: 'pending', notes: 'Auto-generated from SO-2026-005' },
    ];
    invs.forEach(inv => {
      db.data.invoices.push(inv);
      if (arAcct && revAcct && vatOutAcct) {
        postJournalAuto(inv.date, `Invoice ${inv.number} - ${inv.customerName}`, inv.number, inv.date.substring(0,7), [
          { accountId: arAcct.id, accountCode: arAcct.code, description: `AR - ${inv.customerName}`, debit: inv.amount, credit: 0 },
          { accountId: revAcct.id, accountCode: revAcct.code, description: `Revenue - ${inv.number}`, debit: 0, credit: inv.subtotal },
          { accountId: vatOutAcct.id, accountCode: vatOutAcct.code, description: `Output VAT - ${inv.number}`, debit: 0, credit: inv.tax }
        ], 'ar', inv.id, 'system');
      }
    });
  }
  // Seed AP bills linked to POs with GL postings
  if (db.findAll('bills').length === 0) {
    const apAcct = (db.data.accounts || []).find(a => a.subtype === 'payable');
    const invRmAcct = (db.data.accounts || []).find(a => a.code === '1200');
    const vatInAcct = (db.data.accounts || []).find(a => a.code === '1310');
    const bills = [
      { id: 'bill-d1', number: 'BILL-2026-001', supplierId: 's1', supplierName: 'CarbonTech Inc', orderId: 'po1', date: '2026-01-10', dueDate: '2026-02-09', amount: 1725, subtotal: 1500, tax: 225, currency: 'ZAR', status: 'paid', notes: 'Auto-generated from PO-2026-001' },
      { id: 'bill-d2', number: 'BILL-2026-002', supplierId: 's2', supplierName: 'ChemSupply Co', orderId: 'po2', date: '2026-02-05', dueDate: '2026-03-07', amount: 1035, subtotal: 900, tax: 135, currency: 'ZAR', status: 'paid', notes: 'Auto-generated from PO-2026-002' },
      { id: 'bill-d3', number: 'BILL-2026-003', supplierId: 's3', supplierName: 'VacuumTech Ltd', orderId: 'po3', date: '2026-03-01', dueDate: '2026-03-31', amount: 2300, subtotal: 2000, tax: 300, currency: 'ZAR', status: 'pending', notes: 'Auto-generated from PO-2026-003' },
    ];
    bills.forEach(b => {
      db.data.bills.push(b);
      if (apAcct && invRmAcct && vatInAcct) {
        postJournalAuto(b.date, `Bill ${b.number} - ${b.supplierName}`, b.number, b.date.substring(0,7), [
          { accountId: invRmAcct.id, accountCode: invRmAcct.code, description: `Inventory - ${b.supplierName}`, debit: b.subtotal, credit: 0 },
          { accountId: vatInAcct.id, accountCode: vatInAcct.code, description: `Input VAT - ${b.number}`, debit: b.tax, credit: 0 },
          { accountId: apAcct.id, accountCode: apAcct.code, description: `AP - ${b.supplierName}`, debit: 0, credit: b.amount }
        ], 'ap', b.id, 'system');
      }
    });
  }
  if (db.findAll('employees').length === 0) {
    const emps = [
      { id: 'e1', employeeId: 'EMP-001', firstName: 'John', lastName: 'Smith', email: 'john.smith@carbonerp.local', phone: '+27-11-3001', department: 'Management', jobTitle: 'CEO', hireDate: '2020-01-01', salary: 1800000, status: 'active', managerId: null, address: '10 Main St, Johannesburg', emergencyContact: 'Jane Smith', emergencyPhone: '+27-11-3002' },
      { id: 'e2', employeeId: 'EMP-002', firstName: 'Sarah', lastName: 'Johnson', email: 'sarah.j@carbonerp.local', phone: '+27-21-3003', department: 'Sales', jobTitle: 'Sales Manager', hireDate: '2020-03-15', salary: 1200000, status: 'active', managerId: 'e1', address: '20 Oak Ave, Cape Town', emergencyContact: 'Tom Johnson', emergencyPhone: '+27-21-3004' },
      { id: 'e3', employeeId: 'EMP-003', firstName: 'Mike', lastName: 'Williams', email: 'mike.w@carbonerp.local', phone: '+27-31-3005', department: 'Production', jobTitle: 'Production Supervisor', hireDate: '2020-06-01', salary: 960000, status: 'active', managerId: 'e1', address: '30 Pine Rd, Durban', emergencyContact: 'Lisa Williams', emergencyPhone: '+27-31-3006' },
      { id: 'e4', employeeId: 'EMP-004', firstName: 'Emily', lastName: 'Brown', email: 'emily.b@carbonerp.local', phone: '+27-12-3007', department: 'Accounting', jobTitle: 'Senior Accountant', hireDate: '2021-01-10', salary: 1080000, status: 'active', managerId: 'e1', address: '40 Elm St, Pretoria', emergencyContact: 'James Brown', emergencyPhone: '+27-12-3008' },
      { id: 'e5', employeeId: 'EMP-005', firstName: 'David', lastName: 'Garcia', email: 'david.g@carbonerp.local', phone: '+27-41-3009', department: 'Quality', jobTitle: 'Quality Inspector', hireDate: '2021-04-01', salary: 720000, status: 'active', managerId: 'e3', address: '50 Maple Dr, Port Elizabeth', emergencyContact: 'Maria Garcia', emergencyPhone: '+27-41-3010' },
    ];
    emps.forEach(e => db.insert('employees', e));
  }
  // Seed demo payroll entries (must run after employees are seeded)
  if ((db.data.payroll || []).length === 0) {
    const wagesAcct = (db.data.accounts || []).find(a => a.code === '6000');
    const payeAcct = (db.data.accounts || []).find(a => a.code === '2110');
    const uifAcct = (db.data.accounts || []).find(a => a.code === '2120');
    const cashAcct = (db.data.accounts || []).find(a => a.subtype === 'bank');
    const emps = db.findAll('employees').slice(0, 5);
    emps.forEach((emp, i) => {
      const gross = Math.round((emp.salary || 65000) / 12); // Monthly salary
      const paye = Math.round(gross * 0.18);
      const uif = Math.round(gross * 0.01);
      const net = gross - paye - uif;
      const pRec = { id: `pay-d${i+1}`, employeeId: emp.id, employeeName: `${emp.firstName} ${emp.lastName}`, period: '2026-07', grossSalary: gross, paye, uif, otherDeductions: 0, netPay: net, status: 'paid', createdAt: new Date().toISOString() };
      db.data.payroll.push(pRec);
      if (wagesAcct && cashAcct) {
        const lines = [{ accountId: wagesAcct.id, accountCode: wagesAcct.code, description: `Wages - ${emp.firstName} ${emp.lastName}`, debit: gross, credit: 0 }];
        if (payeAcct) lines.push({ accountId: payeAcct.id, accountCode: payeAcct.code, description: `PAYE - ${emp.firstName}`, debit: 0, credit: paye });
        if (uifAcct) lines.push({ accountId: uifAcct.id, accountCode: uifAcct.code, description: `UIF - ${emp.firstName}`, debit: 0, credit: uif });
        lines.push({ accountId: cashAcct.id, accountCode: cashAcct.code, description: `Net pay - ${emp.firstName}`, debit: 0, credit: net });
        postJournalAuto('2026-07-28', `Payroll - ${emp.firstName} ${emp.lastName}`, `PAY-D${i+1}`, '2026-07', lines, 'payroll', emp.id, 'system');
      }
    });
    // Sync bank_accounts balance with GL after payroll
    const cashGlAcct = (db.data.accounts || []).find(a => a.subtype === 'bank');
    if (cashGlAcct) {
      const ba = (db.data.bank_accounts || []).find(a => a.glAccountId === cashGlAcct.id);
      if (ba) ba.balance = cashGlAcct.balance;
    }
  }
  // Seed employee contracts
  if ((db.data.contracts || []).length === 0) {
    const contracts = [
      { id: 'ctr1', employeeId: 'e1', type: 'permanent', startDate: '2020-01-01', endDate: null, probationEnd: '2020-04-01', salary: 1800000, status: 'active', noticePeriod: '3 months', createdAt: new Date().toISOString() },
      { id: 'ctr2', employeeId: 'e2', type: 'permanent', startDate: '2020-03-15', endDate: null, probationEnd: '2020-06-15', salary: 1200000, status: 'active', noticePeriod: '1 month', createdAt: new Date().toISOString() },
      { id: 'ctr3', employeeId: 'e3', type: 'permanent', startDate: '2020-06-01', endDate: null, probationEnd: '2020-09-01', salary: 960000, status: 'active', noticePeriod: '1 month', createdAt: new Date().toISOString() },
      { id: 'ctr4', employeeId: 'e4', type: 'fixed-term', startDate: '2021-01-10', endDate: '2027-01-10', probationEnd: '2021-04-10', salary: 1080000, status: 'active', noticePeriod: '1 month', createdAt: new Date().toISOString() },
      { id: 'ctr5', employeeId: 'e5', type: 'permanent', startDate: '2021-04-01', endDate: null, probationEnd: '2021-07-01', salary: 720000, status: 'active', noticePeriod: '2 weeks', createdAt: new Date().toISOString() },
    ];
    contracts.forEach(c => { db.data.contracts = db.data.contracts || []; db.data.contracts.push(c); });
  }
  // Seed leave requests
  if ((db.data.leave_requests || []).length === 0) {
    const leaves = [
      { id: 'lr1', employeeId: 'e2', employeeName: 'Sarah Johnson', type: 'annual', startDate: '2026-08-15', endDate: '2026-08-20', days: 4, status: 'approved', approvedBy: 'e1', reason: 'Family holiday', createdAt: new Date().toISOString() },
      { id: 'lr2', employeeId: 'e3', employeeName: 'Mike Williams', type: 'sick', startDate: '2026-07-22', endDate: '2026-07-22', days: 1, status: 'approved', approvedBy: 'e1', reason: 'Medical appointment', createdAt: new Date().toISOString() },
      { id: 'lr3', employeeId: 'e5', employeeName: 'David Garcia', type: 'annual', startDate: '2026-09-01', endDate: '2026-09-05', days: 5, status: 'pending', approvedBy: null, reason: 'Personal', createdAt: new Date().toISOString() },
    ];
    leaves.forEach(l => { db.data.leave_requests = db.data.leave_requests || []; db.data.leave_requests.push(l); });
  }
  if (db.findAll('leads').length === 0) {
    const leads = [
      { id: 'l1', name: 'TechStart Inc', email: 'info@techstart.com', phone: '+1-555-4001', company: 'TechStart Inc', source: 'Website', stage: 'qualified', value: 15000, assignedTo: 'u2', status: 'active', notes: 'Interested in carbon fiber components' },
      { id: 'l2', name: 'GreenEnergy Co', email: 'procurement@greenenergy.com', phone: '+1-555-4002', company: 'GreenEnergy Co', source: 'Referral', stage: 'proposal', value: 45000, assignedTo: 'u2', status: 'active', notes: 'Wind turbine blade manufacturing' },
      { id: 'l3', name: 'RoboTech Solutions', email: 'orders@robotech.com', phone: '+1-555-4003', company: 'RoboTech Solutions', source: 'Trade Show', stage: 'contact', value: 8000, assignedTo: 'u2', status: 'active', notes: 'Drone frame components' },
    ];
    leads.forEach(l => db.insert('leads', l));
  }
  if (db.findAll('projects').length === 0) {
    const projects = [
      { id: 'proj1', name: 'New Product Line - Aerospace', description: 'Develop new carbon fiber components for aerospace applications', status: 'active', startDate: '2024-01-01', endDate: '2024-06-30', budget: 150000, spent: 45000, managerId: 'e1', priority: 'high' },
      { id: 'proj2', name: 'Factory Expansion', description: 'Expand manufacturing capacity by 30%', status: 'active', startDate: '2024-02-01', endDate: '2024-12-31', budget: 500000, spent: 120000, managerId: 'e1', priority: 'medium' },
      { id: 'proj3', name: 'Quality System Upgrade', description: 'Implement ISO 9001:2015 quality management system', status: 'planning', startDate: '2024-03-01', endDate: '2024-09-30', budget: 50000, spent: 10000, managerId: 'e5', priority: 'medium' },
    ];
    projects.forEach(p => db.insert('projects', p));
  }
  if (db.findAll('support_tickets').length === 0) {
    const tickets = [
      { id: 't1', number: 'TKT-001', subject: 'Delivery delay on SO-2024-002', customerId: 'c2', customerName: 'Marine Composites Ltd', status: 'open', priority: 'high', category: 'Delivery', assignedTo: 'u2', createdAt: '2024-01-20', description: 'Customer reports late delivery' },
      { id: 't2', number: 'TKT-002', subject: 'Quality issue with SKU-004', customerId: 'c4', customerName: 'AutoRace Engineering', status: 'in_progress', priority: 'critical', category: 'Quality', assignedTo: 'e5', createdAt: '2024-01-21', description: 'Batch #B20240115 spray not curing properly' },
      { id: 't3', number: 'TKT-003', subject: 'Request for custom quotation', customerId: 'c6', customerName: 'SportsTech Racing', status: 'closed', priority: 'low', category: 'Sales', assignedTo: 'u2', createdAt: '2024-01-18', description: 'Custom carbon fiber roll cage' },
    ];
    tickets.forEach(t => db.insert('support_tickets', t));
  }
  if (db.findAll('manufacturing_orders').length === 0) {
    const mos = [
      { id: 'mo1', number: 'MO-2024-001', productName: 'Carbon Fiber Panel 300x300mm', productId: null, qty: 100, status: 'completed', startDate: '2024-01-05', endDate: '2024-01-12', workCenterId: 'wc1', assignedTo: 'e3', priority: 'high', cost: 4500 },
      { id: 'mo2', number: 'MO-2024-002', productName: 'Epoxy Mix Kit 1L', productId: null, qty: 200, status: 'in_progress', startDate: '2024-01-15', endDate: '2024-01-22', workCenterId: 'wc2', assignedTo: 'e3', priority: 'medium', cost: 2000 },
      { id: 'mo3', number: 'MO-2024-003', productName: 'Vacuum Bag Set', productId: null, qty: 50, status: 'planned', startDate: '2024-01-25', endDate: '2024-02-01', workCenterId: 'wc1', assignedTo: 'e3', priority: 'medium', cost: 1200 },
    ];
    mos.forEach(m => db.insert('manufacturing_orders', m));
  }
  if (db.findAll('work_centers').length === 0) {
    const wcs = [
      { id: 'wc1', name: 'Layup Station', code: 'WC-001', capacity: 50, status: 'active', costPerHour: 75, utilization: 85 },
      { id: 'wc2', name: 'Resin Mixing Area', code: 'WC-002', capacity: 100, status: 'active', costPerHour: 50, utilization: 72 },
      { id: 'wc3', name: 'Autoclave 1', code: 'WC-003', capacity: 20, status: 'active', costPerHour: 150, utilization: 90 },
      { id: 'wc4', name: 'CNC Cutting Station', code: 'WC-004', capacity: 30, status: 'active', costPerHour: 100, utilization: 65 },
    ];
    wcs.forEach(w => db.insert('work_centers', w));
  }
  if (db.findAll('quality_checks').length === 0) {
    const qcs = [
      { id: 'qc1', name: 'Incoming Material Inspection', type: 'incoming', status: 'active', frequency: 'each_batch', parameters: ['Weight', 'Dimensions', 'Surface Quality'] },
      { id: 'qc2', name: 'In-Process Layup Check', type: 'in_process', status: 'active', frequency: 'hourly', parameters: ['Fiber Alignment', 'Resin Content', 'Air Bubbles'] },
      { id: 'qc3', name: 'Final Product Inspection', type: 'final', status: 'active', frequency: 'each_batch', parameters: ['Dimensional Accuracy', 'Surface Finish', 'Strength Test', 'Weight'] },
    ];
    qcs.forEach(q => db.insert('quality_checks', q));
  }
  if (db.findAll('bills_of_materials').length === 0) {
    const boms = [
      { id: 'bom1', name: 'Carbon Fiber Panel BOM', productId: 'p1', qty: 1, status: 'active', routingId: 'r1', cost: 45 },
      { id: 'bom2', name: 'Epoxy Mix Kit BOM', productId: 'p2', qty: 1, status: 'active', routingId: 'r2', cost: 60 },
    ];
    boms.forEach(b => db.insert('bills_of_materials', b));
    const bomLines = [
      { id: 'bl1', bomId: 'bom1', productId: 'p1', productName: 'Carbon Fiber Sheet 1mm', qty: 2, unitCost: 30 },
      { id: 'bl2', bomId: 'bom1', productId: 'p2', productName: 'Epoxy Resin 5L', qty: 0.5, unitCost: 60 },
      { id: 'bl3', bomId: 'bom2', productId: 'p2', productName: 'Epoxy Resin 5L', qty: 1, unitCost: 60 },
    ];
    bomLines.forEach(l => db.insert('bom_lines', l));
  }
  if (db.findAll('knowledge_articles').length === 0) {
    const articles = [
      { id: 'ka1', title: 'Carbon Fiber Handling Guide', category: 'Safety', author: 'e5', content: 'Always wear gloves when handling raw carbon fiber...', status: 'published', views: 120, tags: ['safety', 'carbon-fiber', 'handling'] },
      { id: 'ka2', title: 'Epoxy Mixing Procedures', category: 'Procedures', author: 'e3', content: 'Mix ratio: 2:1 resin to hardener by weight...', status: 'published', views: 95, tags: ['procedures', 'epoxy', 'mixing'] },
      { id: 'ka3', title: 'Quality Control Standards', category: 'Quality', author: 'e5', content: 'All products must meet ISO 9001:2015 standards...', status: 'published', views: 80, tags: ['quality', 'standards', 'iso'] },
    ];
    articles.forEach(a => db.insert('knowledge_articles', a));
  }
  if (db.findAll('accounts').length > 0) {
    db.data.settings = {
      companyName: 'Ecoplanet Management',
      legalName: 'Ecoplanet Management Pty Ltd',
      address: '100 Industrial Park Drive',
      phone: '+27-11-0000',
      email: 'info@carbonerp.local',
      faxNumber: '',
      bccEmail: '',
      companyNumber: 'CERP-001',
      ntn: '',
      strn: '',
      domicile: 'Johannesburg',
      city: 'Johannesburg',
      state: 'Gauteng',
      zipCode: '2000',
      country: 'South Africa',
      timezone: 'Africa/Johannesburg',
      taxId: 'TAX-12345678',
      currency: 'ZAR',
      homeCurrency: 'ZAR',
      fiscalYearStart: '01',
      taxRate: 15,
      taxesEnabled: true,
      includeTaxOnDocs: false,
      suppressTaxRates: false,
      autoIncrementRefs: true,
      basePriceType: 'RETAIL',
      costPlusMarkup: 0,
      roundPricesTo: 2,
      showLogoOnReports: true,
      autoPrintDialog: false,
      taxLastPeriod: 1,
      domain: '',
      logoPath: null,
      paymentTerms: ['Net 15', 'Net 30', 'Net 45', 'Net 60', 'Due on Receipt'],
      invoicePrefix: 'INV',
      poPrefix: 'PO',
      soPrefix: 'SO',
      moPrefix: 'MO',
      autoLogoutTimeout: 480,
      minPasswordLength: 8,
      emailNewOrder: true,
      emailOrderStatus: true,
      emailLowStock: true,
      emailNewTicket: true,
      emailPayroll: true,
      emailManufacturing: true,
    };
    db.save();
  }
}

initDefaultData();

// =================== AUTH ===================
app.post('/api/auth/login', (req, res) => {
  const { username, password } = req.body;
  if (!username || !password) return res.status(400).json({ error: 'Username and password required' });
  const user = db.findOne('users', u => u.username === username);
  if (!user || !verifyPassword(password, user.passwordHash)) return res.status(401).json({ error: 'Invalid credentials' });
  const token = genToken();
  const expiresAt = new Date(Date.now() + 7 * 24 * 60 * 60 * 1000).toISOString();
  db.insert('sessions', { token, userId: user.id, expiresAt });
  db.update('users', user.id, { lastLogin: now() });
  const { passwordHash, ...safe } = user;
  res.json({ token, user: safe, expiresAt });
});

app.post('/api/auth/logout', auth, (req, res) => {
  db.data.sessions = (db.data.sessions || []).filter(s => s.token !== req.token);
  db.save();
  res.json({ success: true });
});

app.get('/api/auth/me', auth, (req, res) => {
  const { passwordHash, ...safe } = req.user;
  res.json({ user: safe });
});

// =================== MODULE-SPECIFIC ROUTES (must be before generic CRUD) ===================

// Dashboard
app.get('/api/dashboard/stats', auth, (req, res) => {
  const products = db.findAll('products');
  const orders = db.findAll('sales_orders');
  const customers = db.findAll('customers');
  const employees = db.findAll('employees');
  const tickets = db.findAll('support_tickets');
  const projects = db.findAll('projects');
  const inventoryValue = products.reduce((s, p) => s + (p.stock * p.cost), 0);
  const totalRevenue = orders.reduce((s, o) => s + Number(o.total || 0), 0);
  const openTickets = tickets.filter(t => t.status !== 'closed').length;
  const activeProjects = projects.filter(p => p.status === 'active').length;
  const lowStockProducts = products.filter(p => p.stock <= p.minStock).length;
  res.json({
    products: { total: products.length, lowStock: lowStockProducts },
    orders: { total: orders.length, pending: orders.filter(o => o.status === 'pending').length, processing: orders.filter(o => o.status === 'processing').length, shipped: orders.filter(o => o.status === 'shipped').length },
    customers: { total: customers.length },
    employees: { total: employees.length, active: employees.filter(e => e.status === 'active').length },
    inventoryValue, totalRevenue, openTickets, activeProjects,
    recentOrders: orders.slice(-5).reverse(),
    lowStockProducts: products.filter(p => p.stock <= p.minStock).slice(0, 5),
    timestamp: now()
  });
});

// Inventory stats
app.get('/api/inventory/stats', auth, (req, res) => {
  const products = db.findAll('products');
  const totalValue = products.reduce((s, p) => s + (p.stock * p.cost), 0);
  const retailValue = products.reduce((s, p) => s + (p.stock * p.price), 0);
  const lowStock = products.filter(p => p.stock <= p.minStock);
  const outOfStock = products.filter(p => p.stock === 0);
  res.json({ totalProducts: products.length, totalValue, retailValue, lowStockCount: lowStock.length, outOfStockCount: outOfStock.length, lowStock, outOfStock });
});

// Sales stats
app.get('/api/sales/stats', auth, (req, res) => {
  const orders = db.findAll('sales_orders');
  const total = orders.reduce((s, o) => s + Number(o.total || 0), 0);
  const pending = orders.filter(o => o.status === 'pending').length;
  const processing = orders.filter(o => o.status === 'processing').length;
  const shipped = orders.filter(o => o.status === 'shipped').length;
  const byMonth = {};
  orders.forEach(o => { const m = o.orderDate?.substring(0, 7); if (m) byMonth[m] = (byMonth[m] || 0) + Number(o.total || 0); });
  res.json({ totalRevenue: total, orderCount: orders.length, pending, processing, shipped, byMonth });
});

// Purchasing stats
app.get('/api/purchases/stats', auth, (req, res) => {
  const orders = db.findAll('purchase_orders');
  const total = orders.reduce((s, o) => s + Number(o.total || 0), 0);
  res.json({ totalSpend: total, orderCount: orders.length, draft: orders.filter(o => o.status === 'draft').length, approved: orders.filter(o => o.status === 'approved').length, received: orders.filter(o => o.status === 'received').length });
});

// Accounting stats
app.get('/api/accounting/stats', auth, (req, res) => {
  const accounts = db.findAll('accounts');
  const assets = accounts.filter(a => a.type === 'asset').reduce((s, a) => s + a.balance, 0);
  const liabilities = accounts.filter(a => a.type === 'liability').reduce((s, a) => s + a.balance, 0);
  const equity = accounts.filter(a => a.type === 'equity').reduce((s, a) => s + a.balance, 0);
  const revenue = accounts.filter(a => a.type === 'income').reduce((s, a) => s + a.balance, 0);
  const expenses = accounts.filter(a => a.type === 'expense').reduce((s, a) => s + a.balance, 0);
  const profit = revenue - expenses;
  res.json({ assets, liabilities, equity, revenue, expenses, profit, totalAccounts: accounts.length });
});

// =================== AIS - ACCOUNTING INFORMATION SYSTEM ===================

// --- Fiscal Periods ---
app.get('/api/accounting/fiscal-periods', auth, (req, res) => {
  let periods = db.findAll('fiscal_periods');
  if (req.query.year) periods = periods.filter(p => p.year === parseInt(req.query.year));
  if (req.query.status) periods = periods.filter(p => p.status === req.query.status);
  res.json(periods.sort((a, b) => a.startDate.localeCompare(b.startDate)));
});
app.post('/api/accounting/fiscal-periods', auth, (req, res) => {
  const entry = { id: `fp-${Date.now()}`, ...req.body, status: 'open', createdAt: new Date().toISOString() };
  db.data.fiscal_periods.push(entry); db.save(); res.json(entry);
});
app.put('/api/accounting/fiscal-periods/:id/close', auth, (req, res) => {
  const period = (db.data.fiscal_periods || []).find(p => p.id === req.params.id);
  if (!period) return res.status(404).json({ error: 'Period not found' });
  if (period.status === 'closed') return res.status(400).json({ error: 'Period already closed' });
  // Validate: check for unposted/draft journal entries in this period
  const draftEntries = (db.data.journal_entries || []).filter(e => e.period === period.name && e.status === 'draft');
  if (draftEntries.length > 0) {
    return res.status(400).json({ error: `Cannot close period: ${draftEntries.length} draft journal entry(ies) must be posted or deleted first`, draftCount: draftEntries.length, entries: draftEntries.map(e => ({ id: e.id, number: e.number, description: e.description })) });
  }
  const pendingEntries = (db.data.journal_entries || []).filter(e => e.period === period.name && e.status === 'pending');
  if (pendingEntries.length > 0) {
    return res.status(400).json({ error: `Cannot close period: ${pendingEntries.length} pending journal entry(ies) must be approved first`, pendingCount: pendingEntries.length, entries: pendingEntries.map(e => ({ id: e.id, number: e.number, description: e.description })) });
  }
  // Validate trial balance is balanced
  const periodEntries = (db.data.journal_entries || []).filter(e => e.period === period.name && e.status === 'posted');
  const entryIds = new Set(periodEntries.map(e => e.id));
  const lines = (db.data.journal_lines || []).filter(l => entryIds.has(l.entryId));
  const totalDebits = lines.reduce((s, l) => s + (l.debit || 0), 0);
  const totalCredits = lines.reduce((s, l) => s + (l.credit || 0), 0);
  if (Math.abs(totalDebits - totalCredits) > 0.01) {
    return res.status(400).json({ error: `Cannot close period: trial balance not balanced (debits: ${totalDebits}, credits: ${totalCredits})` });
  }
  period.status = 'closed'; period.closedBy = req.user?.username || 'admin'; period.closedAt = new Date().toISOString();
  period.summary = { totalDebits, totalCredits, entryCount: periodEntries.length, lineCount: lines.length };
  db.save(); res.json(period);
});
app.put('/api/accounting/fiscal-periods/:id/reopen', auth, (req, res) => {
  const period = (db.data.fiscal_periods || []).find(p => p.id === req.params.id);
  if (!period) return res.status(404).json({ error: 'Period not found' });
  period.status = 'open'; period.closedBy = null; period.closedAt = null;
  db.save(); res.json(period);
});

// --- List Accounts (dedicated endpoint for drill-down etc.) ---
app.get('/api/accounting/accounts', auth, (req, res) => {
  res.json(db.findAll('accounts'));
});

// --- Journal Entry Engine (Double-Entry) ---
app.get('/api/accounting/journal-entries', auth, (req, res) => {
  let entries = db.findAll('journal_entries');
  if (req.query.status) entries = entries.filter(e => e.status === req.query.status);
  if (req.query.period) entries = entries.filter(e => e.period === req.query.period);
  if (req.query.from) entries = entries.filter(e => e.date >= req.query.from);
  if (req.query.to) entries = entries.filter(e => e.date <= req.query.to);
  const enriched = entries.map(e => {
    const lines = (db.data.journal_lines || []).filter(l => l.entryId === e.id);
    return { ...e, lines, totalDebit: lines.reduce((s, l) => s + (l.debit || 0), 0), totalCredit: lines.reduce((s, l) => s + (l.credit || 0), 0) };
  });
  res.json(enriched.sort((a, b) => (b.date || '').localeCompare(a.date || '')));
});
app.post('/api/accounting/journal-entries', auth, (req, res) => {
  const { date, description, reference, period, lines, sourceModule, sourceId } = req.body;
  if (!lines || lines.length < 2) return res.status(400).json({ error: 'Journal entry requires at least 2 lines' });
  const totalDebit = lines.reduce((s, l) => s + (l.debit || 0), 0);
  const totalCredit = lines.reduce((s, l) => s + (l.credit || 0), 0);
  if (Math.abs(totalDebit - totalCredit) > 0.01) return res.status(400).json({ error: `Debits (${totalDebit}) must equal Credits (${totalCredit})` });
  // Enforce period close
  const jePeriod = period || date?.substring(0, 7) || '';
  if (jePeriod) {
    const fp = (db.data.fiscal_periods || []).find(p => p.name === jePeriod || p.startDate?.substring(0, 7) === jePeriod);
    if (fp && fp.status === 'closed') return res.status(400).json({ error: `Period ${fp.name} is closed. Reopen it first.` });
  }
  const entryNum = `JE-${Date.now()}`;
  const entry = { id: `je-${Date.now()}-${Math.random().toString(36).slice(2,5)}`, number: entryNum, date, description, reference: reference || '', period: jePeriod, status: 'draft', sourceModule: sourceModule || 'manual', sourceId: sourceId || null, createdBy: req.user?.username || 'admin', createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() };
  db.data.journal_entries.push(entry);
  lines.forEach(l => {
    db.data.journal_lines.push({ id: `jl-${Date.now()}-${Math.random().toString(36).slice(2,5)}`, entryId: entry.id, accountId: l.accountId, accountCode: l.accountCode || '', description: l.description || '', debit: l.debit || 0, credit: l.credit || 0, createdAt: new Date().toISOString() });
  });
  db.save();
  if (typeof auditLog === 'function') auditLog(req.user?.id || 'system', req.user?.username || 'admin', 'CREATE', 'journal_entries', entry.id, entry.description, null, entry);
  res.json(entry);
});
app.put('/api/accounting/journal-entries/:id/approve', auth, (req, res) => {
  const entry = (db.data.journal_entries || []).find(e => e.id === req.params.id);
  if (!entry) return res.status(404).json({ error: 'Entry not found' });
  if (entry.status === 'posted') return res.status(400).json({ error: 'Entry already posted' });
  // Enforce period close
  if (entry.period) {
    const fp = (db.data.fiscal_periods || []).find(p => p.startDate?.substring(0, 7) === entry.period);
    if (fp && fp.status === 'closed') return res.status(400).json({ error: `Period ${fp.name} is closed. Reopen it first.` });
  }
  entry.status = 'posted'; entry.approvedBy = req.user?.username || 'admin'; entry.postedAt = new Date().toISOString();
  // Update account balances
  const lines = (db.data.journal_lines || []).filter(l => l.entryId === entry.id);
  lines.forEach(l => {
    const acct = (db.data.accounts || []).find(a => a.id === l.accountId);
    if (acct) { acct.balance = (acct.balance || 0) + (l.debit || 0) - (l.credit || 0); }
  });
  db.save();
  if (typeof auditLog === 'function') auditLog(req.user?.id || 'system', req.user?.username || 'admin', 'UPDATE', 'journal_entries', entry.id, `Approved: ${entry.description}`, { status: 'draft' }, { status: 'posted', approvedBy: entry.approvedBy });
  res.json(entry);
});
app.put('/api/accounting/journal-entries/:id/reject', auth, (req, res) => {
  const entry = (db.data.journal_entries || []).find(e => e.id === req.params.id);
  if (!entry) return res.status(404).json({ error: 'Entry not found' });
  entry.status = 'rejected'; entry.rejectedBy = req.user?.username || 'admin';
  db.save(); res.json(entry);
});
app.delete('/api/accounting/journal-entries/:id', auth, (req, res) => {
  const entry = (db.data.journal_entries || []).find(e => e.id === req.params.id);
  if (entry && entry.status === 'posted') return res.status(400).json({ error: 'Cannot delete posted entry' });
  db.data.journal_entries = (db.data.journal_entries || []).filter(e => e.id !== req.params.id);
  db.data.journal_lines = (db.data.journal_lines || []).filter(l => l.entryId !== req.params.id);
  db.save(); res.json({ success: true });
});

// --- Auto-post helper functions ---
function postJournalAuto(date, description, reference, period, lines, sourceModule, sourceId, username) {
  const entryNum = `JE-${Date.now()}`;
  const entry = { id: `je-${Date.now()}-${Math.random().toString(36).slice(2,5)}`, number: entryNum, date, description, reference: reference || '', period: period || date?.substring(0, 7) || '', status: 'posted', sourceModule: sourceModule || 'auto', sourceId: sourceId || null, createdBy: username || 'system', approvedBy: username || 'system', postedAt: new Date().toISOString(), createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() };
  db.data.journal_entries.push(entry);
  lines.forEach(l => {
    db.data.journal_lines.push({ id: `jl-${Date.now()}-${Math.random().toString(36).slice(2,5)}`, entryId: entry.id, accountId: l.accountId, accountCode: l.accountCode || '', description: l.description || '', debit: l.debit || 0, credit: l.credit || 0, createdAt: new Date().toISOString() });
    const acct = (db.data.accounts || []).find(a => a.id === l.accountId);
    if (acct) acct.balance = (acct.balance || 0) + (l.debit || 0) - (l.credit || 0);
  });
  db.save();
  eventBus.emitEvent('journal.posted', { module: sourceModule || 'accounting', entityId: entry.id, username: username || 'system', data: { entryNumber: entryNum, description, totalDebit: lines.reduce((s,l) => s + (l.debit||0), 0), totalCredit: lines.reduce((s,l) => s + (l.credit||0), 0), sourceModule, lineCount: lines.length } });
  return entry;
}

// --- Accounts Receivable Subledger ---
app.get('/api/accounting/ar/invoices', auth, (req, res) => {
  let invoices = db.findAll('invoices');
  if (req.query.status) invoices = invoices.filter(i => i.status === req.query.status);
  if (req.query.customerId) invoices = invoices.filter(i => i.customerId === req.query.customerId);
  const enriched = invoices.map(inv => {
    const payments = (db.data.payments || []).filter(p => p.invoiceId === inv.id);
    const paid = payments.reduce((s, p) => s + (p.amount || 0), 0);
    return { ...inv, paid, balance: (inv.amount || inv.total || 0) - paid };
  });
  res.json(enriched);
});
app.get('/api/accounting/ar/aging', auth, (req, res) => {
  const invoices = db.findAll('invoices');
  const today = new Date();
  const aging = { current: 0, days30: 0, days60: 0, days90: 0, over90: 0, count: { current: 0, days30: 0, days60: 0, days90: 0, over90: 0 } };
  invoices.forEach(inv => {
    if (inv.status === 'paid' || inv.status === 'cancelled') return;
    const due = inv.dueDate ? new Date(inv.dueDate) : new Date(inv.date || Date.now());
    const diff = Math.floor((today - due) / (1000 * 60 * 60 * 24));
    const amt = inv.amount || inv.total || 0;
    if (diff <= 0) { aging.current += amt; aging.count.current++; }
    else if (diff <= 30) { aging.days30 += amt; aging.count.days30++; }
    else if (diff <= 60) { aging.days60 += amt; aging.count.days60++; }
    else if (diff <= 90) { aging.days90 += amt; aging.count.days90++; }
    else { aging.over90 += amt; aging.count.over90++; }
  });
  aging.total = aging.current + aging.days30 + aging.days60 + aging.days90 + aging.over90;
  res.json(aging);
});
app.post('/api/accounting/ar/invoices', auth, (req, res) => {
  const inv = { id: `inv-${Date.now()}-${Math.random().toString(36).slice(2,5)}`, number: `INV-${Date.now()}`, ...req.body, status: 'pending', createdAt: new Date().toISOString() };
  db.data.invoices.push(inv); db.save();
  // Auto-post: Dr Accounts Receivable, Cr Revenue (use selected account or default)
  const arAcct = (db.data.accounts || []).find(a => a.subtype === 'receivable');
  const revAcct = inv.revenueAccountId ? (db.data.accounts || []).find(a => a.id === inv.revenueAccountId) : (db.data.accounts || []).find(a => a.subtype === 'revenue');
  if (arAcct && revAcct) {
    const amt = inv.amount || inv.total || 0;
    postJournalAuto(inv.date || new Date().toISOString().slice(0,10), `Invoice ${inv.number} - ${inv.customerName||'Customer'}`, inv.number, inv.date?.substring(0,7), [
      { accountId: arAcct.id, accountCode: arAcct.code, description: `AR - ${inv.customerName||''}`, debit: amt, credit: 0 },
      { accountId: revAcct.id, accountCode: revAcct.code, description: `Revenue - ${inv.number}`, debit: 0, credit: amt }
    ], 'ar', inv.id, req.user?.username || 'admin');
  }
  res.json(inv);
});
app.post('/api/accounting/ar/payments', auth, (req, res) => {
  const pmt = { id: `pmt-${Date.now()}-${Math.random().toString(36).slice(2,5)}`, number: `PMT-${Date.now()}`, ...req.body, createdAt: new Date().toISOString() };
  db.data.payments.push(pmt); db.save();
  // Auto-post: Dr Cash, Cr Accounts Receivable
  const cashAcct = (db.data.accounts || []).find(a => a.subtype === 'bank');
  const arAcct = (db.data.accounts || []).find(a => a.subtype === 'receivable');
  if (cashAcct && arAcct) {
    const amt = pmt.amount || 0;
    postJournalAuto(pmt.date || new Date().toISOString().slice(0,10), `Payment received - ${pmt.customerName||pmt.invoiceNumber||''}`, pmt.number, pmt.date?.substring(0,7), [
      { accountId: cashAcct.id, accountCode: cashAcct.code, description: `Cash receipt`, debit: amt, credit: 0 },
      { accountId: arAcct.id, accountCode: arAcct.code, description: `AR payment - ${pmt.invoiceNumber||''}`, debit: 0, credit: amt }
    ], 'ar_payment', pmt.id, req.user?.username || 'admin');
  }
  // Update invoice status if fully paid
  if (pmt.invoiceId) {
    const inv = (db.data.invoices || []).find(i => i.id === pmt.invoiceId);
    if (inv) {
      const totalPaid = (db.data.payments || []).filter(p => p.invoiceId === inv.id).reduce((s, p) => s + (p.amount || 0), 0);
      inv.paid = totalPaid;
      inv.balance = (inv.amount || 0) - totalPaid;
      if (inv.balance <= 0.01) inv.status = 'paid';
      db.save();
    }
  }
  res.json(pmt);
});

// --- Accounts Payable Subledger ---
app.get('/api/accounting/ap/bills', auth, (req, res) => {
  let bills = db.findAll('bills');
  if (req.query.status) bills = bills.filter(b => b.status === req.query.status);
  if (req.query.supplierId) bills = bills.filter(b => b.supplierId === req.query.supplierId);
  const enriched = bills.map(bill => {
    const payments = (db.data.payments || []).filter(p => p.billId === bill.id);
    const paid = payments.reduce((s, p) => s + (p.amount || 0), 0);
    return { ...bill, paid, balance: (bill.amount || bill.total || 0) - paid };
  });
  res.json(enriched);
});
app.get('/api/accounting/ap/aging', auth, (req, res) => {
  const bills = db.findAll('bills');
  const today = new Date();
  const aging = { current: 0, days30: 0, days60: 0, days90: 0, over90: 0, count: { current: 0, days30: 0, days60: 0, days90: 0, over90: 0 } };
  bills.forEach(bill => {
    if (bill.status === 'paid' || bill.status === 'cancelled') return;
    const due = bill.dueDate ? new Date(bill.dueDate) : new Date(bill.date || Date.now());
    const diff = Math.floor((today - due) / (1000 * 60 * 60 * 24));
    const amt = bill.amount || bill.total || 0;
    if (diff <= 0) { aging.current += amt; aging.count.current++; }
    else if (diff <= 30) { aging.days30 += amt; aging.count.days30++; }
    else if (diff <= 60) { aging.days60 += amt; aging.count.days60++; }
    else if (diff <= 90) { aging.days90 += amt; aging.count.days90++; }
    else { aging.over90 += amt; aging.count.over90++; }
  });
  aging.total = aging.current + aging.days30 + aging.days60 + aging.days90 + aging.over90;
  res.json(aging);
});
app.post('/api/accounting/ap/bills', auth, (req, res) => {
  const bill = { id: `bill-${Date.now()}-${Math.random().toString(36).slice(2,5)}`, number: `BILL-${Date.now()}`, ...req.body, status: 'pending', createdAt: new Date().toISOString() };
  db.data.bills.push(bill); db.save();
  // Auto-post: Dr Expense/Inventory (use selected account or default), Cr Accounts Payable
  const apAcct = (db.data.accounts || []).find(a => a.subtype === 'payable');
  const debitAcct = bill.expenseAccountId ? (db.data.accounts || []).find(a => a.id === bill.expenseAccountId) : ((db.data.accounts || []).find(a => a.subtype === 'inventory') || (db.data.accounts || []).find(a => a.code === '5000') || (db.data.accounts || []).find(a => a.subtype === 'operating'));
  if (apAcct && debitAcct) {
    const amt = bill.amount || bill.total || 0;
    postJournalAuto(bill.date || new Date().toISOString().slice(0,10), `Bill ${bill.number} - ${bill.supplierName||'Supplier'}`, bill.number, bill.date?.substring(0,7), [
      { accountId: debitAcct.id, accountCode: debitAcct.code, description: `Expense - ${bill.supplierName||''}`, debit: amt, credit: 0 },
      { accountId: apAcct.id, accountCode: apAcct.code, description: `AP - ${bill.number}`, debit: 0, credit: amt }
    ], 'ap', bill.id, req.user?.username || 'admin');
  }
  res.json(bill);
});
app.post('/api/accounting/ap/payments', auth, (req, res) => {
  const pmt = { id: `apmt-${Date.now()}-${Math.random().toString(36).slice(2,5)}`, number: `APMT-${Date.now()}`, ...req.body, createdAt: new Date().toISOString() };
  db.data.payments.push(pmt); db.save();
  // Auto-post: Dr Accounts Payable, Cr Cash
  const cashAcct = (db.data.accounts || []).find(a => a.subtype === 'bank');
  const apAcct = (db.data.accounts || []).find(a => a.subtype === 'payable');
  if (cashAcct && apAcct) {
    const amt = pmt.amount || 0;
    postJournalAuto(pmt.date || new Date().toISOString().slice(0,10), `Payment to ${pmt.supplierName||pmt.billNumber||''}`, pmt.number, pmt.date?.substring(0,7), [
      { accountId: apAcct.id, accountCode: apAcct.code, description: `AP payment - ${pmt.billNumber||''}`, debit: amt, credit: 0 },
      { accountId: cashAcct.id, accountCode: cashAcct.code, description: `Cash payment`, debit: 0, credit: amt }
    ], 'ap_payment', pmt.id, req.user?.username || 'admin');
  }
  // Update bill status if fully paid
  if (pmt.billId) {
    const bill = (db.data.bills || []).find(b => b.id === pmt.billId);
    if (bill) {
      const totalPaid = (db.data.payments || []).filter(p => p.billId === bill.id).reduce((s, p) => s + (p.amount || 0), 0);
      bill.paid = totalPaid;
      bill.balance = (bill.amount || bill.total || 0) - totalPaid;
      if (bill.balance <= 0.01) bill.status = 'paid';
      db.save();
    }
  }
  // Update bank_accounts balance
  if (pmt.bankAccountId) {
    const ba = (db.data.bank_accounts || []).find(a => a.id === pmt.bankAccountId);
    if (ba) { ba.balance = (ba.balance || 0) - (pmt.amount || 0); db.save(); }
  }
  res.json(pmt);
});

// --- Trial Balance ---
app.get('/api/accounting/trial-balance', auth, (req, res) => {
  const accounts = db.findAll('accounts');
  const period = req.query.period;
  let journalLines = db.findAll('journal_lines');
  // If period filter, recalculate from journal entries for that period
  if (period) {
    const entries = (db.data.journal_entries || []).filter(e => e.period === period && e.status === 'posted');
    const entryIds = new Set(entries.map(e => e.id));
    journalLines = journalLines.filter(l => entryIds.has(l.entryId));
  }
  const balances = {};
  accounts.forEach(a => { balances[a.id] = { id: a.id, code: a.code, name: a.name, type: a.type, subtype: a.subtype, debit: 0, credit: 0, balance: 0 }; });
  journalLines.forEach(l => {
    if (balances[l.accountId]) {
      balances[l.accountId].debit += (l.debit || 0);
      balances[l.accountId].credit += (l.credit || 0);
      balances[l.accountId].balance = balances[l.accountId].debit - balances[l.accountId].credit;
    }
  });
  const rows = Object.values(balances).filter(b => Math.abs(b.debit) > 0.01 || Math.abs(b.credit) > 0.01 || Math.abs(b.balance) > 0.01);
  const totalDebit = rows.reduce((s, r) => s + Math.max(0, r.debit), 0);
  const totalCredit = rows.reduce((s, r) => s + Math.max(0, r.credit), 0);
  res.json({ accounts: rows, totalDebit, totalCredit, balanced: Math.abs(totalDebit - totalCredit) < 0.01, period: period || 'All' });
});

// --- Income Statement ---
app.get('/api/accounting/income-statement', auth, (req, res) => {
  const period = req.query.period;
  const accounts = db.findAll('accounts');
  const incomes = accounts.filter(a => a.type === 'income');
  const expenses = accounts.filter(a => a.type === 'expense');
  const totalIncome = incomes.reduce((s, a) => s - (a.balance || 0), 0);
  const totalExpense = expenses.reduce((s, a) => s + (a.balance || 0), 0);
  const netIncome = totalIncome - totalExpense;
  res.json({ incomes: incomes.filter(a => Math.abs(a.balance) > 0.01), expenses: expenses.filter(a => Math.abs(a.balance) > 0.01), totalIncome, totalExpense, netIncome, period: period || 'All' });
});

// --- Balance Sheet ---
app.get('/api/accounting/balance-sheet', auth, (req, res) => {
  const accounts = db.findAll('accounts');
  const assets = accounts.filter(a => a.type === 'asset');
  const liabilities = accounts.filter(a => a.type === 'liability');
  const equity = accounts.filter(a => a.type === 'equity');
  const incomes = accounts.filter(a => a.type === 'income');
  const expenses = accounts.filter(a => a.type === 'expense');
  const totalAssets = assets.reduce((s, a) => s + (a.balance || 0), 0);
  const totalLiabilities = liabilities.reduce((s, a) => s - (a.balance || 0), 0);
  const totalEquity = equity.reduce((s, a) => s - (a.balance || 0), 0);
  const totalIncome = incomes.reduce((s, a) => s - (a.balance || 0), 0);
  const totalExpense = expenses.reduce((s, a) => s + (a.balance || 0), 0);
  const netIncome = totalIncome - totalExpense;
  const totalEquityAndIncome = totalEquity + netIncome;
  res.json({
    assets: assets.filter(a => Math.abs(a.balance) > 0.01).map(a => ({ ...a, displayBalance: a.balance })),
    liabilities: liabilities.filter(a => Math.abs(a.balance) > 0.01).map(a => ({ ...a, displayBalance: -(a.balance || 0) })),
    equity: equity.filter(a => Math.abs(a.balance) > 0.01).map(a => ({ ...a, displayBalance: -(a.balance || 0) })),
    netIncomeLine: { name: 'Net Income (Current Year)', displayBalance: netIncome },
    totalAssets, totalLiabilities, totalEquity, netIncome,
    balanced: Math.abs(totalAssets - (totalLiabilities + totalEquityAndIncome)) < 0.01
  });
});

// --- Cash Flow Statement ---
app.get('/api/accounting/cash-flow', auth, (req, res) => {
  const period = req.query.period;
  let lines = db.findAll('journal_lines');
  if (period) {
    const entries = (db.data.journal_entries || []).filter(e => e.period === period && e.status === 'posted');
    const entryIds = new Set(entries.map(e => e.id));
    lines = lines.filter(l => entryIds.has(l.entryId));
  }
  const accounts = db.findAll('accounts');
  const cashAccts = accounts.filter(a => a.subtype === 'bank').map(a => a.id);
  let operating = 0, investing = 0, financing = 0;
  const operatingItems = [], investingItems = [], financingItems = [];
  lines.forEach(l => {
    const acct = accounts.find(a => a.id === l.accountId);
    if (!acct) return;
    const amt = (l.debit || 0) - (l.credit || 0);
    if (acct.type === 'income') { operating += amt; operatingItems.push({ description: l.description, amount: amt }); }
    else if (acct.type === 'expense') { operating -= amt; operatingItems.push({ description: l.description, amount: -amt }); }
    else if (acct.subtype === 'fixed') { investing += amt; investingItems.push({ description: l.description, amount: amt }); }
    else if (acct.subtype === 'loan') { financing += amt; financingItems.push({ description: l.description, amount: amt }); }
    else if (acct.type === 'equity') { financing += amt; financingItems.push({ description: l.description, amount: amt }); }
  });
  const netCashFlow = operating + investing + financing;
  res.json({ operating, investing, financing, netCashFlow, operatingItems, investingItems, financingItems, period: period || 'All' });
});

// =================== IFRS-COMPLIANT FINANCIAL STATEMENTS ===================

// --- IFRS Classified Balance Sheet (IAS 1.54 / IFRS 18 role-210000) ---
app.get('/api/accounting/ifrs/balance-sheet', auth, (req, res) => {
  const accounts = db.findAll('accounts');
  const bs = accounts.filter(a => a.type === 'asset' || a.type === 'liability' || a.type === 'equity');

  // Classify by current/non-current
  const currentAssets = bs.filter(a => a.type === 'asset' && a.current_non_current === 'current');
  const nonCurrentAssets = bs.filter(a => a.type === 'asset' && a.current_non_current === 'non-current');
  const currentLiabilities = bs.filter(a => a.type === 'liability' && a.current_non_current === 'current');
  const nonCurrentLiabilities = bs.filter(a => a.type === 'liability' && a.current_non_current === 'non-current');
  const equity = bs.filter(a => a.type === 'equity');

  // Aggregate by IFRS element
  const aggregateByElement = (accts) => {
    const groups = {};
    accts.forEach(a => {
      const el = a.ifrs_element || a.code;
      if (!groups[el]) groups[el] = { element: el, name: a.name, balance: 0, accounts: [] };
      groups[el].balance += (a.balance || 0);
      groups[el].accounts.push({ code: a.code, name: a.name, balance: a.balance });
    });
    return Object.values(groups);
  };

  const totalCurrentAssets = currentAssets.reduce((s, a) => s + (a.balance || 0), 0);
  const totalNonCurrentAssets = nonCurrentAssets.reduce((s, a) => s + (a.balance || 0), 0);
  const totalAssets = totalCurrentAssets + totalNonCurrentAssets;
  const totalCurrentLiabilities = currentLiabilities.reduce((s, a) => s - (a.balance || 0), 0);
  const totalNonCurrentLiabilities = nonCurrentLiabilities.reduce((s, a) => s - (a.balance || 0), 0);
  const totalLiabilities = totalCurrentLiabilities + totalNonCurrentLiabilities;
  const totalEquity = equity.reduce((s, a) => s - (a.balance || 0), 0);

  // Net income from P&L accounts
  const incomes = accounts.filter(a => a.type === 'income');
  const expenses = accounts.filter(a => a.type === 'expense');
  const totalIncome = incomes.reduce((s, a) => s - (a.balance || 0), 0);
  const totalExpense = expenses.reduce((s, a) => s + (a.balance || 0), 0);
  const netIncome = totalIncome - totalExpense;
  const totalEquityAndIncome = totalEquity + netIncome;

  res.json({
    currentAssets: aggregateByElement(currentAssets),
    nonCurrentAssets: aggregateByElement(nonCurrentAssets),
    totalCurrentAssets, totalNonCurrentAssets, totalAssets,
    currentLiabilities: aggregateByElement(currentLiabilities),
    nonCurrentLiabilities: aggregateByElement(nonCurrentLiabilities),
    totalCurrentLiabilities, totalNonCurrentLiabilities, totalLiabilities,
    equity: aggregateByElement(equity),
    totalEquity, netIncome,
    totalEquityAndIncome,
    balanced: Math.abs(totalAssets - totalEquityAndIncome - totalLiabilities) < 0.01,
    standard: 'IAS 1 / IFRS 18'
  });
});

// --- IFRS 18 Statement of Profit or Loss (by categories: Operating/Investing/Financing/Tax) ---
app.get('/api/accounting/ifrs/profit-or-loss', auth, (req, res) => {
  const accounts = db.findAll('accounts');
  const pl = accounts.filter(a => a.type === 'income' || a.type === 'expense');

  const operating = pl.filter(a => a.ifrs_category === 'operating');
  const investing = pl.filter(a => a.ifrs_category === 'investing');
  const financing = pl.filter(a => a.ifrs_category === 'financing');
  const incomeTaxes = pl.filter(a => a.ifrs_category === 'income_taxes');

  const sumCategory = (accts) => {
    const revenue = accts.filter(a => a.type === 'income').reduce((s, a) => s - (a.balance || 0), 0);
    const expense = accts.filter(a => a.type === 'expense').reduce((s, a) => s + (a.balance || 0), 0);
    return { revenue, expense, net: revenue - expense };
  };

  const op = sumCategory(operating);
  const inv = sumCategory(investing);
  const fin = sumCategory(financing);
  const tax = sumCategory(incomeTaxes);

  const profitBeforeFinancingAndTax = op.net + inv.net;
  const profitBeforeTax = profitBeforeFinancingAndTax + fin.net;
  const profitForPeriod = profitBeforeTax + tax.net;

  // Aggregate by IFRS element within each category
  const aggregateByElement = (accts) => {
    const groups = {};
    accts.forEach(a => {
      const el = a.ifrs_element || a.code;
      if (!groups[el]) groups[el] = { element: el, name: a.name, balance: 0, type: a.type };
      groups[el].balance += (a.balance || 0);
    });
    return Object.values(groups).map(g => ({
      ...g,
      displayBalance: g.type === 'income' ? -(g.balance || 0) : (g.balance || 0)
    }));
  };

  // IFRS 18 mandatory subtotals
  const grossProfit = op.revenue - (pl.filter(a => a.ifrs_element === 'ifrs-full_CostOfSales').reduce((s, a) => s + (a.balance || 0), 0));

  res.json({
    operating: { items: aggregateByElement(operating), ...op },
    investing: { items: aggregateByElement(investing), ...inv },
    financing: { items: aggregateByElement(financing), ...fin },
    incomeTaxes: { items: aggregateByElement(incomeTaxes), ...tax },
    grossProfit,
    operatingProfit: op.net,
    profitBeforeFinancingAndIncomeTaxes: profitBeforeFinancingAndTax,
    profitBeforeTax,
    profitForPeriod,
    standard: 'IFRS 18'
  });
});

// --- IFRS Statement of Comprehensive Income ---
app.get('/api/accounting/ifrs/comprehensive-income', auth, (req, res) => {
  const accounts = db.findAll('accounts');
  const incomes = accounts.filter(a => a.type === 'income');
  const expenses = accounts.filter(a => a.type === 'expense');
  const totalIncome = incomes.reduce((s, a) => s - (a.balance || 0), 0);
  const totalExpense = expenses.reduce((s, a) => s + (a.balance || 0), 0);
  const profitForPeriod = totalIncome - totalExpense;

  // OCI items would come from equity revaluations, FX translation, etc.
  // For now, OCI = 0 (no fair value adjustments in demo data)
  const otherComprehensiveIncome = 0;
  const comprehensiveIncome = profitForPeriod + otherComprehensiveIncome;

  res.json({
    profitForPeriod,
    otherComprehensiveIncome,
    comprehensiveIncome,
    attributableToOwners: comprehensiveIncome,
    attributableToNCI: 0,
    standard: 'IAS 1 / IFRS 18'
  });
});

// --- IFRS Statement of Changes in Equity ---
app.get('/api/accounting/ifrs/changes-in-equity', auth, (req, res) => {
  const accounts = db.findAll('accounts');
  const equity = accounts.filter(a => a.type === 'equity');
  const incomes = accounts.filter(a => a.type === 'income');
  const expenses = accounts.filter(a => a.type === 'expense');
  const totalIncome = incomes.reduce((s, a) => s - (a.balance || 0), 0);
  const totalExpense = expenses.reduce((s, a) => s + (a.balance || 0), 0);
  const profitForPeriod = totalIncome - totalExpense;

  const components = equity.map(a => ({
    element: a.ifrs_element || a.code,
    name: a.name,
    code: a.code,
    openingBalance: -(a.balance || 0),
    profitForPeriod: a.subtype === 'current' ? profitForPeriod : 0,
    otherComprehensiveIncome: 0,
    totalComprehensiveIncome: a.subtype === 'current' ? profitForPeriod : 0,
    closingBalance: -(a.balance || 0)
  }));

  const totalOpening = components.reduce((s, c) => s + c.openingBalance, 0);
  const totalClosing = components.reduce((s, c) => s + c.closingBalance, 0);

  res.json({
    components,
    totalOpening,
    totalProfitForPeriod: profitForPeriod,
    totalOCI: 0,
    totalComprehensiveIncome: profitForPeriod,
    totalClosing,
    standard: 'IAS 1'
  });
});

// --- IFRS Cash Flow Statement (Indirect Method - IAS 7) ---
app.get('/api/accounting/ifrs/cash-flow', auth, (req, res) => {
  const accounts = db.findAll('accounts');
  const lines = db.findAll('journal_lines');
  const entries = (db.data.journal_entries || []).filter(e => e.status === 'posted');
  const postedIds = new Set(entries.map(e => e.id));
  const postedLines = lines.filter(l => postedIds.has(l.entryId));

  // Net profit
  const incomes = accounts.filter(a => a.type === 'income');
  const expenses = accounts.filter(a => a.type === 'expense');
  const totalIncome = incomes.reduce((s, a) => s - (a.balance || 0), 0);
  const totalExpense = expenses.reduce((s, a) => s + (a.balance || 0), 0);
  const netProfit = totalIncome - totalExpense;

  // Adjustments for non-cash items
  const depreciation = accounts.find(a => a.code === '6400');
  const depreciationAmount = depreciation ? (depreciation.balance || 0) : 0;

  // Working capital changes
  const ar = accounts.find(a => a.subtype === 'receivable');
  const inv = accounts.filter(a => a.subtype === 'inventory');
  const ap = accounts.find(a => a.subtype === 'payable');
  const accrued = accounts.find(a => a.subtype === 'accrued');

  const changeInReceivables = ar ? -(ar.balance || 0) : 0;
  const changeInInventory = inv.reduce((s, a) => s - (a.balance || 0), 0);
  const changeInPayables = ap ? (ap.balance || 0) : 0;
  const changeInAccrued = accrued ? (accrued.balance || 0) : 0;

  const operatingActivities = netProfit + depreciationAmount + changeInReceivables + changeInInventory + changeInPayables + changeInAccrued;

  // Investing: fixed asset purchases (from journal entries)
  const investingItems = postedLines.filter(l => {
    const acct = accounts.find(a => a.id === l.accountId);
    return acct && acct.subtype === 'fixed' && l.debit > 0;
  }).map(l => ({ description: l.description, amount: -(l.debit || 0) }));
  const investingActivities = investingItems.reduce((s, i) => s + i.amount, 0);

  // Financing: loan changes
  const loans = accounts.filter(a => a.subtype === 'loan');
  const financingActivities = loans.reduce((s, a) => s - (a.balance || 0), 0);

  const netChange = operatingActivities + investingActivities + financingActivities;
  const cashAccounts = accounts.filter(a => a.subtype === 'bank');
  const closingCash = cashAccounts.reduce((s, a) => s + (a.balance || 0), 0);
  const openingCash = closingCash - netChange;

  res.json({
    operating: {
      netProfit,
      adjustments: {
        depreciation: depreciationAmount,
        changeInReceivables, changeInInventory, changeInPayables, changeInAccrued
      },
      total: operatingActivities
    },
    investing: { items: investingItems, total: investingActivities },
    financing: { total: financingActivities },
    netChange, openingCash, closingCash,
    standard: 'IAS 7'
  });
});

// =================== IFRS DISCLOSURE NOTES ===================

// --- IFRS Disclosure Notes (IAS 1.110-116) ---
app.get('/api/accounting/ifrs/disclosures', auth, (req, res) => {
  const accounts = db.findAll('accounts');
  const fp = db.findAll('fiscal_periods');
  const openPeriods = fp.filter(p => p.status === 'open');
  const closedPeriods = fp.filter(p => p.status === 'closed');

  // Significant accounting policies
  const policies = {
    basisOfPreparation: 'These financial statements have been prepared in accordance with IFRS Accounting Standards as issued by the IASB.',
    goingConcern: 'The directors have assessed the company\'s ability to continue as a going concern and are satisfied that the company has the resources to continue for the foreseeable future.',
    significantJudgments: 'The preparation of financial statements requires management to make judgments, estimates, and assumptions that affect the application of accounting policies and the reported amounts of assets, liabilities, income, and expenses.',
    revenueRecognition: 'Revenue is recognized when control of goods or services is transferred to the customer at the transaction price, in accordance with IFRS 15.',
    propertyPlantAndEquipment: 'Property, plant and equipment are measured at cost less accumulated depreciation and accumulated impairment losses, in accordance with IAS 16.',
    inventories: 'Inventories are measured at the lower of cost and net realizable value, in accordance with IAS 2.',
    employeeBenefits: 'Employee benefits are accrued as services are rendered, in accordance with IAS 19.',
    incomeTaxes: 'Current and deferred income taxes are recognized in profit or loss, in accordance with IAS 12.'
  };

  // Related party transactions
  const employees = db.findAll('employees');
  const relatedParties = employees.slice(0, 3).map(e => ({
    name: e.name || e.firstName + ' ' + e.lastName,
    relationship: 'Key management personnel',
    transactions: 'Employment fees, bonuses'
  }));

  // Financial instruments risk disclosures
  const riskDisclosures = {
    creditRisk: 'Credit risk arises from cash and cash equivalents and trade receivables. The company manages credit risk by maintaining relationships with creditworthy counterparties.',
    liquidityRisk: 'Liquidity risk is the risk that the company will not be able to meet its financial obligations. The company manages liquidity by maintaining adequate cash reserves.',
    marketRisk: 'Market risk includes interest rate risk and foreign currency risk. The company is exposed to interest rate risk on its borrowings.'
  };

  // Subsequent events
  const subsequentEvents = [
    { date: new Date().toISOString().slice(0,10), description: 'No events subsequent to the reporting period have occurred that require adjustment or disclosure.' }
  ];

  res.json({
    period: { open: openPeriods.length, closed: closedPeriods.length },
    significantAccountingPolicies: policies,
    relatedPartyDisclosures: relatedParties,
    financialInstrumentsRisk: riskDisclosures,
    subsequentEvents,
    standard: 'IAS 1.110-116'
  });
});

// --- Period Comparison (Current vs Prior Year) ---
app.get('/api/accounting/ifrs/period-comparison', auth, (req, res) => {
  const accounts = db.findAll('accounts');
  const fp = db.findAll('fiscal_periods');
  const currentYear = 2026;
  const priorYear = 2025;

  // Current year balances
  const currentBalances = {};
  accounts.forEach(a => { currentBalances[a.code] = { code: a.code, name: a.name, type: a.type, balance: a.balance || 0 }; });

  // For prior year, we simulate by adjusting current balances (in real system, would query prior year DB)
  const priorBalances = {};
  accounts.forEach(a => {
    let priorBalance = (a.balance || 0) * 0.85; // Simulated prior year (85% of current)
    if (a.type === 'income' || a.type === 'expense') priorBalance = (a.balance || 0) * 0.9;
    priorBalances[a.code] = { code: a.code, name: a.name, type: a.type, balance: priorBalance };
  });

  // Calculate variances
  const comparison = accounts.map(a => {
    const curr = currentBalances[a.code]?.balance || 0;
    const prev = priorBalances[a.code]?.balance || 0;
    const variance = curr - prev;
    const variancePct = prev !== 0 ? ((variance / Math.abs(prev)) * 100).toFixed(1) : '0.0';
    return { code: a.code, name: a.name, type: a.type, currentYear: curr, priorYear: prev, variance, variancePct: parseFloat(variancePct) };
  }).filter(c => Math.abs(c.currentYear) > 0.01 || Math.abs(c.priorYear) > 0.01);

  // Summary totals
  const totalAssets = comparison.filter(c => c.type === 'asset').reduce((s, c) => s + c.currentYear, 0);
  const priorTotalAssets = comparison.filter(c => c.type === 'asset').reduce((s, c) => s + c.priorYear, 0);
  const totalRevenue = comparison.filter(c => c.type === 'income').reduce((s, c) => s - c.currentYear, 0);
  const priorTotalRevenue = comparison.filter(c => c.type === 'income').reduce((s, c) => s - c.priorYear, 0);
  const totalExpenses = comparison.filter(c => c.type === 'expense').reduce((s, c) => s + c.currentYear, 0);
  const priorTotalExpenses = comparison.filter(c => c.type === 'expense').reduce((s, c) => s + c.priorYear, 0);

  res.json({
    currentYear, priorYear,
    items: comparison,
    summary: {
      totalAssets: { current: totalAssets, prior: priorTotalAssets, variance: totalAssets - priorTotalAssets },
      totalRevenue: { current: totalRevenue, prior: priorTotalRevenue, variance: totalRevenue - priorTotalRevenue },
      totalExpenses: { current: totalExpenses, prior: priorTotalExpenses, variance: totalExpenses - priorTotalExpenses }
    },
    standard: 'IAS 1.138'
  });
});

// --- IFRS 18 Specified Expenses by Nature ---
app.get('/api/accounting/ifrs/expenses-by-nature', auth, (req, res) => {
  const accounts = db.findAll('accounts');

  // Map accounts to nature categories per IFRS 18
  const natureMap = {
    '6000': { nature: 'EmployeeBenefitsExpense', label: 'Employee benefits expense' },
    '6400': { nature: 'DepreciationAndAmortisationExpense', label: 'Depreciation and amortisation expense' },
    '7200': { nature: 'ImpairmentLossRecognisedInProfitOrLoss', label: 'Impairment loss recognised in P&L' },
    '5000': { nature: 'RawMaterialsAndConsumablesUsed', label: 'Raw materials and consumables used' },
    '5010': { nature: 'RawMaterialsAndConsumablesUsed', label: 'Raw materials and consumables used' },
    '5020': { nature: 'OtherOperatingIncomeExpense', label: 'Other operating expenses' },
    '6100': { nature: 'OtherOperatingIncomeExpense', label: 'Other operating expenses' },
    '6200': { nature: 'OtherOperatingIncomeExpense', label: 'Other operating expenses' },
    '6300': { nature: 'OtherOperatingIncomeExpense', label: 'Other operating expenses' },
    '6500': { nature: 'OtherOperatingIncomeExpense', label: 'Other operating expenses' },
    '6600': { nature: 'SellingExpense', label: 'Selling expenses' },
    '6700': { nature: 'AdministrativeExpense', label: 'Administrative expenses' },
    '6800': { nature: 'AdministrativeExpense', label: 'Administrative expenses' },
    '6900': { nature: 'AdministrativeExpense', label: 'Administrative expenses' },
    '7000': { nature: 'FinanceCosts', label: 'Finance costs' },
    '7100': { nature: 'FinanceCosts', label: 'Finance costs' }
  };

  // Group by nature
  const groups = {};
  accounts.filter(a => a.type === 'expense').forEach(a => {
    const mapping = natureMap[a.code] || { nature: 'OtherOperatingIncomeExpense', label: 'Other expenses' };
    if (!groups[mapping.nature]) groups[mapping.nature] = { nature: mapping.nature, label: mapping.label, amount: 0, accounts: [] };
    groups[mapping.nature].amount += (a.balance || 0);
    groups[mapping.nature].accounts.push({ code: a.code, name: a.name, balance: a.balance });
  });

  const totalExpenses = Object.values(groups).reduce((s, g) => s + g.amount, 0);

  // Attribution by function (for disclosure)
  const costOfSales = accounts.filter(a => a.subtype === 'cogs').reduce((s, a) => s + (a.balance || 0), 0);
  const distribution = accounts.filter(a => a.ifrs_element === 'ifrs-full_SellingExpense').reduce((s, a) => s + (a.balance || 0), 0);
  const administrative = accounts.filter(a => a.ifrs_element === 'ifrs-full_AdministrativeExpense').reduce((s, a) => s + (a.balance || 0), 0);

  res.json({
    expensesByNature: Object.values(groups),
    totalExpensesByNature: totalExpenses,
    attributionByFunction: {
      costOfSales,
      distribution,
      administrative,
      other: totalExpenses - costOfSales - distribution - administrative
    },
    standard: 'IFRS 18.97-99'
  });
});

// --- IFRS 18 Management Performance Measures (MPM) ---
app.get('/api/accounting/ifrs/mpm', auth, (req, res) => {
  const accounts = db.findAll('accounts');
  const incomes = accounts.filter(a => a.type === 'income');
  const expenses = accounts.filter(a => a.type === 'expense');

  const totalIncome = incomes.reduce((s, a) => s - (a.balance || 0), 0);
  const totalExpense = expenses.reduce((s, a) => s + (a.balance || 0), 0);
  const profitForPeriod = totalIncome - totalExpense;

  // Common management performance measures
  const depreciation = accounts.find(a => a.code === '6400');
  const depreciationAmount = depreciation ? (depreciation.balance || 0) : 0;

  const ebitda = profitForPeriod + depreciationAmount;
  const operatingProfit = accounts.filter(a => a.ifrs_category === 'operating').reduce((s, a) => {
    return s + (a.type === 'income' ? -(a.balance || 0) : (a.balance || 0));
  }, 0);

  const mpm = [
    {
      name: 'EBITDA',
      description: 'Earnings before interest, taxes, depreciation and amortisation',
      value: ebitda,
      reconciliation: [
        { item: 'Profit for the period', amount: profitForPeriod },
        { item: 'Depreciation and amortisation', amount: depreciationAmount },
        { item: 'EBITDA', amount: ebitda }
      ]
    },
    {
      name: 'Operating Profit',
      description: 'Profit from operating activities before finance costs and income taxes',
      value: operatingProfit,
      reconciliation: [
        { item: 'Profit for the period', amount: profitForPeriod },
        { item: 'Finance costs', amount: accounts.filter(a => a.ifrs_category === 'financing' && a.type === 'expense').reduce((s, a) => s + (a.balance || 0), 0) },
        { item: 'Operating Profit', amount: operatingProfit }
      ]
    },
    {
      name: 'Free Cash Flow',
      description: 'Cash generated from operations less capital expenditure',
      value: 0,
      reconciliation: [
        { item: 'Operating cash flow', amount: 0 },
        { item: 'Capital expenditure', amount: 0 },
        { item: 'Free Cash Flow', amount: 0 }
      ]
    }
  ];

  res.json({
    profitForPeriod,
    mpm,
    standard: 'IFRS 18.139-146'
  });
});

// --- XBRL Instance Export ---
app.get('/api/accounting/ifrs/xbrl-export', auth, (req, res) => {
  const accounts = db.findAll('accounts');
  const bs = accounts.filter(a => a.type === 'asset' || a.type === 'liability' || a.type === 'equity');
  const pl = accounts.filter(a => a.type === 'income' || a.type === 'expense');

  const facts = [];

  // Balance sheet facts
  bs.forEach(a => {
    if (a.ifrs_element) {
      facts.push({
        concept: a.ifrs_element,
        contextRef: 'current',
        unit: 'ZAR',
        value: a.balance || 0,
        decimals: 0
      });
    }
  });

  // P&L facts
  pl.forEach(a => {
    if (a.ifrs_element) {
      facts.push({
        concept: a.ifrs_element,
        contextRef: 'duration',
        unit: 'ZAR',
        value: a.type === 'income' ? -(a.balance || 0) : (a.balance || 0),
        decimals: 0
      });
    }
  });

  // Calculate totals
  const totalAssets = bs.filter(a => a.type === 'asset').reduce((s, a) => s + (a.balance || 0), 0);
  const totalLiabilities = bs.filter(a => a.type === 'liability').reduce((s, a) => s - (a.balance || 0), 0);
  const totalEquity = bs.filter(a => a.type === 'equity').reduce((s, a) => s - (a.balance || 0), 0);

  res.json({
    schema: 'https://xbrl.ifrs.org/taxonomy/2025-03-27',
    namespace: 'ifrs-full',
    facts,
    totals: { totalAssets, totalLiabilities, totalEquity },
    factCount: facts.length,
    standard: 'IFRS Accounting Taxonomy 2025'
  });
});

// --- MIS Executive Summary ---
app.get('/api/accounting/mis-summary', auth, (req, res) => {
  const accounts = db.findAll('accounts');
  const entries = db.findAll('journal_entries');
  const postedIds = new Set(entries.filter(e => e.status === 'posted').map(e => e.id));
  const lines = (db.data.journal_lines || []).filter(l => postedIds.has(l.entryId));
  const currentYear = parseInt(req.query.year) || 2026;

  // Current year vs prior year
  const yearEntries = entries.filter(e => e.status === 'posted' && e.date && e.date.startsWith(String(currentYear)));
  const priorEntries = entries.filter(e => e.status === 'posted' && e.date && e.date.startsWith(String(currentYear - 1)));
  const getLines = (entries) => {
    const ids = new Set(entries.map(e => e.id));
    return (db.data.journal_lines || []).filter(l => ids.has(l.entryId));
  };
  const summarizeByType = (lineList) => {
    const result = { revenue: 0, cogs: 0, operatingExpenses: 0, financialExpenses: 0, tax: 0, otherIncome: 0 };
    lineList.forEach(l => {
      const acct = accounts.find(a => a.id === l.accountId);
      if (!acct) return;
      const amt = (l.debit || 0) - (l.credit || 0);
      if (acct.type === 'income') result.revenue += acct.subtype === 'other_income' ? amt : Math.abs(amt);
      else if (acct.subtype === 'cogs') result.cogs += Math.abs(amt);
      else if (acct.subtype === 'operating') result.operatingExpenses += Math.abs(amt);
      else if (acct.subtype === 'financial') result.financialExpenses += Math.abs(amt);
      else if (acct.subtype === 'income_taxes') result.tax += Math.abs(amt);
    });
    return result;
  };
  const current = summarizeByType(getLines(yearEntries));
  const prior = summarizeByType(getLines(priorEntries));
  current.grossProfit = current.revenue - current.cogs;
  current.operatingProfit = current.grossProfit - current.operatingExpenses;
  current.profitBeforeTax = current.operatingProfit - current.financialExpenses + current.otherIncome;
  current.netIncome = current.profitBeforeTax - current.tax;
  prior.grossProfit = prior.revenue - prior.cogs;
  prior.operatingProfit = prior.grossProfit - prior.operatingExpenses;
  prior.profitBeforeTax = prior.operatingProfit - prior.financialExpenses + prior.otherIncome;
  prior.netIncome = prior.profitBeforeTax - prior.tax;

  const pctChange = (curr, prev) => prev !== 0 ? Math.round(((curr - prev) / Math.abs(prev)) * 10000) / 100 : null;

  // Balance sheet totals
  const totalAssets = accounts.filter(a => a.type === 'asset').reduce((s, a) => s + (a.balance || 0), 0);
  const totalLiabilities = Math.abs(accounts.filter(a => a.type === 'liability').reduce((s, a) => s + (a.balance || 0), 0));
  const totalEquity = Math.abs(accounts.filter(a => a.type === 'equity').reduce((s, a) => s + (a.balance || 0), 0));
  const cash = accounts.filter(a => a.subtype === 'bank').reduce((s, a) => s + (a.balance || 0), 0);
  const receivables = accounts.filter(a => a.subtype === 'receivable').reduce((s, a) => s + (a.balance || 0), 0);
  const payables = accounts.filter(a => a.subtype === 'payable').reduce((s, a) => s + Math.abs(a.balance || 0), 0);

  // Monthly revenue trend
  const monthlyRevenue = {};
  for (let m = 1; m <= 12; m++) {
    const mm = String(m).padStart(2, '0');
    monthlyRevenue[`${currentYear}-${mm}`] = 0;
  }
  yearEntries.forEach(e => {
    if (!e.date || !e.period) return;
    const entryLines = lines.filter(l => l.entryId === e.id);
    entryLines.forEach(l => {
      const acct = accounts.find(a => a.id === l.accountId);
      if (acct && acct.type === 'income') {
        monthlyRevenue[e.period] = (monthlyRevenue[e.period] || 0) + Math.abs((l.debit || 0) - (l.credit || 0));
      }
    });
  });

  // Top 5 expense accounts
  const expenseAccounts = accounts.filter(a => a.type === 'expense').map(a => {
    const expLines = lines.filter(l => l.accountId === a.id);
    const total = expLines.reduce((s, l) => s + Math.abs((l.debit || 0) - (l.credit || 0)), 0);
    return { code: a.code, name: a.name, subtype: a.subtype, total };
  }).sort((a, b) => b.total - a.total).slice(0, 5);

  res.json({
    period: { year: currentYear, priorYear: currentYear - 1 },
    incomeStatement: {
      current, prior,
      changes: {
        revenue: pctChange(current.revenue, prior.revenue),
        grossProfit: pctChange(current.grossProfit, prior.grossProfit),
        operatingProfit: pctChange(current.operatingProfit, prior.operatingProfit),
        netIncome: pctChange(current.netIncome, prior.netIncome)
      }
    },
    balanceSheet: { totalAssets, totalLiabilities, totalEquity, cash, receivables, payables },
    monthlyRevenue,
    topExpenses: expenseAccounts,
    keyMetrics: {
      grossMargin: current.revenue ? Math.round((current.grossProfit / current.revenue) * 10000) / 100 : null,
      operatingMargin: current.revenue ? Math.round((current.operatingProfit / current.revenue) * 10000) / 100 : null,
      netMargin: current.revenue ? Math.round((current.netIncome / current.revenue) * 10000) / 100 : null,
      currentRatio: payables ? Math.round((cash + receivables) / payables * 100) / 100 : null,
      debtToEquity: totalEquity ? Math.round(totalLiabilities / totalEquity * 100) / 100 : null
    }
  });
});

// --- Financial Ratio Analysis ---
app.get('/api/accounting/ratios', auth, (req, res) => {
  const accounts = db.findAll('accounts');
  const lines = db.findAll('journal_lines');
  const entries = db.findAll('journal_entries');
  const postedIds = new Set(entries.filter(e => e.status === 'posted').map(e => e.id));
  const postedLines = lines.filter(l => postedIds.has(l.entryId));

  const total = (type) => accounts.filter(a => a.type === type).reduce((s, a) => s + (a.balance || 0), 0);
  const totalSub = (subtype) => accounts.filter(a => a.subtype === subtype).reduce((s, a) => s + (a.balance || 0), 0);

  const totalAssets = total('asset');
  const totalLiabilities = Math.abs(total('liability'));
  const totalEquity = Math.abs(total('equity'));
  const netIncome = Math.abs(total('income')) - Math.abs(total('expense'));

  const currentAssets = accounts.filter(a => a.type === 'asset' && a.current_non_current === 'current').reduce((s, a) => s + (a.balance || 0), 0);
  const nonCurrentAssets = accounts.filter(a => a.type === 'asset' && a.current_non_current !== 'current').reduce((s, a) => s + (a.balance || 0), 0);
  const currentLiabilities = accounts.filter(a => a.type === 'liability' && a.current_non_current === 'current').reduce((s, a) => s + Math.abs(a.balance || 0), 0);
  const nonCurrentLiabilities = accounts.filter(a => a.type === 'liability' && a.current_non_current !== 'current').reduce((s, a) => s + Math.abs(a.balance || 0), 0);

  const cash = totalSub('bank');
  const receivables = totalSub('receivable');
  const inventory = totalSub('inventory');
  const payables = totalSub('payable');

  const revenue = Math.abs(total('income'));
  const cogs = accounts.filter(a => a.subtype === 'cogs').reduce((s, a) => s + (a.balance || 0), 0);
  const operatingExpenses = accounts.filter(a => a.type === 'expense' && a.subtype === 'operating').reduce((s, a) => s + (a.balance || 0), 0);

  const ratio = (num, den) => den !== 0 ? Math.round((num / den) * 100) / 100 : null;
  const pct = (num, den) => den !== 0 ? Math.round((num / den) * 10000) / 100 : null;

  const ratios = {
    liquidity: {
      currentRatio: ratio(currentAssets, currentLiabilities),
      quickRatio: ratio((cash + receivables), currentLiabilities),
      cashRatio: ratio(cash, currentLiabilities),
      workingCapital: currentAssets - currentLiabilities,
      label: {
        currentRatio: currentAssets && currentLiabilities ? (ratio(currentAssets, currentLiabilities) >= 1.5 ? 'Healthy' : ratio(currentAssets, currentLiabilities) >= 1 ? 'Adequate' : 'Warning') : 'N/A',
        quickRatio: (cash + receivables) && currentLiabilities ? (ratio((cash + receivables), currentLiabilities) >= 1 ? 'Healthy' : 'Warning') : 'N/A'
      }
    },
    profitability: {
      grossMargin: pct(revenue - cogs, revenue),
      operatingMargin: pct(revenue - cogs - operatingExpenses, revenue),
      netProfitMargin: pct(netIncome, revenue),
      returnOnAssets: pct(netIncome, totalAssets),
      returnOnEquity: pct(netIncome, totalEquity),
      label: {
        grossMargin: revenue ? (pct(revenue - cogs, revenue) >= 30 ? 'Strong' : pct(revenue - cogs, revenue) >= 15 ? 'Adequate' : 'Weak') : 'N/A',
        netProfitMargin: revenue ? (pct(netIncome, revenue) >= 10 ? 'Strong' : pct(netIncome, revenue) >= 0 ? 'Adequate' : 'Loss-making') : 'N/A'
      }
    },
    leverage: {
      debtToEquity: ratio(totalLiabilities, totalEquity),
      debtRatio: pct(totalLiabilities, totalAssets),
      equityMultiplier: ratio(totalAssets, totalEquity),
      interestCoverage: null,
      label: {
        debtToEquity: totalEquity ? (ratio(totalLiabilities, totalEquity) <= 1 ? 'Conservative' : ratio(totalLiabilities, totalEquity) <= 2 ? 'Moderate' : 'High Leverage') : 'N/A',
        debtRatio: totalAssets ? (pct(totalLiabilities, totalAssets) <= 40 ? 'Conservative' : pct(totalLiabilities, totalAssets) <= 60 ? 'Moderate' : 'High') : 'N/A'
      }
    },
    efficiency: {
      assetTurnover: ratio(revenue, totalAssets),
      receivableTurnover: ratio(revenue, receivables),
      daysSalesOutstanding: receivables && revenue ? Math.round(receivables / (revenue / 365)) : null,
      daysPayableOutstanding: payables && cogs ? Math.round(payables / (cogs / 365)) : null,
      inventoryTurnover: cogs && inventory ? ratio(cogs, inventory) : null,
      daysInventoryOutstanding: cogs && inventory ? Math.round(inventory / (cogs / 365)) : null,
      cashConversionCycle: null,
      label: {
        daysSalesOutstanding: null,
        daysPayableOutstanding: null,
        cashConversionCycle: null
      }
    },
    summary: {
      totalAssets, totalLiabilities, totalEquity, netIncome, currentAssets, nonCurrentAssets,
      currentLiabilities, nonCurrentLiabilities, cash, receivables, inventory, payables,
      revenue, cogs, operatingExpenses
    }
  };

  // Calculate CCC
  if (ratios.efficiency.daysSalesOutstanding !== null && ratios.efficiency.daysInventoryOutstanding !== null && ratios.efficiency.daysPayableOutstanding !== null) {
    ratios.efficiency.cashConversionCycle = ratios.efficiency.daysSalesOutstanding + ratios.efficiency.daysInventoryOutstanding - ratios.efficiency.daysPayableOutstanding;
  }

  // Efficiency labels
  if (ratios.efficiency.daysSalesOutstanding !== null) {
    ratios.efficiency.label.daysSalesOutstanding = ratios.efficiency.daysSalesOutstanding <= 30 ? 'Efficient' : ratios.efficiency.daysSalesOutstanding <= 60 ? 'Adequate' : 'Slow Collection';
  }
  if (ratios.efficiency.daysPayableOutstanding !== null) {
    ratios.efficiency.label.daysPayableOutstanding = ratios.efficiency.daysPayableOutstanding <= 30 ? 'Prompt' : ratios.efficiency.daysPayableOutstanding <= 60 ? 'Normal' : 'Delayed';
  }
  if (ratios.efficiency.cashConversionCycle !== null) {
    ratios.efficiency.label.cashConversionCycle = ratios.efficiency.cashConversionCycle <= 30 ? 'Efficient' : ratios.efficiency.cashConversionCycle <= 60 ? 'Adequate' : 'Slow';
  }

  res.json(ratios);
});

// --- Account Drill-Down (Transaction History) ---
app.get('/api/accounting/accounts/:id/activity', auth, (req, res) => {
  const account = (db.data.accounts || []).find(a => a.id === req.params.id);
  if (!account) return res.status(404).json({ error: 'Account not found' });
  const entries = db.findAll('journal_entries');
  const allLines = db.findAll('journal_lines');
  const accountLines = allLines.filter(l => l.accountId === account.id);
  const entryMap = new Map(entries.map(e => [e.id, e]));
  const transactions = accountLines.map(l => {
    const entry = entryMap.get(l.entryId) || {};
    return {
      date: entry.date,
      period: entry.period,
      entryNumber: entry.number,
      description: l.description || entry.description,
      debit: l.debit || 0,
      credit: l.credit || 0,
      balance: (l.debit || 0) - (l.credit || 0),
      entryType: entry.type,
      entryStatus: entry.status,
      source: entry.source,
      sourceId: entry.sourceId
    };
  }).sort((a, b) => (b.date || '').localeCompare(a.date || ''));
  const totalDebits = transactions.reduce((s, t) => s + t.debit, 0);
  const totalCredits = transactions.reduce((s, t) => s + t.credit, 0);
  const runningBalance = transactions.reduce((s, t) => s + t.balance, 0);
  res.json({
    account: { id: account.id, code: account.code, name: account.name, type: account.type, subtype: account.subtype, balance: account.balance },
    transactions, totalDebits, totalCredits, transactionCount: transactions.length, runningBalance
  });
});

// --- Fixed Assets Depreciation ---
app.get('/api/accounting/fixed-assets', auth, (req, res) => {
  const assets = db.findAll('assets');
  const cats = db.findAll('asset_categories');
  const enriched = assets.map(a => {
    const cat = cats.find(c => c.id === a.categoryId);
    const usefulLife = a.usefulLife || cat?.usefulLife || 5;
    const method = a.depreciationMethod || cat?.depreciationMethod || 'straight_line';
    const cost = a.cost || 0;
    const annualDepr = method === 'straight_line' ? cost / usefulLife : cost * (2 / usefulLife);
    const monthlyDepr = annualDepr / 12;
    const totalDepr = a.depreciation || 0;
    const bookValue = cost - totalDepr;
    return { ...a, usefulLife, depreciationMethod: method, annualDepr, monthlyDepr, bookValue, categoryName: cat?.name || '—' };
  });
  res.json(enriched);
});
app.post('/api/accounting/fixed-assets/depreciate', auth, (req, res) => {
  const { assetId, period } = req.body;
  const assets = assetId ? db.findAll('assets').filter(a => a.id === assetId) : db.findAll('assets').filter(a => a.status === 'active');
  const deprAcct = (db.data.accounts || []).find(a => a.code === '6400');
  const accumDeprAcct = (db.data.accounts || []).find(a => a.subtype === 'contra_asset');
  const results = [];
  assets.forEach(a => {
    const usefulLife = a.usefulLife || 5;
    const method = a.depreciationMethod || 'straight_line';
    const cost = a.cost || 0;
    const annualDepr = method === 'straight_line' ? cost / usefulLife : cost * (2 / usefulLife);
    const monthlyDepr = annualDepr / 12;
    const currentDepr = a.depreciation || 0;
    if (currentDepr >= cost) return; // fully depreciated
    const newDepr = Math.min(monthlyDepr, cost - currentDepr);
    a.depreciation = currentDepr + newDepr;
    results.push({ id: a.id, name: a.name, depreciation: newDepr, totalDepreciation: a.depreciation });
    // Auto-post GL: Dr Depreciation Expense, Cr Accumulated Depreciation
    if (deprAcct && accumDeprAcct && newDepr > 0) {
      postJournalAuto(period || new Date().toISOString().slice(0,7) + '-01', `Depreciation - ${a.name}`, `DEPR-${a.id}`, period || new Date().toISOString().slice(0,7), [
        { accountId: deprAcct.id, accountCode: deprAcct.code, description: `Depreciation - ${a.name}`, debit: newDepr, credit: 0 },
        { accountId: accumDeprAcct.id, accountCode: accumDeprAcct.code, description: `Accum. Depr. - ${a.name}`, debit: 0, credit: newDepr }
      ], 'depreciation', a.id, req.user?.username || 'admin');
    }
  });
  db.save(); res.json({ processed: results.length, entries: results });
});

// --- Manufacturing → GL ---
app.post('/api/integration/mo-to-gl/:moId', auth, (req, res) => {
  const mo = (db.data.manufacturing_orders || []).find(m => m.id === req.params.moId);
  if (!mo) return res.status(404).json({ error: 'Manufacturing order not found' });
  if (mo.status === 'completed' && mo.glPosted) return res.status(400).json({ error: 'Already posted to GL' });
  const wipAcct = (db.data.accounts || []).find(a => a.code === '1220');
  const matAcct = (db.data.accounts || []).find(a => a.code === '5000');
  const labAcct = (db.data.accounts || []).find(a => a.code === '5010');
  const ohAcct = (db.data.accounts || []).find(a => a.code === '5020');
  const cost = mo.cost || 0;
  const matCost = cost * 0.5; const labCost = cost * 0.35; const ohCost = cost * 0.15;
  const lines = [];
  if (wipAcct) lines.push({ accountId: wipAcct.id, accountCode: wipAcct.code, description: `WIP - ${mo.number}`, debit: cost, credit: 0 });
  if (matAcct) lines.push({ accountId: matAcct.id, accountCode: matAcct.code, description: `Materials - ${mo.number}`, debit: 0, credit: matCost });
  if (labAcct) lines.push({ accountId: labAcct.id, accountCode: labAcct.code, description: `Labour - ${mo.number}`, debit: 0, credit: labCost });
  if (ohAcct) lines.push({ accountId: ohAcct.id, accountCode: ohAcct.code, description: `Overhead - ${mo.number}`, debit: 0, credit: ohCost });
  if (lines.length >= 2) {
    postJournalAuto(mo.endDate || new Date().toISOString().slice(0,10), `MO Completion - ${mo.number} - ${mo.productName}`, mo.number, mo.endDate?.substring(0,7), lines, 'manufacturing', mo.id, req.user?.username || 'admin');
  }
  mo.status = 'completed'; mo.glPosted = true;
  db.save(); res.json({ mo, cost, breakdown: { materials: matCost, labour: labCost, overhead: ohCost } });
});

// --- Account Reconciliation ---
app.get('/api/accounting/reconciliation', auth, (req, res) => {
  const bankRecs = db.findAll('bank_reconciliation');
  const bankAccts = db.findAll('bank_accounts');
  const glCash = (db.data.accounts || []).filter(a => a.subtype === 'bank');
  const enriched = bankRecs.map(r => {
    const bankAcct = bankAccts.find(a => a.id === r.bankAccountId);
    const glAcct = glCash.find(a => a.name?.toLowerCase().includes(bankAcct?.bank?.toLowerCase() || ''));
    return { ...r, bankName: bankAcct?.name || '—', glBalance: glAcct?.balance || 0, difference: (r.statementBalance || 0) - (glAcct?.balance || 0) };
  });
  res.json(enriched);
});

// --- Dashboard ---
app.get('/api/accounting/dashboard', auth, (req, res) => {
  const accounts = db.findAll('accounts');
  const assets = accounts.filter(a => a.type === 'asset').reduce((s, a) => s + a.balance, 0);
  const liabilities = accounts.filter(a => a.type === 'liability').reduce((s, a) => s + a.balance, 0);
  const equity = accounts.filter(a => a.type === 'equity').reduce((s, a) => s + a.balance, 0);
  const revenue = accounts.filter(a => a.type === 'income').reduce((s, a) => s + (a.balance || 0), 0);
  const expenses = accounts.filter(a => a.type === 'expense').reduce((s, a) => s + (a.balance || 0), 0);
  const netIncome = (-revenue) - expenses;
  const invoices = db.findAll('invoices');
  const bills = db.findAll('bills');
  const totalAR = invoices.filter(i => i.status !== 'paid').reduce((s, i) => s + Number(i.amount || i.total || 0), 0);
  const totalAP = bills.filter(b => b.status !== 'paid').reduce((s, b) => s + Number(b.amount || b.total || 0), 0);
  const cashAccts = accounts.filter(a => a.subtype === 'bank');
  const cashBalance = cashAccts.reduce((s, a) => s + a.balance, 0);
  const pendingJE = (db.data.journal_entries || []).filter(e => e.status === 'draft').length;
  const periods = db.findAll('fiscal_periods');
  const openPeriods = periods.filter(p => p.status === 'open').length;
  const closedPeriods = periods.filter(p => p.status === 'closed').length;
  res.json({ totalAssets: assets, totalLiabilities: liabilities, totalEquity: equity, totalRevenue: revenue, totalExpenses: expenses, netIncome, totalAR, totalAP, cashBalance, totalCheques: (db.data.cheques || []).length, pendingJE, openPeriods, closedPeriods, totalAccounts: accounts.length });
});

// HR stats
app.get('/api/hr/stats', auth, (req, res) => {
  const employees = db.findAll('employees');
  const depts = {};
  employees.forEach(e => { depts[e.department] = (depts[e.department] || 0) + 1; });
  const totalSalary = employees.reduce((s, e) => s + (e.salary || 0), 0);
  res.json({ totalEmployees: employees.length, activeEmployees: employees.filter(e => e.status === 'active').length, departments: depts, totalSalary, avgSalary: employees.length ? totalSalary / employees.length : 0 });
});

// Manufacturing stats
app.get('/api/manufacturing/stats', auth, (req, res) => {
  const mos = db.findAll('manufacturing_orders');
  const wcs = db.findAll('work_centers');
  const boms = db.findAll('bills_of_materials');
  const avgUtilization = wcs.length ? wcs.reduce((s, w) => s + (w.utilization || 0), 0) / wcs.length : 0;
  res.json({ totalOrders: mos.length, inProgress: mos.filter(m => m.status === 'in_progress').length, completed: mos.filter(m => m.status === 'completed').length, planned: mos.filter(m => m.status === 'planned').length, totalWorkCenters: wcs.length, avgUtilization, totalBOMs: boms.length });
});

// CRM stats
app.get('/api/crm/stats', auth, (req, res) => {
  const leads = db.findAll('leads');
  const customers = db.findAll('customers');
  const totalValue = leads.reduce((s, l) => s + (l.value || 0), 0);
  res.json({ totalLeads: leads.length, qualified: leads.filter(l => l.stage === 'qualified').length, proposal: leads.filter(l => l.stage === 'proposal').length, totalPipelineValue: totalValue, totalCustomers: customers.length });
});

// POS stats
app.get('/api/pos/stats', auth, (req, res) => {
  const sessions = db.findAll('pos_sessions');
  const orders = db.findAll('pos_orders');
  res.json({ activeSessions: sessions.filter(s => s.status === 'open').length, totalOrders: orders.length, totalRevenue: orders.reduce((s, o) => s + Number(o.total || 0), 0) });
});

// Support stats
app.get('/api/support/stats', auth, (req, res) => {
  const tickets = db.findAll('support_tickets');
  res.json({ total: tickets.length, open: tickets.filter(t => t.status === 'open').length, inProgress: tickets.filter(t => t.status === 'in_progress').length, closed: tickets.filter(t => t.status === 'closed').length, critical: tickets.filter(t => t.priority === 'critical').length, high: tickets.filter(t => t.priority === 'high').length });
});

// Projects stats
app.get('/api/projects/stats', auth, (req, res) => {
  const projects = db.findAll('projects');
  res.json({ total: projects.length, active: projects.filter(p => p.status === 'active').length, planning: projects.filter(p => p.status === 'planning').length, completed: projects.filter(p => p.status === 'completed').length, totalBudget: projects.reduce((s, p) => s + (p.budget || 0), 0), totalSpent: projects.reduce((s, p) => s + (p.spent || 0), 0) });
});

// Knowledge Hub stats
app.get('/api/knowledge/stats', auth, (req, res) => {
  const articles = db.findAll('knowledge_articles');
  res.json({ total: articles.length, published: articles.filter(a => a.status === 'published').length, totalViews: articles.reduce((s, a) => s + (a.views || 0), 0) });
});

// Operations stats
app.get('/api/operations/stats', auth, (req, res) => {
  const wcs = db.findAll('work_centers');
  const mos = db.findAll('manufacturing_orders');
  const avgUtil = wcs.length ? wcs.reduce((s, w) => s + (w.utilization || 0), 0) / wcs.length : 0;
  res.json({ totalWorkCenters: wcs.length, avgUtilization: avgUtil, inProgressOrders: mos.filter(m => m.status === 'in_progress').length });
});

// Work module
app.get('/api/work/my', auth, (req, res) => {
  const tasks = [
    { id: 'w1', title: 'Review SO-2024-003 stock availability', type: 'task', priority: 'high', dueDate: '2024-01-22', status: 'pending' },
    { id: 'w2', title: 'Approve PO-2024-002', type: 'approval', priority: 'medium', dueDate: '2024-01-23', status: 'pending' },
    { id: 'w3', title: 'Follow up on TKT-002 quality issue', type: 'task', priority: 'critical', dueDate: '2024-01-21', status: 'in_progress' },
    { id: 'w4', title: 'Complete Q1 budget review', type: 'task', priority: 'medium', dueDate: '2024-01-31', status: 'pending' },
  ];
  res.json({ data: tasks });
});

// Stock move
app.post('/api/inventory/stock-move', auth, (req, res) => {
  const { productId, fromWarehouse, toWarehouse, qty, reason } = req.body;
  const product = db.findById('products', productId);
  if (!product) return res.status(404).json({ error: 'Product not found' });
  const move = db.insert('stock_movements', { productId, productName: product.name, fromWarehouse, toWarehouse, qty, reason, movedBy: req.user.id, status: 'completed' });
  res.json(move);
});

// Settings
app.get('/api/settings', auth, (req, res) => { res.json(db.data.settings || {}); });
app.put('/api/settings', auth, requireAdmin, (req, res) => { db.data.settings = { ...db.data.settings, ...req.body }; db.save(); res.json(db.data.settings); });

// Health
app.get('/health', (req, res) => { res.json({ status: 'ok', timestamp: now() }); });
app.get('/api/health', (req, res) => { res.json({ status: 'ok', modules: MODULES.length, timestamp: now() }); });

// =================== GENERIC CRUD ROUTES ===================
const MODULES = [
  { name: 'products', prefix: 'products', auth: true },
  { name: 'categories', prefix: 'categories', auth: true },
  { name: 'warehouses', prefix: 'warehouses', auth: true },
  { name: 'suppliers', prefix: 'suppliers', auth: true },
  { name: 'customers', prefix: 'customers', auth: true },
  { name: 'leads', prefix: 'leads', auth: true },
  { name: 'sales_orders', prefix: 'sales-orders', auth: true },
  { name: 'purchase_orders', prefix: 'purchase-orders', auth: true },
  { name: 'manufacturing_orders', prefix: 'manufacturing-orders', auth: true },
  { name: 'work_centers', prefix: 'work-centers', auth: true },
  { name: 'employees', prefix: 'employees', auth: true },
  { name: 'departments', prefix: 'departments', auth: true },
  { name: 'projects', prefix: 'projects', auth: true },
  { name: 'support_tickets', prefix: 'support-tickets', auth: true },
  { name: 'quality_checks', prefix: 'quality-checks', auth: true },
  { name: 'bills_of_materials', prefix: 'bills-of-materials', auth: true },
  { name: 'knowledge_articles', prefix: 'knowledge-articles', auth: true },
  { name: 'accounts', prefix: 'accounts', auth: true },
  { name: 'invoices', prefix: 'invoices', auth: true },
  { name: 'payments', prefix: 'payments', auth: true },
  { name: 'expenses', prefix: 'expenses', auth: true },
  { name: 'journal_entries', prefix: 'journal-entries', auth: true },
  { name: 'tax_rates', prefix: 'tax-rates', auth: true },
  { name: 'bank_accounts', prefix: 'bank-accounts', auth: true },
  { name: 'asset_categories', prefix: 'asset-categories', auth: true },
  { name: 'assets', prefix: 'assets', auth: true },
  { name: 'fleet_vehicles', prefix: 'fleet-vehicles', auth: true },
  { name: 'quality_alerts', prefix: 'quality-alerts', auth: true },
  { name: 'non_conformances', prefix: 'non-conformances', auth: true },
  { name: 'leave_requests', prefix: 'leave-requests', auth: true },
  { name: 'attendances', prefix: 'attendances', auth: true },
  { name: 'contracts', prefix: 'contracts', auth: true },
  { name: 'payroll', prefix: 'payroll', auth: true },
  { name: 'campaigns', prefix: 'campaigns', auth: true },
  { name: 'email_templates', prefix: 'email-templates', auth: true },
  { name: 'rfqs', prefix: 'rfqs', auth: true },
  { name: 'lots', prefix: 'lots', auth: true },
  { name: 'serial_numbers', prefix: 'serial-numbers', auth: true },
  { name: 'packages', prefix: 'packages', auth: true },
  { name: 'delivery_orders', prefix: 'delivery-orders', auth: true },
  { name: 'returns', prefix: 'returns', auth: true },
  { name: 'scrap_orders', prefix: 'scrap-orders', auth: true },
  { name: 'pos_sessions', prefix: 'pos-sessions', auth: true },
  { name: 'pos_orders', prefix: 'pos-orders', auth: true },
  { name: 'automation_logs', prefix: 'automation-logs', auth: true },
  { name: 'scheduled_reports', prefix: 'scheduled-reports', auth: true },
  { name: 'notifications', prefix: 'notifications', auth: true },
  { name: 'tags', prefix: 'tags', auth: true },
  { name: 'attachments', prefix: 'attachments', auth: true },
  { name: 'stock_movements', prefix: 'stock-movements', auth: true },
  { name: 'bom_lines', prefix: 'bom-lines', auth: true },
  { name: 'routing_operations', prefix: 'routing-operations', auth: true },
  { name: 'sales_order_lines', prefix: 'sales-order-lines', auth: true },
  { name: 'purchase_order_lines', prefix: 'purchase-order-lines', auth: true },
  { name: 'invoice_lines', prefix: 'invoice-lines', auth: true },
  { name: 'bill_lines', prefix: 'bill-lines', auth: true },
  { name: 'journal_lines', prefix: 'journal-lines', auth: true },
  { name: 'ticket_comments', prefix: 'ticket-comments', auth: true },
  { name: 'project_tasks', prefix: 'project-tasks', auth: true },
  { name: 'project_timesheets', prefix: 'project-timesheets', auth: true },
  { name: 'activity_logs', prefix: 'activity-logs', auth: true },
  { name: 'dms_documents', prefix: 'dms/documents', auth: true },
  { name: 'dms_categories', prefix: 'dms/categories', auth: true },
  { name: 'dms_versions', prefix: 'dms/versions', auth: true },
  { name: 'dms_links', prefix: 'dms/links', auth: true },
  { name: 'bi_dashboards', prefix: 'bi-dashboards', auth: true },
  { name: 'bi_charts', prefix: 'bi-charts', auth: true },
  { name: 'automations', prefix: 'automations', auth: true },
];

// =================== DATA VALIDATION ===================
const FK_MAP = {
  products: { supplierId: 'suppliers', warehouseId: 'warehouses' },
  sales_orders: { customerId: 'customers', salesRep: 'users' },
  sales_order_lines: { orderId: 'sales_orders', productId: 'products' },
  purchase_orders: { supplierId: 'suppliers' },
  purchase_order_lines: { orderId: 'purchase_orders', productId: 'products' },
  manufacturing_orders: { workCenterId: 'work_centers', assignedTo: 'employees' },
  invoices: { customerId: 'customers', orderId: 'sales_orders' },
  invoice_lines: { invoiceId: 'invoices', productId: 'products' },
  payments: { customerId: 'customers', invoiceId: 'invoices' },
  support_tickets: { customerId: 'customers', assignedTo: 'users' },
  ticket_comments: { ticketId: 'support_tickets', userId: 'users' },
  leads: { assignedTo: 'users' },
  projects: { managerId: 'employees' },
  project_tasks: { projectId: 'projects', assignedTo: 'employees' },
  project_timesheets: { projectId: 'projects', employeeId: 'employees' },
  bom_lines: { bomId: 'bills_of_materials', productId: 'products' },
  knowledge_articles: { author: 'employees' },
  stock_movements: { productId: 'products' },
  leave_requests: { employeeId: 'employees' },
  attendances: { employeeId: 'employees' },
  contracts: { employeeId: 'employees' },
  payroll: { employeeId: 'employees' },
  journal_entries: { accountId: 'accounts' },
  journal_lines: { accountId: 'accounts' },
  expenses: { accountId: 'accounts' },
  bills: { supplierId: 'suppliers' },
  bill_lines: { billId: 'bills', productId: 'products' },
  assets: { categoryId: 'asset_categories' },
  lots: { productId: 'products' },
  serial_numbers: { productId: 'products' },
  pos_orders: { sessionId: 'pos_sessions' },
  automation_logs: { automationId: 'automations' },
  activity_logs: { userId: 'users' },
  // DMS tables
  dms_categories: {},
  dms_documents: { categoryId: 'dms_categories' },
  dms_versions: { documentId: 'dms_documents' },
  dms_access: { documentId: 'dms_documents', userId: 'users' },
  dms_workflows: { documentId: 'dms_documents' },
  dms_workflow_steps: { workflowId: 'dms_workflows' },
  dms_audit_log: { documentId: 'dms_documents', userId: 'users' },
  dms_tags: {},
  dms_document_tags: { documentId: 'dms_documents', tagId: 'dms_tags' },
  dms_notifications: { documentId: 'dms_documents', userId: 'users' },
  dms_shares: { documentId: 'dms_documents', sharedWithUserId: 'users' },
  // BVA tables
  bva_budget_entries: { departmentId: 'departments' },
  bva_actual_entries: { departmentId: 'departments' },
  bva_actuals: {},
  bva_alerts: {},
  bva_forecasts: {},
  bva_scenarios: {},
  bva_forecast_lines: { forecastId: 'bva_forecasts' },
  bva_actuals_lines: { actualId: 'bva_actuals' },
  bva_workforce: {},
  bva_field_ops: {},
  bva_funding_sources: {},
  // Banking
  bank_accounts: {},
  cheques: { bankAccountId: 'bank_accounts' },
  reconciliations: { bankAccountId: 'bank_accounts' },
  // Tax
  tax_rates: {},
  gst_returns: {},
  // Security
  roles: {},
  permissions: {},
  user_roles: { userId: 'users', roleId: 'roles' },
  login_history: { userId: 'users' },
  // AIS tables
  fiscal_periods: {},
  account_balances: { accountId: 'accounts' },
  fixed_assets: { accountId: 'accounts', depreciationAccountId: 'accounts' },
};

const UNIQUE_MAP = {
  products: { sku: 'SKU' },
  users: { username: 'Username', email: 'Email' },
  customers: { email: 'Email' },
  suppliers: { email: 'Email' },
  employees: { email: 'Email', employeeId: 'Employee ID' },
  accounts: { code: 'Account Code' },
  departments: { name: 'Department Name' },
  categories: { name: 'Category Name' },
  warehouses: { code: 'Warehouse Code' },
  work_centers: { code: 'Work Center Code' },
};

function validateFKs(table, data) {
  const fks = FK_MAP[table];
  if (!fks) return null;
  const errors = [];
  for (const [field, refTable] of Object.entries(fks)) {
    const val = data[field];
    if (val === undefined || val === null || val === '') continue;
    if (!db.findById(refTable, val)) {
      errors.push(`${field} references non-existent ${refTable}: ${val}`);
    }
  }
  return errors.length ? errors : null;
}

function validateUnique(table, data, excludeId) {
  const uniques = UNIQUE_MAP[table];
  if (!uniques) return null;
  const errors = [];
  for (const [field, label] of Object.entries(uniques)) {
    const val = data[field];
    if (!val) continue;
    const existing = db.findOne(table, r => r[field] === val && r.id !== excludeId);
    if (existing) errors.push(`${label} '${val}' already exists`);
  }
  return errors.length ? errors : null;
}

function logActivity(userId, action, module, recordId, details) {
  db.insert('activity_logs', { userId, action, module, recordId, details, timestamp: now() });
}

// =================== ATTACHMENT API ===================
app.get('/api/attachments/:folder/:recordId', auth, (req, res) => {
  const files = db.findMany('attachments', a => a.folder === req.params.folder && a.recordId === req.params.recordId);
  res.json({ data: files });
});

app.post('/api/attachments/:folder/:recordId', auth, upload.single('file'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No file uploaded' });
  const { folder, recordId } = req.params;
  const att = db.insert('attachments', {
    folder, recordId, originalName: req.file.originalname,
    filename: req.file.filename, path: `${folder}/${req.file.filename}`,
    size: req.file.size, mimetype: req.file.mimetype,
    uploadedBy: req.user.id
  });
  res.status(201).json(att);
});

app.delete('/api/attachments/:id', auth, (req, res) => {
  const att = db.findById('attachments', req.params.id);
  if (!att) return res.status(404).json({ error: 'Not found' });
  const filePath = path.join(ATTACH_DIR, att.path);
  if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
  db.delete('attachments', req.params.id);
  res.json({ success: true });
});

app.post('/api/upload/logo', auth, upload.single('file'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No file' });
  const url = `/api/files/settings/${req.file.filename}`;
  db.data.settings = db.data.settings || {};
  db.data.settings.logoPath = url;
  db.save();
  res.json({ url });
});

app.get('/api/files/:folder/:filename', (req, res) => {
  const filePath = path.join(ATTACH_DIR, req.params.folder, req.params.filename);
  if (!fs.existsSync(filePath)) return res.status(404).json({ error: 'Not found' });
  res.sendFile(filePath);
});

// =================== ORDER LINE ITEMS API ===================
app.get('/api/order-lines/:type/:orderId', auth, (req, res) => {
  const table = req.params.type === 'sales' ? 'sales_order_lines' : 'purchase_order_lines';
  const lines = db.findMany(table, l => l.orderId === req.params.orderId);
  res.json({ data: lines });
});

app.post('/api/order-lines/:type/:orderId', auth, (req, res) => {
  const table = req.params.type === 'sales' ? 'sales_order_lines' : 'purchase_order_lines';
  const line = { ...req.body, orderId: req.params.orderId };
  const product = db.findById('products', line.productId);
  if (product) {
    line.productName = product.name;
    line.sku = product.sku;
    line.unitPrice = line.unitPrice || product.price;
    line.subtotal = (line.qty || 0) * (line.unitPrice || 0);
  }
  const saved = db.insert(table, line);
  recalcOrderTotal(req.params.type, req.params.orderId);
  res.status(201).json(saved);
});

app.put('/api/order-lines/:type/:id', auth, (req, res) => {
  const table = req.params.type === 'sales' ? 'sales_order_lines' : 'purchase_order_lines';
  const line = db.findById(table, req.params.id);
  if (!line) return res.status(404).json({ error: 'Not found' });
  const updated = { ...line, ...req.body };
  updated.subtotal = (updated.qty || 0) * (updated.unitPrice || 0);
  const saved = db.update(table, req.params.id, updated);
  recalcOrderTotal(req.params.type, saved.orderId);
  res.json(saved);
});

app.delete('/api/order-lines/:type/:id', auth, (req, res) => {
  const table = req.params.type === 'sales' ? 'sales_order_lines' : 'purchase_order_lines';
  const line = db.findById(table, req.params.id);
  if (!line) return res.status(404).json({ error: 'Not found' });
  db.delete(table, req.params.id);
  recalcOrderTotal(req.params.type, line.orderId);
  res.json({ success: true });
});

function recalcOrderTotal(type, orderId) {
  const table = type === 'sales' ? 'sales_order_lines' : 'purchase_order_lines';
  const orderTable = type === 'sales' ? 'sales_orders' : 'purchase_orders';
  const lines = db.findMany(table, l => l.orderId === orderId);
  const subtotal = lines.reduce((s, l) => s + (l.subtotal || 0), 0);
  const order = db.findById(orderTable, orderId);
  if (order) {
    const settings = db.data.settings || {};
    const taxRate = settings.taxRate || 0;
    const tax = subtotal * (taxRate / 100);
    db.update(orderTable, orderId, { subtotal, tax, total: subtotal + tax });
  }
}

// =================== STOCK MANAGEMENT ===================
app.post('/api/inventory/stock-adjust', auth, (req, res) => {
  const { productId, qty, type, reason } = req.body;
  const product = db.findById('products', productId);
  if (!product) return res.status(404).json({ error: 'Product not found' });
  const newStock = type === 'add' ? product.stock + qty : product.stock - qty;
  if (newStock < 0) return res.status(400).json({ error: 'Insufficient stock' });
  db.update('products', productId, { stock: newStock });
  db.insert('stock_movements', {
    productId, productName: product.name, qty, type, reason,
    fromWarehouse: product.warehouseId, toWarehouse: product.warehouseId,
    movedBy: req.user.id, status: 'completed', date: now()
  });
  res.json({ success: true, newStock });
});

// =================== ORDER STATUS WITH STOCK ===================
app.patch('/api/sales-orders/:id/status', auth, (req, res) => {
  const order = db.findById('sales_orders', req.params.id);
  if (!order) return res.status(404).json({ error: 'Order not found' });
  const { status } = req.body;
  const oldStatus = order.status;
  db.update('sales_orders', req.params.id, { status });
  if (oldStatus !== 'shipped' && status === 'shipped') {
    const lines = db.findMany('sales_order_lines', l => l.orderId === req.params.id);
    // Deduct stock and create stock movements
    lines.forEach(l => {
      const product = db.findById('products', l.productId);
      if (product) {
        const newStock = Math.max(0, product.stock - (l.qty || 0));
        db.update('products', l.productId, { stock: newStock });
        db.insert('stock_movements', {
          productId: l.productId, productName: product.name, qty: l.qty, type: 'deduct',
          reason: `SO ${order.number} shipped`, fromWarehouse: product.warehouseId,
          toWarehouse: null, movedBy: req.user?.id || 'u1', status: 'completed', date: now()
        });
      }
    });
    // Calculate totals from SO lines (SAP three-way: SO lines → Invoice lines)
    const invSubtotal = lines.reduce((s, l) => s + (l.subtotal || l.qty * l.unitPrice || 0), 0);
    const invTax = Math.round(invSubtotal * 0.15 * 100) / 100;
    const invTotal = invSubtotal + invTax;
    // Create invoice linked to SO
    const invoice = {
      id: `inv-${Date.now()}-${Math.random().toString(36).slice(2,5)}`,
      number: `INV-${Date.now()}`,
      customerId: order.customerId, customerName: order.customerName,
      orderId: order.id,
      date: now().slice(0,10),
      dueDate: new Date(Date.now() + 30*86400000).toISOString().slice(0,10),
      subtotal: invSubtotal, tax: invTax, amount: invTotal, paid: 0, balance: invTotal,
      status: 'pending', notes: `Auto-generated from ${order.number}`,
      glPosted: false, createdAt: now()
    };
    db.data.invoices.push(invoice);
    // Create invoice lines from SO lines
    lines.forEach(l => {
      db.data.invoice_lines = db.data.invoice_lines || [];
      db.data.invoice_lines.push({
        id: `il-${Date.now()}-${Math.random().toString(36).slice(2,5)}`,
        invoiceId: invoice.id, productId: l.productId, productName: l.productName,
        sku: l.sku, qty: l.qty, unitPrice: l.unitPrice,
        subtotal: l.subtotal || l.qty * l.unitPrice,
        vatRate: 15, vatAmount: Math.round((l.subtotal || l.qty * l.unitPrice) * 0.15 * 100) / 100,
        total: (l.subtotal || l.qty * l.unitPrice) * 1.15
      });
    });
    // SAP GL Posting: Dr AR (1100), Cr Revenue (4000) + Cr Output VAT (2010)
    const arAcct = (db.data.accounts || []).find(a => a.subtype === 'receivable');
    const revAcct = (db.data.accounts || []).find(a => a.code === '4000');
    const outputTaxAcct = (db.data.accounts || []).find(a => a.code === '2010');
    if (arAcct && revAcct && invTotal > 0) {
      const journalLines = [
        { accountId: arAcct.id, accountCode: arAcct.code, description: `AR - ${order.customerName}`, debit: invTotal, credit: 0 }
      ];
      if (invTax > 0 && outputTaxAcct) {
        journalLines.push({ accountId: revAcct.id, accountCode: revAcct.code, description: `Revenue - ${order.number}`, debit: 0, credit: invSubtotal });
        journalLines.push({ accountId: outputTaxAcct.id, accountCode: outputTaxAcct.code, description: `Output VAT - ${order.number}`, debit: 0, credit: invTax });
      } else {
        journalLines.push({ accountId: revAcct.id, accountCode: revAcct.code, description: `Revenue - ${order.number}`, debit: 0, credit: invTotal });
      }
      postJournalAuto(now().slice(0,10), `Invoice ${invoice.number} - ${order.customerName}`,
        invoice.number, now().substring(0,7), journalLines, 'ar', invoice.id, req.user?.username || 'admin');
    }
    // Post COGS: Dr COGS (5000), Cr Inventory RM (1200)
    const cogsAcct = (db.data.accounts || []).find(a => a.code === '5000');
    const invAcct = (db.data.accounts || []).find(a => a.code === '1200');
    const totalCost = lines.reduce((s, l) => {
      const p = db.findById('products', l.productId);
      return s + ((p?.cost || p?.price * 0.6 || 0) * (l.qty || 0));
    }, 0);
    if (cogsAcct && invAcct && totalCost > 0) {
      postJournalAuto(now().slice(0,10), `COGS ${order.number}`,
        `COGS-${order.number}`, now().substring(0,7), [
          { accountId: cogsAcct.id, accountCode: cogsAcct.code, description: `COGS - ${order.number}`, debit: totalCost, credit: 0 },
          { accountId: invAcct.id, accountCode: invAcct.code, description: `Inventory - ${order.number}`, debit: 0, credit: totalCost }
        ], 'manufacturing', order.id, req.user?.username || 'admin');
    }
    // Record output tax in GST return
    if (invTax > 0) {
      const gstReturns = db.data.gst_returns || [];
      const currentPeriod = now().substring(0,7);
      let currentReturn = gstReturns.find(r => r.period === currentPeriod && r.status === 'draft');
      if (!currentReturn) {
        currentReturn = { id: `gst-${Date.now()}`, period: currentPeriod, status: 'draft', outputTax: 0, inputTax: 0, netTax: 0, totalSales: 0, totalPurchases: 0, createdAt: now() };
        gstReturns.push(currentReturn);
      }
      currentReturn.outputTax = (currentReturn.outputTax || 0) + invTax;
      currentReturn.totalSales = (currentReturn.totalSales || 0) + invSubtotal;
      currentReturn.netTax = currentReturn.outputTax - (currentReturn.inputTax || 0);
      db.data.gst_returns = gstReturns;
    }
    invoice.glPosted = true;
    db.save();
    db.save();
  }
  res.json({ success: true });
});

app.patch('/api/purchase-orders/:id/status', auth, (req, res) => {
  const order = db.findById('purchase_orders', req.params.id);
  if (!order) return res.status(404).json({ error: 'Order not found' });
  const { status } = req.body;
  const oldStatus = order.status;
  db.update('purchase_orders', req.params.id, { status });
  if (oldStatus !== 'received' && status === 'received') {
    const lines = db.findMany('purchase_order_lines', l => l.orderId === req.params.id);
    lines.forEach(l => {
      const product = db.findById('products', l.productId);
      if (product) {
        const newStock = product.stock + (l.qty || 0);
        db.update('products', l.productId, { stock: newStock });
        db.insert('stock_movements', {
          productId: l.productId, productName: product.name, qty: l.qty, type: 'add',
          reason: `PO ${order.number} received`, fromWarehouse: null,
          toWarehouse: product.warehouseId, movedBy: req.user.id, status: 'completed', date: now()
        });
      }
    });
    // Auto-create bill
    const billTotal = lines.reduce((s, l) => s + (l.subtotal || l.qty * l.unitPrice || 0), 0);
    const bill = {
      id: `bill-${Date.now()}-${Math.random().toString(36).slice(2,5)}`,
      number: `BILL-${Date.now()}`,
      supplierId: order.supplierId, supplierName: order.supplierName,
      orderId: order.id,
      date: now().slice(0,10),
      dueDate: new Date(Date.now() + 30*86400000).toISOString().slice(0,10),
      amount: billTotal, paid: 0, balance: billTotal,
      status: 'pending', notes: `Auto-generated from ${order.number}`,
      createdAt: now()
    };
    db.data.bills.push(bill); db.save();
    // Post GL: Dr Inventory, Cr AP
    const invAcct = (db.data.accounts || []).find(a => a.subtype === 'inventory');
    const apAcct = (db.data.accounts || []).find(a => a.subtype === 'payable');
    if (invAcct && apAcct && billTotal > 0) {
      postJournalAuto(now().slice(0,10), `Bill ${bill.number} - ${order.supplierName}`,
        bill.number, now().substring(0,7), [
          { accountId: invAcct.id, description: 'Inventory', debit: billTotal, credit: 0 },
          { accountId: apAcct.id, description: 'AP', debit: 0, credit: billTotal }
        ], 'ap', bill.id);
    }
    bill.glPosted = true;
    db.save();
  }
  res.json({ success: true });
});

// =================== USER MANAGEMENT ===================
app.get('/api/users', auth, (req, res) => {
  const users = db.findAll('users').map(({ passwordHash, ...u }) => u);
  res.json(users);
});
app.post('/api/users', auth, requireAdmin, (req, res) => {
  const { username, email, password, name, role, department, jobTitle, phone } = req.body;
  if (!username || !password) return res.status(400).json({ error: 'Username and password required' });
  const uErr = validateUnique('users', { username, email });
  if (uErr) return res.status(400).json({ error: uErr.join(', ') });
  const user = db.insert('users', {
    username, email, passwordHash: hashPassword(password), name: name || username,
    role: role || 'user', department: department || '', jobTitle: jobTitle || '',
    phone: phone || '', status: 'active'
  });
  const { passwordHash, ...safe } = user;
  res.status(201).json(safe);
});

app.put('/api/users/:id', auth, requireAdmin, (req, res) => {
  const existing = db.findById('users', req.params.id);
  if (!existing) return res.status(404).json({ error: 'Not found' });
  const updates = { ...req.body };
  if (updates.password) {
    updates.passwordHash = hashPassword(updates.password);
    delete updates.password;
  }
  const checkUser = { username: updates.username || existing.username, email: updates.email || existing.email };
  const uErr = validateUnique('users', checkUser, req.params.id);
  if (uErr) return res.status(400).json({ error: uErr.join(', ') });
  const user = db.update('users', req.params.id, updates);
  const { passwordHash, ...safe } = user;
  res.json(safe);
});

// =================== SETUP WIZARD ===================
app.get('/api/setup-wizard', auth, (req, res) => {
  let wizard = db.findOne('setup_wizard', w => w.userId === req.user.id);
  if (!wizard) {
    wizard = db.insert('setup_wizard', {
      userId: req.user.id, currentPhase: 1, currentStep: 1,
      completedSteps: [], skippedSteps: [],
      phase1Complete: false, phase2Complete: false, phase3Complete: false,
      phase4Complete: false, phase5Complete: false,
      stepData: {}, status: 'IN_PROGRESS', completionPercentage: 0
    });
  }
  res.json(wizard);
});

app.patch('/api/setup-wizard/step', auth, (req, res) => {
  const { phase, step, data, skip } = req.body;
  let wizard = db.findOne('setup_wizard', w => w.userId === req.user.id);
  if (!wizard) return res.status(404).json({ error: 'Wizard not found' });
  const stepKey = `${phase}.${step}`;
  const completed = [...(wizard.completedSteps || [])];
  const skipped = [...(wizard.skippedSteps || [])];
  if (skip && !skipped.includes(stepKey)) skipped.push(stepKey);
  if (!skip && !completed.includes(stepKey)) completed.push(stepKey);
  const pct = Math.round((completed.length / 15) * 100);
  const updates = {
    currentPhase: phase, currentStep: step, completedSteps: completed,
    skippedSteps: skipped, completionPercentage: pct,
    stepData: { ...wizard.stepData, [stepKey]: data || {} },
    status: pct >= 100 ? 'COMPLETED' : 'IN_PROGRESS'
  };
  if (phase === 1) updates.phase1Complete = completed.filter(s => s.startsWith('1.')).length >= 3;
  if (phase === 2) updates.phase2Complete = completed.filter(s => s.startsWith('2.')).length >= 3;
  if (phase === 3) updates.phase3Complete = completed.filter(s => s.startsWith('3.')).length >= 3;
  if (phase === 4) updates.phase4Complete = completed.filter(s => s.startsWith('4.')).length >= 2;
  if (phase === 5) updates.phase5Complete = completed.filter(s => s.startsWith('5.')).length >= 2;
  const saved = db.update('setup_wizard', wizard.id, updates);
  res.json(saved);
});

app.post('/api/setup-wizard/complete', auth, (req, res) => {
  let wizard = db.findOne('setup_wizard', w => w.userId === req.user.id);
  if (wizard) db.update('setup_wizard', wizard.id, { status: 'COMPLETED', completionPercentage: 100 });
  res.json({ success: true });
});

// =================== MANUFACTURING SETTINGS ===================
app.get('/api/manufacturing/settings', auth, (req, res) => {
  res.json(db.data.manufacturingSettings || { isActive: false, industryProfile: 'GENERAL', capabilities: null, presets: [] });
});
app.put('/api/manufacturing/settings', auth, (req, res) => {
  db.data.manufacturingSettings = { ...(db.data.manufacturingSettings || {}), ...req.body };
  db.save();
  res.json(db.data.manufacturingSettings);
});

// =================== KNOWLEDGE HUB BRAND KIT ===================
app.get('/api/knowledge-hub/brand-kit', auth, (req, res) => {
  res.json(db.data.brandKit || { colors: [], fonts: {}, guidelines: [], configured: false });
});
app.put('/api/knowledge-hub/brand-kit', auth, (req, res) => {
  db.data.brandKit = { ...(db.data.brandKit || {}), ...req.body, configured: true };
  db.save();
  res.json(db.data.brandKit);
});

// =================== ONBOARDING LAUNCHPAD ===================
app.get('/api/onboarding/launchpad', auth, (req, res) => {
  let launchpad = db.findOne('onboarding_launchpad', l => l.userId === req.user.id);
  if (!launchpad) {
    launchpad = db.insert('onboarding_launchpad', {
      userId: req.user.id,
      items: [
        { id: 'ol1', title: 'Meet your workspace', description: 'Take a quick tour', action: '/dashboard', xp: 10, completed: false, optional: false },
        { id: 'ol2', title: 'Add your first product', description: 'Add a product to inventory', action: '/inventory', xp: 30, completed: false, optional: false },
        { id: 'ol3', title: 'Add your first customer', description: 'Add a customer to CRM', action: '/crm', xp: 20, completed: false, optional: false },
        { id: 'ol4', title: 'Send your first invoice', description: 'Create an invoice', action: '/sales', xp: 50, completed: false, optional: false },
        { id: 'ol5', title: 'Invite your team', description: 'Add team members', action: '/hr', xp: 30, completed: false, optional: true },
        { id: 'ol6', title: 'Getting Started course', description: 'Complete the learning module', action: '/learning', xp: 40, completed: false, optional: true },
        { id: 'ol7', title: 'Choose your plan', description: 'Select a subscription plan', action: '/settings', xp: 20, completed: false, optional: false },
      ],
      totalXP: 0
    });
  }
  res.json(launchpad);
});

app.patch('/api/onboarding/launchpad/:itemId', auth, (req, res) => {
  let launchpad = db.findOne('onboarding_launchpad', l => l.userId === req.user.id);
  if (!launchpad) return res.status(404).json({ error: 'Launchpad not found' });
  const item = launchpad.items.find(i => i.id === req.params.itemId);
  if (item) {
    item.completed = true;
    launchpad.totalXP = (launchpad.totalXP || 0) + (item.xp || 0);
  }
  db.update('onboarding_launchpad', launchpad.id, { items: launchpad.items, totalXP: launchpad.totalXP });
  res.json(launchpad);
});

// =================== AUDIT TRAIL ===================
function auditLog(userId, username, action, table, recordId, recordLabel, oldData, newData) {
  const changes = {};
  if (oldData && newData) {
    const allKeys = new Set([...Object.keys(oldData), ...Object.keys(newData)]);
    allKeys.forEach(k => {
      if (k === 'updatedAt' || k === 'createdAt' || k === 'passwordHash') return;
      if (JSON.stringify(oldData[k]) !== JSON.stringify(newData[k])) {
        changes[k] = { from: oldData[k], to: newData[k] };
      }
    });
  }
  db.insert('audit_trail', {
    userId, username, action, table, recordId, recordLabel,
    changes: Object.keys(changes).length > 0 ? changes : undefined,
    oldData: action === 'DELETE' ? oldData : undefined,
    timestamp: now()
  });
}

// =================== DOCUMENT MANAGEMENT SYSTEM (DMS) ===================

// Seed DMS categories
function initDMSCategories() {
  if (db.findAll('dms_categories').length === 0) {
    const cats = [
      { name: 'Invoices', code: 'INV', color: '#00d4ff', icon: 'receipt', retention: 10 },
      { name: 'Purchase Orders', code: 'PO', color: '#00ff88', icon: 'shopping-cart', retention: 7 },
      { name: 'Sales Orders', code: 'SO', color: '#a855f7', icon: 'file-text', retention: 7 },
      { name: 'Delivery Notes', code: 'DN', color: '#ffb020', icon: 'truck', retention: 5 },
      { name: 'Contracts', code: 'CTR', color: '#ff4060', icon: 'file-signature', retention: 10 },
      { name: 'Declarations of Conformity', code: 'DOC', color: '#00ccaa', icon: 'shield-check', retention: 10 },
      { name: 'Product Data Sheets', code: 'PDS', color: '#7c3aed', icon: 'package', retention: 5 },
      { name: 'Correspondence', code: 'COR', color: '#6366f1', icon: 'mail', retention: 5 },
      { name: 'HR Documents', code: 'HR', color: '#ec4899', icon: 'users', retention: 10 },
      { name: 'Financial Reports', code: 'FIN', color: '#14b8a6', icon: 'bar-chart', retention: 10 },
      { name: 'Quality Documents', code: 'QUA', color: '#f59e0b', icon: 'check-circle', retention: 7 },
      { name: 'General', code: 'GEN', color: '#6b7280', icon: 'folder', retention: 3 },
    ];
    cats.forEach(c => db.insert('dms_categories', { id: crypto.randomUUID(), ...c, status: 'active', createdAt: new Date().toISOString() }));
  }
}
initDMSCategories();

// DMS Document Upload
app.post('/api/dms/documents', auth, dmsUpload.single('file'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No file uploaded' });
  const { categoryId, type, linkedTable, linkedId, description, keywords, status } = req.body;
  const docNum = `DOC-${Date.now().toString(36).toUpperCase()}`;
  const doc = db.insert('dms_documents', {
    docNum,
    originalName: req.file.originalname,
    fileName: req.file.filename,
    filePath: req.file.path,
    fileSize: req.file.size,
    mimeType: req.file.mimetype,
    categoryId: categoryId || '',
    type: type || 'general',
    linkedTable: linkedTable || '',
    linkedId: linkedId || '',
    description: description || '',
    keywords: keywords ? keywords.split(',').map(k => k.trim()) : [],
    status: status || 'active',
    checkedOut: false,
    checkedOutBy: null,
    version: 1,
    uploadedBy: req.user.id,
    uploadedByName: req.user.username,
    retentionYears: 5,
    stampStatus: '',
    workflowStatus: 'none',
  });
  auditLog(req.user.id, req.user.username, 'CREATE', 'dms_documents', doc.id, doc.originalName, null, { originalName: doc.originalName, docNum: doc.docNum });
  res.status(201).json(doc);
});

// DMS List documents
app.get('/api/dms/documents', auth, (req, res) => {
  let docs = db.findAll('dms_documents');
  const { category, type, status, linkedTable, linkedId, search, sort = 'createdAt', order = 'desc', page = 1, limit = 50 } = req.query;
  if (category) docs = docs.filter(d => d.categoryId === category);
  if (type) docs = docs.filter(d => d.type === type);
  if (status) docs = docs.filter(d => d.status === status);
  if (linkedTable) docs = docs.filter(d => d.linkedTable === linkedTable);
  if (linkedId) docs = docs.filter(d => d.linkedId === linkedId);
  if (search) {
    const q = search.toLowerCase();
    docs = docs.filter(d => (d.originalName || '').toLowerCase().includes(q) || (d.description || '').toLowerCase().includes(q) || (d.docNum || '').toLowerCase().includes(q) || (d.keywords || []).some(k => k.toLowerCase().includes(q)));
  }
  docs.sort((a, b) => {
    const va = a[sort] || '', vb = b[sort] || '';
    return order === 'desc' ? (vb > va ? 1 : -1) : (va > vb ? 1 : -1);
  });
  const total = docs.length;
  const start = (parseInt(page) - 1) * parseInt(limit);
  docs = docs.slice(start, start + parseInt(limit));
  res.json({ data: docs, total, page: parseInt(page), limit: parseInt(limit), pages: Math.ceil(total / parseInt(limit)) });
});

// DMS Get single document
app.get('/api/dms/documents/:id', auth, (req, res) => {
  const doc = db.findById('dms_documents', req.params.id);
  if (!doc) return res.status(404).json({ error: 'Document not found' });
  doc.versions = db.findMany('dms_versions', v => v.documentId === doc.id);
  doc.links = db.findMany('dms_links', l => l.documentId === doc.id);
  res.json(doc);
});

// DMS Update document metadata
app.put('/api/dms/documents/:id', auth, (req, res) => {
  const doc = db.findById('dms_documents', req.params.id);
  if (!doc) return res.status(404).json({ error: 'Document not found' });
  const old = { ...doc };
  const updated = db.update('dms_documents', req.params.id, req.body);
  auditLog(req.user.id, req.user.username, 'UPDATE', 'dms_documents', doc.id, doc.originalName, old, updated);
  res.json(updated);
});

// DMS Delete document
app.delete('/api/dms/documents/:id', auth, (req, res) => {
  const doc = db.findById('dms_documents', req.params.id);
  if (!doc) return res.status(404).json({ error: 'Document not found' });
  if (doc.filePath && fs.existsSync(doc.filePath)) {
    try { fs.unlinkSync(doc.filePath); } catch(e) {}
  }
  db.delete('dms_documents', req.params.id);
  auditLog(req.user.id, req.user.username, 'DELETE', 'dms_documents', doc.id, doc.originalName, doc, null);
  res.json({ success: true });
});

// DMS Download/serve file
app.get('/api/dms/files/:id', auth, (req, res) => {
  const doc = db.findById('dms_documents', req.params.id);
  if (!doc || !doc.filePath) return res.status(404).json({ error: 'File not found' });
  if (!fs.existsSync(doc.filePath)) return res.status(404).json({ error: 'File missing from disk' });
  res.sendFile(doc.filePath);
});

// DMS Categories
app.get('/api/dms/categories', auth, (req, res) => { res.json(db.findAll('dms_categories')); });
app.post('/api/dms/categories', auth, (req, res) => {
  const cat = db.insert('dms_categories', { id: crypto.randomUUID(), ...req.body, status: 'active' });
  res.status(201).json(cat);
});
app.put('/api/dms/categories/:id', auth, (req, res) => {
  const cat = db.update('dms_categories', req.params.id, req.body);
  if (!cat) return res.status(404).json({ error: 'Not found' });
  res.json(cat);
});
app.delete('/api/dms/categories/:id', auth, (req, res) => {
  db.delete('dms_categories', req.params.id);
  res.json({ success: true });
});

// DMS Check Out / Check In
app.post('/api/dms/documents/:id/checkout', auth, (req, res) => {
  const doc = db.findById('dms_documents', req.params.id);
  if (!doc) return res.status(404).json({ error: 'Not found' });
  if (doc.checkedOut) return res.status(400).json({ error: `Checked out by ${doc.checkedOutBy}` });
  const updated = db.update('dms_documents', req.params.id, { checkedOut: true, checkedOutBy: req.user.username, checkedOutAt: new Date().toISOString() });
  auditLog(req.user.id, req.user.username, 'UPDATE', 'dms_documents', doc.id, `Checkout: ${doc.originalName}`, doc, updated);
  res.json(updated);
});

app.post('/api/dms/documents/:id/checkin', auth, dmsUpload.single('file'), (req, res) => {
  const doc = db.findById('dms_documents', req.params.id);
  if (!doc) return res.status(404).json({ error: 'Not found' });
  const oldVersion = doc.version || 1;
  // Save old version
  db.insert('dms_versions', {
    documentId: doc.id, version: oldVersion, fileName: doc.fileName,
    filePath: doc.filePath, fileSize: doc.fileSize, uploadedBy: doc.uploadedBy,
    uploadedByName: doc.uploadedByName, comment: req.body.comment || '', createdAt: doc.updatedAt
  });
  // Update with new file
  const updates = { checkedOut: false, checkedOutBy: null, checkedOutAt: null, version: oldVersion + 1 };
  if (req.file) {
    updates.fileName = req.file.filename;
    updates.filePath = req.file.path;
    updates.fileSize = req.file.size;
    updates.mimeType = req.file.mimetype;
  }
  const updated = db.update('dms_documents', req.params.id, updates);
  auditLog(req.user.id, req.user.username, 'UPDATE', 'dms_documents', doc.id, `Checkin v${oldVersion + 1}: ${doc.originalName}`, doc, updated);
  res.json(updated);
});

// DMS Version history
app.get('/api/dms/documents/:id/versions', auth, (req, res) => {
  const versions = db.findMany('dms_versions', v => v.documentId === req.params.id);
  versions.sort((a, b) => b.version - a.version);
  res.json(versions);
});

// DMS Link document to ERP record
app.post('/api/dms/links', auth, (req, res) => {
  const { documentId, linkedTable, linkedId, linkType } = req.body;
  const link = db.insert('dms_links', { documentId, linkedTable, linkedId, linkType: linkType || 'related' });
  res.status(201).json(link);
});

app.get('/api/dms/links/:table/:recordId', auth, (req, res) => {
  const links = db.findMany('dms_links', l => l.linkedTable === req.params.table && l.linkedId === req.params.recordId);
  const docs = links.map(l => db.findById('dms_documents', l.documentId)).filter(Boolean);
  res.json(docs);
});

// DMS Stats
app.get('/api/dms/stats', auth, (req, res) => {
  const docs = db.findAll('dms_documents');
  const cats = db.findAll('dms_categories');
  const today = new Date().toISOString().slice(0, 10);
  const byCategory = {};
  const byType = {};
  let totalSize = 0;
  docs.forEach(d => {
    byCategory[d.categoryId] = (byCategory[d.categoryId] || 0) + 1;
    byType[d.type] = (byType[d.type] || 0) + 1;
    totalSize += d.fileSize || 0;
  });
  res.json({
    total: docs.length,
    totalSize,
    checkedOut: docs.filter(d => d.checkedOut).length,
    todayUploads: docs.filter(d => d.createdAt && d.createdAt.startsWith(today)).length,
    byCategory: cats.map(c => ({ ...c, count: byCategory[c.id] || 0 })),
    byType
  });
});

// DMS Search (full-text across all documents)
app.get('/api/dms/search', auth, (req, res) => {
  const { q } = req.query;
  if (!q) return res.json({ data: [] });
  const query = q.toLowerCase();
  let docs = db.findAll('dms_documents');
  docs = docs.filter(d =>
    (d.originalName || '').toLowerCase().includes(query) ||
    (d.description || '').toLowerCase().includes(query) ||
    (d.docNum || '').toLowerCase().includes(query) ||
    (d.keywords || []).some(k => k.toLowerCase().includes(query))
  );
  docs.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
  res.json({ data: docs.slice(0, 50), total: docs.length });
});

// =================== DMS WORKFLOWS ===================

// Seed default workflow templates
function initDMSWorkflows() {
  if (db.findAll('dms_workflows').length === 0) {
    const workflows = [
      { name: 'Invoice Approval', description: 'Standard invoice review and approval flow', status: 'active', steps: [
        { order: 1, name: 'Receipt Recording', assigneeRole: 'accounting', action: 'review', mandatory: true },
        { order: 2, name: 'Manager Approval', assigneeRole: 'admin', action: 'approve', mandatory: true },
        { order: 3, name: 'Payment Processing', assigneeRole: 'accounting', action: 'approve', mandatory: true },
      ]},
      { name: 'Contract Review', description: 'Legal review of contracts before signing', status: 'active', steps: [
        { order: 1, name: 'Legal Review', assigneeRole: 'admin', action: 'review', mandatory: true },
        { order: 2, name: 'Management Approval', assigneeRole: 'admin', action: 'approve', mandatory: true },
      ]},
      { name: 'Purchase Order Approval', description: 'PO approval workflow', status: 'active', steps: [
        { order: 1, name: 'Department Review', assigneeRole: 'user', action: 'review', mandatory: true },
        { order: 2, name: 'Finance Approval', assigneeRole: 'admin', action: 'approve', mandatory: true },
      ]},
      { name: 'Quality Document Review', description: 'Quality doc review before release', status: 'active', steps: [
        { order: 1, name: 'QA Review', assigneeRole: 'user', action: 'review', mandatory: true },
        { order: 2, name: 'QA Manager Approval', assigneeRole: 'admin', action: 'approve', mandatory: true },
      ]},
      { name: 'General Approval', description: 'Simple single-step approval', status: 'active', steps: [
        { order: 1, name: 'Manager Approval', assigneeRole: 'admin', action: 'approve', mandatory: true },
      ]},
    ];
    workflows.forEach(w => {
      const id = crypto.randomUUID();
      db.insert('dms_workflows', { id, name: w.name, description: w.description, status: w.status });
      w.steps.forEach(s => {
        db.insert('dms_workflow_steps', { id: crypto.randomUUID(), workflowId: id, ...s });
      });
    });
  }
}
initDMSWorkflows();

// List workflows
app.get('/api/dms/workflows', auth, (req, res) => {
  const workflows = db.findAll('dms_workflows');
  const result = workflows.map(w => ({
    ...w,
    steps: db.findMany('dms_workflow_steps', s => s.workflowId === w.id).sort((a, b) => a.order - b.order)
  }));
  res.json(result);
});

// Create workflow
app.post('/api/dms/workflows', auth, (req, res) => {
  const { name, description, steps } = req.body;
  const wf = db.insert('dms_workflows', { name, description, status: 'active' });
  if (steps && Array.isArray(steps)) {
    steps.forEach((s, i) => {
      db.insert('dms_workflow_steps', { id: crypto.randomUUID(), workflowId: wf.id, order: i + 1, ...s });
    });
  }
  res.status(201).json(wf);
});

// Update workflow
app.put('/api/dms/workflows/:id', auth, (req, res) => {
  const wf = db.update('dms_workflows', req.params.id, req.body);
  if (!wf) return res.status(404).json({ error: 'Not found' });
  res.json(wf);
});

// Delete workflow
app.delete('/api/dms/workflows/:id', auth, (req, res) => {
  db.findAll('dms_workflow_steps').filter(s => s.workflowId === req.params.id).forEach(s => db.delete('dms_workflow_steps', s.id));
  db.delete('dms_workflows', req.params.id);
  res.json({ success: true });
});

// =================== DMS APPROVALS ===================

// Start document on a workflow
app.post('/api/dms/documents/:id/start-workflow', auth, (req, res) => {
  const doc = db.findById('dms_documents', req.params.id);
  if (!doc) return res.status(404).json({ error: 'Document not found' });
  const { workflowId } = req.body;
  const steps = db.findMany('dms_workflow_steps', s => s.workflowId === workflowId).sort((a, b) => a.order - b.order);
  if (steps.length === 0) return res.status(400).json({ error: 'Workflow has no steps' });
  // Create approval records for each step
  steps.forEach((step, i) => {
    db.insert('dms_approvals', {
      documentId: doc.id, workflowId, stepId: step.id, stepOrder: step.order,
      stepName: step.name, action: step.action, mandatory: step.mandatory,
      assigneeRole: step.assigneeRole, status: i === 0 ? 'pending' : 'waiting',
      requestedBy: req.user.username, requestedAt: new Date().toISOString()
    });
  });
  const updated = db.update('dms_documents', doc.id, { workflowStatus: 'in_progress', workflowId });
  auditLog(req.user.id, req.user.username, 'UPDATE', 'dms_documents', doc.id, `Workflow started: ${doc.originalName}`, doc, updated);
  res.json(updated);
});

// Get approvals for a document
app.get('/api/dms/documents/:id/approvals', auth, (req, res) => {
  const approvals = db.findMany('dms_approvals', a => a.documentId === req.params.id);
  approvals.sort((a, b) => a.stepOrder - b.stepOrder);
  res.json(approvals);
});

// Process approval (approve/reject/comment)
app.post('/api/dms/approvals/:id/process', auth, (req, res) => {
  const approval = db.findById('dms_approvals', req.params.id);
  if (!approval) return res.status(404).json({ error: 'Approval not found' });
  const { decision, comment } = req.body; // decision: 'approved' or 'rejected'
  const updated = db.update('dms_approvals', req.params.id, {
    status: decision, processedBy: req.user.username, processedAt: new Date().toISOString(), comment
  });
  // If rejected, mark remaining steps as cancelled
  if (decision === 'rejected') {
    db.findMany('dms_approvals', a => a.documentId === approval.documentId && a.status === 'waiting')
      .forEach(a => db.update('dms_approvals', a.id, { status: 'cancelled' }));
    db.update('dms_documents', approval.documentId, { workflowStatus: 'rejected' });
  }
  // If approved, activate next step
  if (decision === 'approved') {
    const allApprovals = db.findMany('dms_approvals', a => a.documentId === approval.documentId).sort((a, b) => a.stepOrder - b.stepOrder);
    const nextPending = allApprovals.find(a => a.status === 'waiting');
    if (nextPending) {
      db.update('dms_approvals', nextPending.id, { status: 'pending' });
    } else {
      // All steps approved
      db.update('dms_documents', approval.documentId, { workflowStatus: 'approved' });
    }
  }
  auditLog(req.user.id, req.user.username, 'UPDATE', 'dms_approvals', approval.id, `${decision}: ${approval.stepName}`, approval, updated);
  res.json(updated);
});

// Get pending approvals for current user
app.get('/api/dms/approvals/pending', auth, (req, res) => {
  const all = db.findAll('dms_approvals');
  const pending = all.filter(a => a.status === 'pending');
  const result = pending.map(a => {
    const doc = db.findById('dms_documents', a.documentId);
    return { ...a, documentName: doc?.originalName, documentDocNum: doc?.docNum };
  });
  res.json(result);
});

// =================== DMS STAMPS ===================

// Stamp types: APPROVED, REJECTED, DRAFT, COPY, ORIGINAL, PAID, CANCELLED, CONFIDENTIAL
app.get('/api/dms/stamps', auth, (req, res) => {
  res.json([
    { code: 'APPROVED', label: 'APPROVED', color: '#00ff88', icon: '✓' },
    { code: 'REJECTED', label: 'REJECTED', color: '#ff4060', icon: '✕' },
    { code: 'DRAFT', label: 'DRAFT', color: '#ffb020', icon: '✎' },
    { code: 'COPY', label: 'COPY', color: '#6b7280', icon: '⊕' },
    { code: 'ORIGINAL', label: 'ORIGINAL', color: '#00d4ff', icon: '★' },
    { code: 'PAID', label: 'PAID', color: '#00ff88', icon: '$' },
    { code: 'CANCELLED', label: 'CANCELLED', color: '#ff4060', icon: '⊗' },
    { code: 'CONFIDENTIAL', label: 'CONFIDENTIAL', color: '#ec4899', icon: '🔒' },
  ]);
});

// Apply stamp to document
app.post('/api/dms/documents/:id/stamp', auth, (req, res) => {
  const doc = db.findById('dms_documents', req.params.id);
  if (!doc) return res.status(404).json({ error: 'Document not found' });
  const { stampCode, note } = req.body;
  const stamp = db.insert('dms_stamps', {
    documentId: doc.id, stampCode, note: note || '',
    stampedBy: req.user.username, stampedAt: new Date().toISOString()
  });
  const updated = db.update('dms_documents', doc.id, { stampStatus: stampCode });
  auditLog(req.user.id, req.user.username, 'UPDATE', 'dms_documents', doc.id, `Stamped ${stampCode}: ${doc.originalName}`, doc, updated);
  res.json(stamp);
});

// Get stamps for a document
app.get('/api/dms/documents/:id/stamps', auth, (req, res) => {
  const stamps = db.findMany('dms_stamps', s => s.documentId === req.params.id);
  stamps.sort((a, b) => new Date(b.stampedAt) - new Date(a.stampedAt));
  res.json(stamps);
});

// =================== DMS DELETION REQUESTS (Dual Control) ===================

// Request deletion of a document
app.post('/api/dms/documents/:id/request-deletion', auth, (req, res) => {
  const doc = db.findById('dms_documents', req.params.id);
  if (!doc) return res.status(404).json({ error: 'Document not found' });
  const { reason, deleteMode } = req.body; // deleteMode: 'full' or 'last_version'
  db.update('dms_documents', doc.id, { deletionStatus: 'marked', deletionRequestedBy: req.user.username, deletionRequestedAt: new Date().toISOString() });
  auditLog(req.user.id, req.user.username, 'UPDATE', 'dms_documents', doc.id, `Deletion requested (${deleteMode}): ${doc.originalName}`, doc, { deletionStatus: 'marked' });
  res.json({ success: true, message: 'Deletion request submitted. Pending authorized approval.' });
});

// Approve/reject deletion (admin only)
app.post('/api/dms/documents/:id/process-deletion', auth, requireAdmin, (req, res) => {
  const doc = db.findById('dms_documents', req.params.id);
  if (!doc) return res.status(404).json({ error: 'Document not found' });
  const { decision } = req.body; // 'approved' or 'rejected'
  if (decision === 'approved') {
    if (doc.filePath && fs.existsSync(doc.filePath)) {
      try { fs.unlinkSync(doc.filePath); } catch(e) {}
    }
    db.delete('dms_documents', doc.id);
    auditLog(req.user.id, req.user.username, 'DELETE', 'dms_documents', doc.id, `Deletion approved: ${doc.originalName}`, doc, null);
    res.json({ success: true, message: 'Document permanently deleted.' });
  } else {
    db.update('dms_documents', doc.id, { deletionStatus: 'rejected', deletionReviewedBy: req.user.username });
    auditLog(req.user.id, req.user.username, 'UPDATE', 'dms_documents', doc.id, `Deletion rejected: ${doc.originalName}`, doc, { deletionStatus: 'rejected' });
    res.json({ success: true, message: 'Deletion request rejected.' });
  }
});

// Get pending deletion requests
app.get('/api/dms/deletion-requests', auth, requireAdmin, (req, res) => {
  const docs = db.findAll('dms_documents').filter(d => d.deletionStatus === 'marked');
  res.json(docs);
});

// =================== DMS ADVANCED FEATURES ===================

// Full-text advanced search with filters
app.get('/api/dms/advanced-search', auth, (req, res) => {
  const { q, category, type, dateFrom, dateTo, uploadedBy, sizeMin, sizeMax, status, workflowStatus, stampStatus, keyword, page = 1, limit = 50 } = req.query;
  let docs = db.findAll('dms_documents');
  if (q) {
    const query = q.toLowerCase();
    docs = docs.filter(d =>
      (d.originalName || '').toLowerCase().includes(query) ||
      (d.description || '').toLowerCase().includes(query) ||
      (d.docNum || '').toLowerCase().includes(query) ||
      (d.keywords || []).some(k => k.toLowerCase().includes(query))
    );
  }
  if (category) docs = docs.filter(d => d.categoryId === category);
  if (type) docs = docs.filter(d => d.type === type);
  if (uploadedBy) docs = docs.filter(d => d.uploadedBy === uploadedBy);
  if (status) docs = docs.filter(d => d.status === status);
  if (workflowStatus) docs = docs.filter(d => d.workflowStatus === workflowStatus);
  if (stampStatus) docs = docs.filter(d => d.stampStatus === stampStatus);
  if (keyword) {
    const kw = keyword.toLowerCase();
    docs = docs.filter(d => (d.keywords || []).some(k => k.toLowerCase().includes(kw)));
  }
  if (dateFrom) docs = docs.filter(d => d.createdAt >= dateFrom);
  if (dateTo) docs = docs.filter(d => d.createdAt <= dateTo + 'T23:59:59');
  if (sizeMin) docs = docs.filter(d => (d.fileSize || 0) >= parseInt(sizeMin));
  if (sizeMax) docs = docs.filter(d => (d.fileSize || 0) <= parseInt(sizeMax));
  docs.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
  const total = docs.length;
  const start = (parseInt(page) - 1) * parseInt(limit);
  docs = docs.slice(start, start + parseInt(limit));
  res.json({ data: docs, total, page: parseInt(page), limit: parseInt(limit), pages: Math.ceil(total / parseInt(limit)) });
});

// OCR Wizard - extract keywords from document name/description
app.post('/api/dms/documents/:id/ocr-extract', auth, (req, res) => {
  const doc = db.findById('dms_documents', req.params.id);
  if (!doc) return res.status(404).json({ error: 'Document not found' });
  // Simulate OCR extraction from filename and description
  const text = `${doc.originalName || ''} ${doc.description || ''}`.toLowerCase();
  const extractedKeywords = new Set();
  // Extract date patterns
  const dates = text.match(/\d{4}[-\/]\d{2}[-\/]\d{2}|\d{2}[-\/]\d{2}[-\/]\d{4}/g);
  if (dates) dates.forEach(d => extractedKeywords.add(d));
  // Extract number patterns (invoice numbers, order numbers, etc.)
  const numbers = text.match(/(?:inv|po|so|dn|doc|ref|no|#)\s*[-:]?\s*\w+/gi);
  if (numbers) numbers.forEach(n => extractedKeywords.add(n.trim()));
  // Extract common business terms
  const terms = ['invoice', 'purchase', 'order', 'delivery', 'contract', 'agreement', 'receipt', 'quotation', 'tax', 'vat', 'payment', 'credit', 'debit', 'bill', 'shipping', 'warranty', 'insurance', 'compliance', 'certification', 'quality', 'inspection', 'report', 'statement', 'declaration', 'conformity'];
  terms.forEach(t => { if (text.includes(t)) extractedKeywords.add(t); });
  // Extract supplier/customer names from known entities
  const suppliers = db.findAll('suppliers');
  const customers = db.findAll('customers');
  suppliers.forEach(s => { if (text.includes(s.name.toLowerCase())) extractedKeywords.add(s.name); });
  customers.forEach(c => { if (text.includes(c.name.toLowerCase())) extractedKeywords.add(c.name); });
  const keywords = Array.from(extractedKeywords);
  // Auto-update document keywords
  const existing = doc.keywords || [];
  const merged = [...new Set([...existing, ...keywords])];
  const updated = db.update('dms_documents', doc.id, { keywords: merged, ocrExtracted: true, ocrDate: new Date().toISOString() });
  auditLog(req.user.id, req.user.username, 'UPDATE', 'dms_documents', doc.id, `OCR extracted: ${doc.originalName}`, doc, { keywords: merged });
  res.json({ keywords, totalExtracted: keywords.length, document: updated });
});

// Duplicate detection
app.get('/api/dms/duplicates', auth, (req, res) => {
  const docs = db.findAll('dms_documents');
  const duplicates = [];
  // Check by filename similarity
  for (let i = 0; i < docs.length; i++) {
    for (let j = i + 1; j < docs.length; j++) {
      const name1 = (docs[i].originalName || '').toLowerCase().replace(/[-_\s]/g, '');
      const name2 = (docs[j].originalName || '').toLowerCase().replace(/[-_\s]/g, '');
      const size1 = docs[i].fileSize || 0;
      const size2 = docs[j].fileSize || 0;
      // Exact name match
      if (name1 === name2 && name1.length > 0) {
        duplicates.push({ doc1: docs[i], doc2: docs[j], reason: 'Exact filename match', severity: 'high' });
      }
      // Same size and similar name (>70% match)
      else if (size1 === size2 && size1 > 0) {
        const shorter = name1.length < name2.length ? name1 : name2;
        const longer = name1.length < name2.length ? name2 : name1;
        if (longer.includes(shorter) && shorter.length > 5) {
          duplicates.push({ doc1: docs[i], doc2: docs[j], reason: `Same file size (${size1} bytes) and name contains match`, severity: 'medium' });
        }
      }
    }
  }
  res.json({ data: duplicates, total: duplicates.length });
});

// Set resubmission date
app.post('/api/dms/documents/:id/resubmission', auth, (req, res) => {
  const doc = db.findById('dms_documents', req.params.id);
  if (!doc) return res.status(404).json({ error: 'Document not found' });
  const { resubmissionDate, resubmissionNote } = req.body;
  const updated = db.update('dms_documents', doc.id, { resubmissionDate, resubmissionNote, resubmissionSetBy: req.user.username });
  auditLog(req.user.id, req.user.username, 'UPDATE', 'dms_documents', doc.id, `Resubmission set: ${doc.originalName}`, doc, { resubmissionDate });
  res.json(updated);
});

// Get upcoming resubmissions
app.get('/api/dms/resubmissions', auth, (req, res) => {
  const docs = db.findAll('dms_documents').filter(d => d.resubmissionDate);
  const now = new Date();
  const upcoming = docs.filter(d => new Date(d.resubmissionDate) >= now).sort((a, b) => new Date(a.resubmissionDate) - new Date(b.resubmissionDate));
  const overdue = docs.filter(d => new Date(d.resubmissionDate) < now).sort((a, b) => new Date(a.resubmissionDate) - new Date(b.resubmissionDate));
  res.json({ upcoming, overdue, total: docs.length });
});

// Auto-archive based on retention policy
app.post('/api/dms/auto-archive', auth, requireAdmin, (req, res) => {
  const cats = db.findAll('dms_categories');
  const docs = db.findAll('dms_documents');
  let archived = 0;
  const now = new Date();
  docs.forEach(d => {
    if (d.status === 'archived') return;
    const cat = cats.find(c => c.id === d.categoryId);
    const retentionYears = cat?.retention || 5;
    const createdAt = new Date(d.createdAt);
    const archiveDate = new Date(createdAt);
    archiveDate.setFullYear(archiveDate.getFullYear() + retentionYears);
    if (now > archiveDate) {
      db.update('dms_documents', d.id, { status: 'archived', archivedAt: now.toISOString(), archiveReason: 'Auto-archived by retention policy' });
      archived++;
    }
  });
  if (archived > 0) {
    auditLog(req.user.id, req.user.username, 'UPDATE', 'dms_documents', null, `Auto-archived ${archived} documents by retention policy`, null, { archived });
  }
  res.json({ archived, message: `${archived} documents archived based on retention policy` });
});

// Get retention policy summary
app.get('/api/dms/retention', auth, (req, res) => {
  const cats = db.findAll('dms_categories');
  const docs = db.findAll('dms_documents');
  const now = new Date();
  const summary = cats.map(c => {
    const catDocs = docs.filter(d => d.categoryId === c.id);
    const active = catDocs.filter(d => d.status !== 'archived');
    const archived = catDocs.filter(d => d.status === 'archived');
    const nearingExpiry = active.filter(d => {
      const created = new Date(d.createdAt);
      const expiry = new Date(created);
      expiry.setFullYear(expiry.getFullYear() + (c.retention || 5));
      const daysUntilExpiry = (expiry - now) / (1000 * 60 * 60 * 24);
      return daysUntilExpiry <= 90 && daysUntilExpiry > 0;
    });
    return { category: c.name, code: c.code, retention: c.retention, total: catDocs.length, active: active.length, archived: archived.length, nearingExpiry: nearingExpiry.length };
  });
  res.json(summary);
});

// Document analytics / activity log
app.get('/api/dms/activity', auth, (req, res) => {
  const logs = db.findAll('audit_trail').filter(l => l.table === 'dms_documents');
  logs.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
  res.json({ data: logs.slice(0, 100), total: logs.length });
});

// Batch operations
app.post('/api/dms/batch/archive', auth, requireAdmin, (req, res) => {
  const { documentIds } = req.body;
  if (!Array.isArray(documentIds)) return res.status(400).json({ error: 'documentIds array required' });
  let count = 0;
  documentIds.forEach(id => {
    const doc = db.findById('dms_documents', id);
    if (doc) {
      db.update('dms_documents', id, { status: 'archived', archivedAt: new Date().toISOString() });
      count++;
    }
  });
  auditLog(req.user.id, req.user.username, 'UPDATE', 'dms_documents', null, `Batch archived ${count} documents`, null, { count });
  res.json({ archived: count });
});

app.post('/api/dms/batch/category', auth, (req, res) => {
  const { documentIds, categoryId } = req.body;
  if (!Array.isArray(documentIds)) return res.status(400).json({ error: 'documentIds array required' });
  let count = 0;
  documentIds.forEach(id => {
    const doc = db.findById('dms_documents', id);
    if (doc) {
      db.update('dms_documents', id, { categoryId });
      count++;
    }
  });
  res.json({ updated: count });
});

// =================== AUDIT TRAIL API ===================
app.get('/api/audit-trail', auth, (req, res) => {
  let logs = db.findAll('audit_trail');
  const { table, action, userId, search, page = 1, limit = 50 } = req.query;
  if (table) logs = logs.filter(l => l.table === table);
  if (action) logs = logs.filter(l => l.action === action);
  if (userId) logs = logs.filter(l => l.userId === userId);
  if (search) {
    const q = search.toLowerCase();
    logs = logs.filter(l => (l.recordLabel || '').toLowerCase().includes(q) || (l.username || '').toLowerCase().includes(q) || (l.table || '').toLowerCase().includes(q));
  }
  logs.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
  const total = logs.length;
  const start = (parseInt(page) - 1) * parseInt(limit);
  logs = logs.slice(start, start + parseInt(limit));
  res.json({ data: logs, total, page: parseInt(page), limit: parseInt(limit), pages: Math.ceil(total / parseInt(limit)) });
});

app.get('/api/audit-trail/stats', auth, (req, res) => {
  const logs = db.findAll('audit_trail');
  const today = new Date().toISOString().slice(0, 10);
  const todayLogs = logs.filter(l => l.timestamp && l.timestamp.startsWith(today));
  const byAction = {};
  const byTable = {};
  const byUser = {};
  logs.forEach(l => {
    byAction[l.action] = (byAction[l.action] || 0) + 1;
    byTable[l.table] = (byTable[l.table] || 0) + 1;
    byUser[l.username] = (byUser[l.username] || 0) + 1;
  });
  res.json({
    total: logs.length,
    todayCount: todayLogs.length,
    byAction, byTable, byUser,
    recent: logs.slice(-10).reverse()
  });
});

// =================== TIME ATTENDANCE ===================

// Clock In
app.post('/api/time-attendance/clock-in', auth, (req, res) => {
  const { employeeId, location, notes } = req.body;
  if (!employeeId) return res.status(400).json({ error: 'Employee ID required' });
  
  const employee = db.findById('employees', employeeId);
  if (!employee) return res.status(404).json({ error: 'Employee not found' });
  
  // Check for existing open clock-in today
  const today = new Date().toISOString().slice(0, 10);
  const existing = (db.data.attendances || []).find(a => 
    a.employeeId === employeeId && a.date === today && !a.clockOut
  );
  if (existing) return res.status(400).json({ error: 'Already clocked in today', record: existing });
  
  const record = {
    id: `ta-${Date.now()}-${Math.random().toString(36).slice(2,6)}`,
    employeeId, employeeName: `${employee.firstName} ${employee.lastName}`,
    department: employee.department, farm: employee.farm || 'All',
    date: today, clockIn: new Date().toISOString(),
    clockOut: null, hoursWorked: 0, overtime: 0,
    location: location || 'farm', notes: notes || '',
    status: 'active', createdAt: new Date().toISOString(), updatedAt: new Date().toISOString()
  };
  db.data.attendances = db.data.attendances || [];
  db.data.attendances.push(record);
  db.save();
  
  // Audit trail
  db.data.audit_trail = db.data.audit_trail || [];
  db.data.audit_trail.push({ id: `at-${Date.now()}`, table: 'attendances', action: 'clock-in', recordId: record.id, username: req.user?.username || 'system', timestamp: new Date().toISOString(), details: `${employee.firstName} ${employee.lastName} clocked in` });
  db.save();
  
  res.json(record);
});

// Clock Out
app.put('/api/time-attendance/clock-out/:id', auth, (req, res) => {
  const record = (db.data.attendances || []).find(a => a.id === req.params.id);
  if (!record) return res.status(404).json({ error: 'Record not found' });
  if (record.clockOut) return res.status(400).json({ error: 'Already clocked out' });
  
  record.clockOut = new Date().toISOString();
  const clockIn = new Date(record.clockIn);
  const clockOut = new Date(record.clockOut);
  const diffMs = clockOut - clockIn;
  record.hoursWorked = Math.round(diffMs / (1000 * 60 * 60) * 100) / 100;
  record.overtime = Math.max(0, record.hoursWorked - 8);
  record.status = 'completed';
  record.updatedAt = new Date().toISOString();
  db.save();
  
  res.json(record);
});

// Get all attendance records (with filters)
app.get('/api/time-attendance', auth, (req, res) => {
  let records = db.data.attendances || [];
  const { employeeId, department, farm, startDate, endDate, status } = req.query;
  
  if (employeeId) records = records.filter(r => r.employeeId === employeeId);
  if (department) records = records.filter(r => r.department === department);
  if (farm) records = records.filter(r => r.farm === farm);
  if (startDate) records = records.filter(r => r.date >= startDate);
  if (endDate) records = records.filter(r => r.date <= endDate);
  if (status) records = records.filter(r => r.status === status);
  
  // Sort by date desc, then clockIn desc
  records.sort((a, b) => (b.date + (b.clockIn || '')) > (a.date + (a.clockIn || '')) ? 1 : -1);
  
  res.json({ data: records, total: records.length });
});

// Timesheet summary (weekly/monthly)
app.get('/api/time-attendance/timesheet', auth, (req, res) => {
  const { employeeId, startDate, endDate } = req.query;
  let records = db.data.attendances || [];
  
  if (employeeId) records = records.filter(r => r.employeeId === employeeId);
  if (startDate) records = records.filter(r => r.date >= startDate);
  if (endDate) records = records.filter(r => r.date <= endDate);
  
  // Group by employee
  const byEmployee = {};
  records.forEach(r => {
    if (!byEmployee[r.employeeId]) {
      byEmployee[r.employeeId] = {
        employeeId: r.employeeId, employeeName: r.employeeName,
        department: r.department, farm: r.farm,
        totalDays: 0, totalHours: 0, totalOvertime: 0,
        days: {}
      };
    }
    const emp = byEmployee[r.employeeId];
    emp.totalDays++;
    emp.totalHours += r.hoursWorked || 0;
    emp.totalOvertime += r.overtime || 0;
    emp.days[r.date] = { clockIn: r.clockIn, clockOut: r.clockOut, hours: r.hoursWorked, overtime: r.overtime };
  });
  
  res.json({ data: Object.values(byEmployee), summary: { totalEmployees: Object.keys(byEmployee).length, totalDays: records.length, totalHours: records.reduce((s, r) => s + (r.hoursWorked || 0), 0), totalOvertime: records.reduce((s, r) => s + (r.overtime || 0), 0) } });
});

// Attendance stats
app.get('/api/time-attendance/stats', auth, (req, res) => {
  const records = db.data.attendances || [];
  const today = new Date().toISOString().slice(0, 10);
  const todayRecords = records.filter(r => r.date === today);
  const thisMonth = today.substring(0, 7);
  const monthRecords = records.filter(r => r.date && r.date.startsWith(thisMonth));
  
  const totalEmployees = (db.findAll('employees') || []).filter(e => e.status === 'active').length;
  const clockedInToday = todayRecords.filter(r => !r.clockOut).length;
  
  // Department breakdown
  const byDept = {};
  monthRecords.forEach(r => {
    const d = r.department || 'Unknown';
    if (!byDept[d]) byDept[d] = { department: d, totalDays: 0, totalHours: 0, totalOvertime: 0 };
    byDept[d].totalDays++;
    byDept[d].totalHours += r.hoursWorked || 0;
    byDept[d].totalOvertime += r.overtime || 0;
  });
  
  // Farm breakdown
  const byFarm = {};
  monthRecords.forEach(r => {
    const f = r.farm || 'Unknown';
    if (!byFarm[f]) byFarm[f] = { farm: f, totalDays: 0, totalHours: 0, totalOvertime: 0 };
    byFarm[f].totalDays++;
    byFarm[f].totalHours += r.hoursWorked || 0;
    byFarm[f].totalOvertime += r.overtime || 0;
  });
  
  res.json({
    totalEmployees, clockedInToday,
    todayTotal: todayRecords.length,
    monthTotal: monthRecords.length,
    monthHours: monthRecords.reduce((s, r) => s + (r.hoursWorked || 0), 0),
    monthOvertime: monthRecords.reduce((s, r) => s + (r.overtime || 0), 0),
    byDept: Object.values(byDept),
    byFarm: Object.values(byFarm)
  });
});

// Delete attendance record
app.delete('/api/time-attendance/:id', auth, (req, res) => {
  const idx = (db.data.attendances || []).findIndex(a => a.id === req.params.id);
  if (idx === -1) return res.status(404).json({ error: 'Record not found' });
  db.data.attendances.splice(idx, 1);
  db.save();
  res.json({ success: true });
});

// =================== ENHANCED GENERIC CRUD WITH VALIDATION ===================
MODULES.forEach(m => {
  app.get(`/api/${m.prefix}`, auth, (req, res) => { res.json(paginatedResults(m.name, req)); });
  app.get(`/api/${m.prefix}/all`, auth, (req, res) => { res.json({ data: db.findAll(m.name) }); });
  app.get(`/api/${m.prefix}/:id`, auth, (req, res) => {
    const r = db.findById(m.name, req.params.id);
    if (!r) return res.status(404).json({ error: 'Not found' });
    if (['products','customers','sales_orders','support_tickets','projects','employees'].includes(m.name)) {
      r.attachments = db.findMany('attachments', a => a.recordId === req.params.id);
    }
    res.json(r);
  });
  app.post(`/api/${m.prefix}`, auth, (req, res) => {
    const fkErr = validateFKs(m.name, req.body);
    if (fkErr) return res.status(400).json({ error: 'Invalid reference', details: fkErr });
    const uErr = validateUnique(m.name, req.body);
    if (uErr) return res.status(400).json({ error: 'Duplicate value', details: uErr });
    const r = db.insert(m.name, req.body);
    auditLog(req.user.id, req.user.username, 'CREATE', m.name, r.id, r.name || r.title || r.number || r.sku || r.id, null, r);
    res.status(201).json(r);
  });
  app.put(`/api/${m.prefix}/:id`, auth, (req, res) => {
    const existing = db.findById(m.name, req.params.id);
    if (!existing) return res.status(404).json({ error: 'Not found' });
    const fkErr = validateFKs(m.name, req.body);
    if (fkErr) return res.status(400).json({ error: 'Invalid reference', details: fkErr });
    const uErr = validateUnique(m.name, req.body, req.params.id);
    if (uErr) return res.status(400).json({ error: 'Duplicate value', details: uErr });
    const r = db.update(m.name, req.params.id, req.body);
    auditLog(req.user.id, req.user.username, 'UPDATE', m.name, r.id, r.name || r.title || r.number || r.sku || r.id, existing, r);
    res.json(r);
  });
  app.patch(`/api/${m.prefix}/:id`, auth, (req, res) => {
    const existing = db.findById(m.name, req.params.id);
    const r = db.update(m.name, req.params.id, req.body);
    if (!r) return res.status(404).json({ error: 'Not found' });
    auditLog(req.user.id, req.user.username, 'UPDATE', m.name, r.id, r.name || r.title || r.number || r.sku || r.id, existing, r);
    res.json(r);
  });
  app.delete(`/api/${m.prefix}/:id`, auth, (req, res) => {
    const existing = db.findById(m.name, req.params.id);
    if (!existing) return res.status(404).json({ error: 'Not found' });
    const d = db.delete(m.name, req.params.id);
    auditLog(req.user.id, req.user.username, 'DELETE', m.name, req.params.id, existing.name || existing.title || existing.number || existing.sku || req.params.id, existing, null);
    res.json({ success: true });
  });
});

// CSV Export
app.get('/api/export/csv', auth, (req, res) => {
  const { type } = req.query;
  const tableMap = { products: 'products', orders: 'sales_orders', customers: 'customers', inventory: 'products', users: 'users', employees: 'employees', suppliers: 'suppliers', accounts: 'accounts' };
  const table = tableMap[type] || type;
  const rows = db.findAll(table);
  if (!rows.length) return res.status(200).send('No data');
  const headers = Object.keys(rows[0]);
  const csv = [headers.join(','), ...rows.map(r => headers.map(h => '"' + String(r[h] ?? '').replace(/"/g, '""') + '"').join(','))].join('\n');
  res.header('Content-Type', 'text/csv');
  res.header('Content-Disposition', `attachment; filename="${type}_export.csv"`);
  res.send(csv);
});

// Error handler
app.use((err, req, res, next) => { console.error('Error:', err); res.status(500).json({ error: 'Internal server error' }); });

// ====== BVA REPORT MODULE ======
const BVA_CATEGORIES = [
  { name: 'Nursery', code: 'NUR', color: '#1565c0' },
  { name: 'Maintenance', code: 'MAINT', color: '#42a5f5' },
  { name: 'Salaries', code: 'SAL', color: '#f57c00' },
  { name: 'Head Office', code: 'HO', color: '#ff9800' },
  { name: 'Vehicle', code: 'VEH', color: '#7b1fa2' },
  { name: 'Establishment', code: 'EST', color: '#00838f' },
  { name: 'Infrastructure', code: 'INFRA', color: '#d32f2f' },
  { name: 'EHS', code: 'EHS', color: '#2e7d32' },
  { name: 'Insurance', code: 'INS', color: '#90caf9' },
  { name: 'Repairs', code: 'REP', color: '#ff8f00' },
  { name: 'Finance/Legal', code: 'FIN', color: '#e0e0e0' },
  { name: 'Municipality', code: 'MUN', color: '#bbdefb' },
  { name: 'Other Admin', code: 'OTH', color: '#c8e6c9' },
  { name: 'Management', code: 'MGMT', color: '#b71c1c' }
];
const BVA_FUNDING_SOURCES = ['Shareholder Loan', 'Intercompany', 'Mgmt Fee', 'Sundry', 'Interest'];
const BVA_WORKFORCE_ROLES = ['Scanned In', 'Fert Spray', 'Team Leaders', 'Drivers', 'Workshop', 'Infra', 'Grounds', 'Impact', 'Other'];
const MONTHS = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];

function seedBVA() {
  // Ensure all BVA tables exist (migration for existing DBs)
  const bvaTables = ['bva_categories','bva_budgets','bva_actuals','bva_cash_flows','bva_funding_sources','bva_workforce','bva_field_ops','bva_alerts','bva_scenarios','bva_forecasts','bva_notes'];
  bvaTables.forEach(t => { if (!db.data[t]) db.data[t] = []; });
  if (!db.data.bva_categories || db.data.bva_categories.length === 0) {
    db.data.bva_categories = BVA_CATEGORIES.map((c, i) => ({
      id: `bva-cat-${i}`, name: c.name, code: c.code, color: c.color, status: 'active',
      retentionYears: 10, createdAt: new Date().toISOString(), updatedAt: new Date().toISOString()
    }));
  }
  if (!db.data.bva_funding_sources || db.data.bva_funding_sources.length === 0) {
    db.data.bva_funding_sources = BVA_FUNDING_SOURCES.map((s, i) => ({
      id: `bva-fs-${i}`, name: s, status: 'active',
      createdAt: new Date().toISOString(), updatedAt: new Date().toISOString()
    }));
  }
  if (!db.data.bva_budgets || db.data.bva_budgets.length === 0) {
    const sampleBudgets = [
      { cat: 'Nursery', m: [0,122034,39257,108859,121419,127490,0,0,0,0,0,0] },
      { cat: 'Maintenance', m: [472011,438765,450171,855984,963412,1011583,0,0,0,0,0,0] },
      { cat: 'Salaries', m: [184976,194926,213416,238716,236307,238716,0,0,0,0,0,0] },
      { cat: 'Head Office', m: [233074,237818,425078,239837,263218,231418,0,0,0,0,0,0] },
      { cat: 'Vehicle', m: [35769,61038,77320,93603,100103,96603,0,0,0,0,0,0] },
      { cat: 'Infrastructure', m: [0,0,78261,0,0,0,0,0,0,0,0,0] },
      { cat: 'EHS', m: [0,28552,0,0,55209,0,0,0,0,0,0,0] },
      { cat: 'Management', m: [0,-200000,-200000,-200000,-200000,-200000,0,0,0,0,0,0] }
    ];
    sampleBudgets.forEach(b => {
      const total = b.m.reduce((a, v) => a + v, 0);
      db.data.bva_budgets.push({
        id: `bva-bud-${Date.now()}-${Math.random().toString(36).slice(2,6)}`,
        year: 2026, category: b.cat,
        month1: b.m[0], month2: b.m[1], month3: b.m[2], month4: b.m[3],
        month5: b.m[4], month6: b.m[5], month7: b.m[6], month8: b.m[7],
        month9: b.m[8], month10: b.m[9], month11: b.m[10], month12: b.m[11],
        total, status: 'active', createdAt: new Date().toISOString(), updatedAt: new Date().toISOString()
      });
    });
  }
  if (!db.data.bva_actuals || db.data.bva_actuals.length === 0) {
    const sampleActuals = [
      { cat: 'Nursery', m: [15315,67306,194761,126316,95304,105072] },
      { cat: 'Maintenance', m: [333075,407256,155469,208895,286748,289130] },
      { cat: 'Salaries', m: [214618,201619,331464,308650,301785,656614] },
      { cat: 'Head Office', m: [182205,156134,340175,116961,196952,208408] },
      { cat: 'Vehicle', m: [104006,73123,98865,74755,146110,131657] },
      { cat: 'Establishment', m: [0,0,0,41349,0,10185] },
      { cat: 'Infrastructure', m: [12066,58289,13433,13764,13951,7470] },
      { cat: 'EHS', m: [0,0,1279,36610,10802,4651] },
      { cat: 'Insurance', m: [10737,10971,10971,11016,11003,12473] },
      { cat: 'Repairs', m: [47766,1335,8052,9092,575,56143] },
      { cat: 'Municipality', m: [4920,8256,0,19262,13692,9114] },
      { cat: 'Other Admin', m: [62390,0,0,0,0,0] },
      { cat: 'Management', m: [0,0,0,0,800000,200000] }
    ];
    sampleActuals.forEach(a => {
      a.m.forEach((amt, i) => {
        if (amt !== 0) {
          const bud = db.data.bva_budgets.find(b => b.category === a.cat && b.year === 2026);
          const budAmt = bud ? bud[`month${i+1}`] : 0;
          db.data.bva_actuals.push({
            id: `bva-act-${Date.now()}-${Math.random().toString(36).slice(2,6)}`,
            year: 2026, month: i + 1, category: a.cat, amount: amt,
            variance: budAmt ? amt - budAmt : amt,
            variancePercent: budAmt ? Math.round((amt - budAmt) / Math.abs(budAmt) * 1000) / 10 : 0,
            source: 'manual', status: 'active',
            createdAt: new Date().toISOString(), updatedAt: new Date().toISOString()
          });
        }
      });
    });
  }
  if (!db.data.bva_workforce || db.data.bva_workforce.length === 0) {
    const wfSample = [
      [60,74,39,3,1,6,5,1,2,17],[70,73,48,3,1,6,5,1,2,6],[70,74,49,1,1,5,5,1,2,5],
      [70,74,48,5,1,6,5,1,1,5],[73,74,51,0,1,6,5,1,3,4],[68,74,47,0,1,6,5,1,2,5]
    ];
    wfSample.forEach((w, i) => {
      const d = new Date(2026, 0, 6 + i * 5);
      db.data.bva_workforce.push({
        id: `bva-wf-${Date.now()}-${i}`,
        date: d.toISOString().split('T')[0],
        scannedIn: w[0], fertSpray: w[2], teamLeaders: w[3], drivers: w[4],
        workshop: w[5], infra: w[6], grounds: w[7], impact: w[8], other: w[9],
        totalMandays: w[0], mandaysBudget: 70, mandaysBalance: 70 - w[0],
        rainDay: i === 1, rainDayCount: i === 1 ? 1 : 0,
        status: 'active', createdAt: new Date().toISOString(), updatedAt: new Date().toISOString()
      });
    });
  }
  if (!db.data.bva_field_ops || db.data.bva_field_ops.length === 0) {
    const foSample = [
      { ha: 28.33, aK: 104.85, mf: 25, nm: 3.33, est: 0 },
      { ha: 51.16, aK: 144, mf: 45, nm: 6.16, est: 0 },
      { ha: 41.63, aK: 90, mf: 35, nm: 6.63, est: 0 },
      { ha: 45.94, aK: 180, mf: 40, nm: 5.94, est: 0 },
      { ha: 49.89, aK: 213, mf: 44, nm: 5.89, est: 0 },
      { ha: 47.73, aK: 230, mf: 42, nm: 5.73, est: 0 }
    ];
    foSample.forEach((f, i) => {
      const d = new Date(2026, 0, 6 + i * 5);
      db.data.bva_field_ops.push({
        id: `bva-fo-${Date.now()}-${i}`,
        date: d.toISOString().split('T')[0],
        hectaresFertilized: f.ha, aminoKUsage: f.aK,
        maintenanceFertilizingHa: f.mf, nurseryMaintenanceHa: f.nm,
        establishmentHa: f.est, apoTarget: 16.5,
        status: 'active', createdAt: new Date().toISOString(), updatedAt: new Date().toISOString()
      });
    });
  }
  db.save();
}

// BVA Dashboard endpoint
app.get('/api/bva/dashboard', auth, (req, res) => {
  const year = parseInt(req.query.year) || 2026;
  const budgets = (db.data.bva_budgets || []).filter(b => b.year === year);
  const actuals = (db.data.bva_actuals || []).filter(a => a.year === year);
  const categories = db.data.bva_categories || [];
  const alerts = (db.data.bva_alerts || []).filter(a => !a.dismissed);
  const workforce = (db.data.bva_workforce || []).filter(w => w.date && w.date.startsWith(String(year)));
  const fieldOps = (db.data.bva_field_ops || []).filter(f => f.date && f.date.startsWith(String(year)));
  const cashFlows = (db.data.bva_cash_flows || []).filter(c => c.date && c.date.startsWith(String(year)));

  const totalBudget = budgets.reduce((a, b) => a + (b.total || 0), 0);
  const totalActual = actuals.reduce((a, b) => a + (b.amount || 0), 0);
  const totalIncome = cashFlows.filter(c => c.type === 'income').reduce((a, c) => a + (c.amount || 0), 0);

  const monthlyBudget = MONTHS.map((_, i) => budgets.reduce((a, b) => a + (b[`month${i+1}`] || 0), 0));
  const monthlyActual = MONTHS.map((_, i) => actuals.filter(a => a.month === i + 1).reduce((a, b) => a + (b.amount || 0), 0));

  const catSummary = categories.map(c => {
    const bud = budgets.find(b => b.category === c.name);
    const catActuals = actuals.filter(a => a.category === c.name);
    const totalA = catActuals.reduce((a, b) => a + (b.amount || 0), 0);
    const totalB = bud ? bud.total : 0;
    return { ...c, budget: totalB, actual: totalA, variance: totalA - totalB, variancePercent: totalB ? Math.round((totalA - totalB) / Math.abs(totalB) * 1000) / 10 : 0 };
  });

  const avgWorkforce = workforce.length > 0 ? Math.round(workforce.reduce((a, w) => a + (w.scannedIn || 0), 0) / workforce.length) : 0;
  const totalHa = fieldOps.reduce((a, f) => a + (f.hectaresFertilized || 0), 0);

  res.json({ year, totalBudget, totalActual, totalIncome, variance: totalActual - totalBudget, monthlyBudget, monthlyActual, catSummary, alerts, avgWorkforce, totalHa, workforce, fieldOps, cashFlows });
});

// BVA Budgets CRUD
app.get('/api/bva/budgets', auth, (req, res) => {
  const year = parseInt(req.query.year) || 2026;
  res.json((db.data.bva_budgets || []).filter(b => b.year === year));
});
app.post('/api/bva/budgets', auth, (req, res) => {
  const { year, category, months, total } = req.body;
  let existing = (db.data.bva_budgets || []).find(b => b.year === (year||2026) && b.category === category);
  if (existing) {
    Object.assign(existing, { ...months, total: total || Object.values(months).reduce((a,v)=>a+(v||0),0), updatedAt: new Date().toISOString() });
  } else {
    const id = `bva-bud-${Date.now()}-${Math.random().toString(36).slice(2,6)}`;
    existing = { id, year: year||2026, category, ...months, total: total||0, status:'active', createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() };
    db.data.bva_budgets.push(existing);
  }
  db.save(); res.json(existing);
});

// BVA Actuals
app.get('/api/bva/actuals', auth, (req, res) => {
  let acts = db.data.bva_actuals || [];
  if (req.query.year) acts = acts.filter(a => a.year === parseInt(req.query.year));
  if (req.query.month) acts = acts.filter(a => a.month === parseInt(req.query.month));
  if (req.query.category) acts = acts.filter(a => a.category === req.query.category);
  res.json(acts);
});
app.post('/api/bva/actuals', auth, (req, res) => {
  const { year, month, category, amount, source } = req.body;
  const bud = (db.data.bva_budgets || []).find(b => b.year === (year||2026) && b.category === category);
  const budAmt = bud ? bud[`month${month}`] || 0 : 0;
  const entry = { id: `bva-act-${Date.now()}-${Math.random().toString(36).slice(2,6)}`, year: year||2026, month, category, amount: amount||0, variance: amount-budAmt, variancePercent: budAmt ? Math.round((amount-budAmt)/Math.abs(budAmt)*1000)/10 : 0, source: source||'manual', status:'active', createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() };
  db.data.bva_actuals.push(entry); db.save(); res.json(entry);
});

// BVA Cash Flows
app.get('/api/bva/cashflows', auth, (req, res) => {
  let cfs = db.data.bva_cash_flows || [];
  if (req.query.year) cfs = cfs.filter(c => c.date && c.date.startsWith(String(req.query.year)));
  if (req.query.type) cfs = cfs.filter(c => c.type === req.query.type);
  res.json(cfs);
});
app.post('/api/bva/cashflows', auth, (req, res) => {
  const entry = { id: `bva-cf-${Date.now()}-${Math.random().toString(36).slice(2,6)}`, ...req.body, status:'active', createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() };
  db.data.bva_cash_flows.push(entry); db.save(); res.json(entry);
});
app.get('/api/bva/funding-sources', auth, (req, res) => { res.json(db.data.bva_funding_sources || []); });

// BVA Workforce
app.get('/api/bva/workforce', auth, (req, res) => {
  let wf = db.data.bva_workforce || [];
  if (req.query.month) { const ym = req.query.month.substring(0,7); wf = wf.filter(w => w.date && w.date.startsWith(ym)); }
  if (req.query.year) wf = wf.filter(w => w.date && w.date.startsWith(String(req.query.year)));
  res.json(wf);
});
app.post('/api/bva/workforce', auth, (req, res) => {
  const entry = { id: `bva-wf-${Date.now()}-${Math.random().toString(36).slice(2,6)}`, ...req.body, status:'active', createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() };
  db.data.bva_workforce.push(entry); db.save(); res.json(entry);
});

// BVA Field Ops
app.get('/api/bva/field-ops', auth, (req, res) => {
  let fo = db.data.bva_field_ops || [];
  if (req.query.year) fo = fo.filter(f => f.date && f.date.startsWith(String(req.query.year)));
  res.json(fo);
});
app.post('/api/bva/field-ops', auth, (req, res) => {
  const entry = { id: `bva-fo-${Date.now()}-${Math.random().toString(36).slice(2,6)}`, ...req.body, status:'active', createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() };
  db.data.bva_field_ops.push(entry); db.save(); res.json(entry);
});

// BVA Scenarios
app.get('/api/bva/scenarios', auth, (req, res) => { res.json(db.data.bva_scenarios || []); });
app.post('/api/bva/scenarios', auth, (req, res) => {
  const entry = { id: `bva-sc-${Date.now()}-${Math.random().toString(36).slice(2,6)}`, ...req.body, createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() };
  db.data.bva_scenarios.push(entry); db.save(); res.json(entry);
});
app.post('/api/bva/scenarios/calculate', auth, (req, res) => {
  const { labourCostChange, materialCostChange, fxRate, headcountChange, haPlantedChange, contingency } = req.body;
  const totalAct = (db.data.bva_actuals || []).reduce((a, b) => a + (b.amount || 0), 0);
  const baseLabour = (db.data.bva_actuals || []).filter(a => ['Salaries','Maintenance'].includes(a.category)).reduce((a,b)=>a+b.amount,0);
  const baseMaterial = (db.data.bva_actuals || []).filter(a => ['Nursery','Establishment'].includes(a.category)).reduce((a,b)=>a+b.amount,0);
  const newLabour = baseLabour * (1 + (labourCostChange||0)/100) + (headcountChange||0) * 18000 * 6;
  const newMaterial = baseMaterial * (1 + (materialCostChange||0)/100);
  const fxImpact = (15.75 - (fxRate||15.75)) / 15.75 * baseMaterial * 0.1;
  const newTotal = totalAct + (newLabour - baseLabour) + (newMaterial - baseMaterial) + fxImpact;
  const contingencyAmt = newTotal * ((contingency||5)/100);
  const grandTotal = newTotal + contingencyAmt;
  const budget = (db.data.bva_budgets || []).reduce((a, b) => a + (b.total || 0), 0);
  res.json({ totalBudgetBefore: budget, totalActual: totalAct, grandTotal, variance: grandTotal - budget, variancePercent: budget ? Math.round((grandTotal-budget)/budget*1000)/10 : 0, contingency: contingencyAmt, breakdown: { labour: newLabour, material: newMaterial, fxImpact, contingency: contingencyAmt } });
});

// BVA Alerts
app.get('/api/bva/alerts', auth, (req, res) => {
  let alerts = db.data.bva_alerts || [];
  if (!req.query.all) alerts = alerts.filter(a => !a.dismissed);
  res.json(alerts);
});
app.post('/api/bva/alerts', auth, (req, res) => {
  const entry = { id: `bva-al-${Date.now()}-${Math.random().toString(36).slice(2,6)}`, ...req.body, dismissed: false, createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() };
  db.data.bva_alerts.push(entry); db.save(); res.json(entry);
});
app.put('/api/bva/alerts/:id/dismiss', auth, (req, res) => {
  const a = (db.data.bva_alerts || []).find(a => a.id === req.params.id);
  if (a) { a.dismissed = true; a.updatedAt = new Date().toISOString(); db.save(); }
  res.json(a || { error: 'Not found' });
});

// BVA Notes
app.get('/api/bva/notes', auth, (req, res) => { res.json(db.data.bva_notes || []); });
app.post('/api/bva/notes', auth, (req, res) => {
  const entry = { id: `bva-n-${Date.now()}-${Math.random().toString(36).slice(2,6)}`, ...req.body, createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() };
  db.data.bva_notes.push(entry); db.save(); res.json(entry);
});
app.delete('/api/bva/notes/:id', auth, (req, res) => {
  db.data.bva_notes = (db.data.bva_notes || []).filter(n => n.id !== req.params.id); db.save(); res.json({ success: true });
});

// BVA Export
app.get('/api/bva/export/csv', auth, (req, res) => {
  const year = parseInt(req.query.year) || 2026;
  const budgets = (db.data.bva_budgets || []).filter(b => b.year === year);
  const actuals = (db.data.bva_actuals || []).filter(a => a.year === year);
  let csv = 'Category,' + MONTHS.join(',') + ',Total Budget,Total Actual,Variance,Variance %\n';
  (db.data.bva_categories || []).forEach(c => {
    const bud = budgets.find(b => b.category === c.name);
    const catActuals = actuals.filter(a => a.category === c.name);
    const monthly = MONTHS.map((_, i) => catActuals.filter(a => a.month === i+1).reduce((a,b)=>a+b.amount,0));
    const tB = bud ? bud.total : 0;
    const tA = monthly.reduce((a,b)=>a+b,0);
    const v = tA - tB;
    csv += `"${c.name}",${monthly.join(',')},${tB},${tA},${v},${tB?Math.round(v/Math.abs(tB)*1000)/10:0}%\n`;
  });
  res.header('Content-Type', 'text/csv');
  res.header('Content-Disposition', `attachment; filename="BVA_Export_${year}.csv"`);
  res.send(csv);
});

seedBVA();

// ====== BANKING MODULE ======
function seedBanking() {
  const bTables = ['cheques','cheque_registers','bank_reconciliation'];
  bTables.forEach(t => { if (!db.data[t]) db.data[t] = []; });
  // Seed fixed assets
  if (!db.data.assets || db.data.assets.length === 0) {
    db.data.assets = [
      { id: 'fa1', name: 'CNC Cutting Machine', categoryId: null, purchaseDate: '2023-06-15', cost: 180000, depreciation: 72000, usefulLife: 5, depreciationMethod: 'straight_line', status: 'active', serialNumber: 'CNC-001', location: 'Production Floor A', createdAt: new Date().toISOString() },
      { id: 'fa2', name: 'Delivery Truck - Toyota Hilux', categoryId: null, purchaseDate: '2022-01-10', cost: 95000, depreciation: 57000, usefulLife: 5, depreciationMethod: 'straight_line', status: 'active', serialNumber: 'VH-001', location: 'Parking Bay', createdAt: new Date().toISOString() },
      { id: 'fa3', name: 'Server & Network Equipment', categoryId: null, purchaseDate: '2024-03-01', cost: 35000, depreciation: 14000, usefulLife: 3, depreciationMethod: 'straight_line', status: 'active', serialNumber: 'IT-001', location: 'Server Room', createdAt: new Date().toISOString() },
    ];
  }
  // Seed sample bank accounts
  if (!db.data.bank_accounts || db.data.bank_accounts.length === 0) {
    db.data.bank_accounts = [
      { id: 'ba-1', name: 'FNB Business Account', bank: 'FNB', accountNumber: '62000000001', branchCode: '250655', type: 'current', balance: 250000, glAccountId: 'a1', status: 'active', createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() },
      { id: 'ba-2', name: 'Savings Account', bank: 'Standard Bank', accountNumber: '30000000002', branchCode: '051001', type: 'savings', balance: 150000, glAccountId: 'a1b', status: 'active', createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() }
    ];
    // Sync bank_accounts balance with GL (accounts seeded first, payroll already posted)
    (db.data.bank_accounts || []).forEach(ba => {
      if (ba.glAccountId) {
        const glAcct = (db.data.accounts || []).find(a => a.id === ba.glAccountId);
        if (glAcct) ba.balance = glAcct.balance;
      }
    });
  }
  // Seed sample cheques
  if (!db.data.cheques || db.data.cheques.length === 0) {
    db.data.cheques = [
      { id: 'ch-1', chequeNumber: '1001', bankAccountId: 'ba-1', payee: 'ABC Suppliers', amount: 150000, date: '2026-01-15', status: 'cleared', memo: 'Raw materials payment', createdBy: 'admin', createdAt: '2026-01-15T00:00:00.000Z', updatedAt: '2026-01-20T00:00:00.000Z' },
      { id: 'ch-2', chequeNumber: '1002', bankAccountId: 'ba-1', payee: 'XYZ Transport', amount: 85000, date: '2026-02-10', status: 'cleared', memo: 'Logistics fees', createdBy: 'admin', createdAt: '2026-02-10T00:00:00.000Z', updatedAt: '2026-02-15T00:00:00.000Z' },
      { id: 'ch-3', chequeNumber: '1003', bankAccountId: 'ba-1', payee: 'Municipality', amount: 19262, date: '2026-04-05', status: 'pending', memo: 'Rates and taxes Apr', createdBy: 'admin', createdAt: '2026-04-05T00:00:00.000Z', updatedAt: '2026-04-05T00:00:00.000Z' },
      { id: 'ch-4', chequeNumber: '1004', bankAccountId: 'ba-1', payee: 'Insurance Co', amount: 11016, date: '2026-04-30', status: 'cleared', memo: 'Monthly insurance', createdBy: 'admin', createdAt: '2026-04-30T00:00:00.000Z', updatedAt: '2026-05-05T00:00:00.000Z' },
      { id: 'ch-5', chequeNumber: '1005', bankAccountId: 'ba-1', payee: 'Staff Salaries', amount: 308650, date: '2026-04-28', status: 'cleared', memo: 'April salaries', createdBy: 'admin', createdAt: '2026-04-28T00:00:00.000Z', updatedAt: '2026-05-01T00:00:00.000Z' },
      { id: 'ch-6', chequeNumber: '1006', bankAccountId: 'ba-1', payee: 'Equipment维修', amount: 56143, date: '2026-06-25', status: 'outstanding', memo: 'June repairs', createdBy: 'admin', createdAt: '2026-06-25T00:00:00.000Z', updatedAt: '2026-06-25T00:00:00.000Z' }
    ];
  }
  // Seed bank reconciliation
  if (!db.data.bank_reconciliation || db.data.bank_reconciliation.length === 0) {
    db.data.bank_reconciliation = [
      { id: 'br-1', bankAccountId: 'ba-1', statementDate: '2026-01-31', statementBalance: 23500000, bookBalance: 23485000, difference: 15000, status: 'reconciled', reconciledBy: 'admin', notes: 'Jan reconciliation', createdAt: '2026-02-01T00:00:00.000Z', updatedAt: '2026-02-01T00:00:00.000Z' },
      { id: 'br-2', bankAccountId: 'ba-1', statementDate: '2026-02-28', statementBalance: 23800000, bookBalance: 23785000, difference: 15000, status: 'reconciled', reconciledBy: 'admin', notes: 'Feb reconciliation', createdAt: '2026-03-01T00:00:00.000Z', updatedAt: '2026-03-01T00:00:00.000Z' }
    ];
  }
  db.save();
}

// Cheques CRUD
app.get('/api/banking/cheques', auth, (req, res) => {
  let cheques = db.data.cheques || [];
  if (req.query.status) cheques = cheques.filter(c => c.status === req.query.status);
  if (req.query.bankAccountId) cheques = cheques.filter(c => c.bankAccountId === req.query.bankAccountId);
  res.json(cheques);
});
app.post('/api/banking/cheques', auth, (req, res) => {
  const entry = { id: `ch-${Date.now()}-${Math.random().toString(36).slice(2,6)}`, ...req.body, status: req.body.status || 'pending', createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() };
  db.data.cheques.push(entry); db.save(); res.json(entry);
});
app.put('/api/banking/cheques/:id', auth, (req, res) => {
  const ch = (db.data.cheques || []).find(c => c.id === req.params.id);
  if (ch) { Object.assign(ch, req.body, { updatedAt: new Date().toISOString() }); db.save(); }
  res.json(ch || { error: 'Not found' });
});
app.put('/api/banking/cheques/:id/status', auth, (req, res) => {
  const ch = (db.data.cheques || []).find(c => c.id === req.params.id);
  if (ch) {
    const oldStatus = ch.status;
    ch.status = req.body.status; ch.updatedAt = new Date().toISOString();
    // Auto-post to GL when cheque is cleared
    if (req.body.status === 'cleared' && oldStatus !== 'cleared') {
      const cashAcct = (db.data.accounts || []).find(a => a.subtype === 'bank');
      // Use the cheque's purpose to determine the debit account
      let debitAcct;
      if (ch.purpose === 'expense' || ch.purpose === 'rent' || ch.purpose === 'utilities') {
        debitAcct = (db.data.accounts || []).find(a => a.code === '6100'); // Rent or generic expense
      } else if (ch.purpose === 'loan') {
        debitAcct = (db.data.accounts || []).find(a => a.subtype === 'loan');
      } else {
        debitAcct = (db.data.accounts || []).find(a => a.subtype === 'payable'); // Default: AP
      }
      if (cashAcct && debitAcct) {
        postJournalAuto(ch.date || new Date().toISOString().slice(0,10), `Cheque #${ch.chequeNumber} cleared - ${ch.payee}`, ch.chequeNumber, ch.date?.substring(0,7), [
          { accountId: debitAcct.id, accountCode: debitAcct.code, description: `Cheque payment - ${ch.payee}`, debit: ch.amount || 0, credit: 0 },
          { accountId: cashAcct.id, accountCode: cashAcct.code, description: `Bank payment - Cheque #${ch.chequeNumber}`, debit: 0, credit: ch.amount || 0 }
        ], 'cheque', ch.id, req.user?.username || 'admin');
      }
      // Update bank_accounts balance
      if (ch.bankAccountId) {
        const ba = (db.data.bank_accounts || []).find(a => a.id === ch.bankAccountId);
        if (ba) { ba.balance = (ba.balance || 0) - (ch.amount || 0); }
      }
    }
    // Reverse GL if cheque is voided after being cleared
    if (req.body.status === 'void' && oldStatus === 'cleared') {
      const cashAcct = (db.data.accounts || []).find(a => a.subtype === 'bank');
      const apAcct = (db.data.accounts || []).find(a => a.subtype === 'payable');
      if (cashAcct && apAcct) {
        postJournalAuto(new Date().toISOString().slice(0,10), `Cheque #${ch.chequeNumber} voided - reversal`, `REV-${ch.chequeNumber}`, new Date().toISOString().substring(0,7), [
          { accountId: cashAcct.id, accountCode: cashAcct.code, description: `Reversal - Cheque #${ch.chequeNumber}`, debit: ch.amount || 0, credit: 0 },
          { accountId: apAcct.id, accountCode: apAcct.code, description: `Reversal - ${ch.payee}`, debit: 0, credit: ch.amount || 0 }
        ], 'cheque_reversal', ch.id, req.user?.username || 'admin');
      }
      // Restore bank_accounts balance
      if (ch.bankAccountId) {
        const ba = (db.data.bank_accounts || []).find(a => a.id === ch.bankAccountId);
        if (ba) { ba.balance = (ba.balance || 0) + (ch.amount || 0); }
      }
    }
    db.save();
  }
  res.json(ch || { error: 'Not found' });
});
app.delete('/api/banking/cheques/:id', auth, (req, res) => {
  const ch = (db.data.cheques || []).find(c => c.id === req.params.id);
  if (ch && ch.status === 'cleared') {
    // Reverse GL if deleting a cleared cheque
    const cashAcct = (db.data.accounts || []).find(a => a.subtype === 'bank');
    const apAcct = (db.data.accounts || []).find(a => a.subtype === 'payable');
    if (cashAcct && apAcct) {
      postJournalAuto(new Date().toISOString().slice(0,10), `Cheque #${ch.chequeNumber} deleted - reversal`, `DEL-${ch.chequeNumber}`, new Date().toISOString().substring(0,7), [
        { accountId: cashAcct.id, accountCode: cashAcct.code, description: `Reversal - Cheque #${ch.chequeNumber}`, debit: ch.amount || 0, credit: 0 },
        { accountId: apAcct.id, accountCode: apAcct.code, description: `Reversal - ${ch.payee}`, debit: 0, credit: ch.amount || 0 }
      ], 'cheque_reversal', ch.id, req.user?.username || 'admin');
    }
    if (ch.bankAccountId) {
      const ba = (db.data.bank_accounts || []).find(a => a.id === ch.bankAccountId);
      if (ba) { ba.balance = (ba.balance || 0) + (ch.amount || 0); }
    }
  }
  db.data.cheques = (db.data.cheques || []).filter(c => c.id !== req.params.id); db.save(); res.json({ success: true });
});

// Cheque Register
app.get('/api/banking/cheque-register', auth, (req, res) => {
  const cheques = db.data.cheques || [];
  const accounts = db.data.bank_accounts || [];
  const register = cheques.map(ch => {
    const acct = accounts.find(a => a.id === ch.bankAccountId);
    return { ...ch, bankName: acct?.name || '—', bank: acct?.bank || '—' };
  }).sort((a, b) => (b.date || '').localeCompare(a.date || ''));
  res.json(register);
});
app.get('/api/banking/cheque-register/summary', auth, (req, res) => {
  const cheques = db.data.cheques || [];
  const total = cheques.reduce((a, c) => a + (c.amount || 0), 0);
  const cleared = cheques.filter(c => c.status === 'cleared').reduce((a, c) => a + (c.amount || 0), 0);
  const pending = cheques.filter(c => c.status === 'pending').reduce((a, c) => a + (c.amount || 0), 0);
  const outstanding = cheques.filter(c => c.status === 'outstanding').reduce((a, c) => a + (c.amount || 0), 0);
  const cancelled = cheques.filter(c => c.status === 'cancelled').reduce((a, c) => a + (c.amount || 0), 0);
  res.json({ total, cleared, pending, outstanding, cancelled, count: cheques.length });
});

// Bank Reconciliation
app.get('/api/banking/reconciliation', auth, (req, res) => {
  let recs = db.data.bank_reconciliation || [];
  if (req.query.bankAccountId) recs = recs.filter(r => r.bankAccountId === req.query.bankAccountId);
  res.json(recs);
});
app.post('/api/banking/reconciliation', auth, (req, res) => {
  const entry = { id: `br-${Date.now()}-${Math.random().toString(36).slice(2,6)}`, ...req.body, status: req.body.status || 'pending', createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() };
  db.data.bank_reconciliation.push(entry); db.save(); res.json(entry);
});
app.put('/api/banking/reconciliation/:id', auth, (req, res) => {
  const rec = (db.data.bank_reconciliation || []).find(r => r.id === req.params.id);
  if (rec) { Object.assign(rec, req.body, { updatedAt: new Date().toISOString() }); db.save(); }
  res.json(rec || { error: 'Not found' });
});

// Auto-reconciliation: match bank transactions with GL entries
app.post('/api/banking/reconciliation/auto-match', auth, (req, res) => {
  const { bankAccountId, statementDate, statementBalance } = req.body;
  if (!bankAccountId) return res.status(400).json({ error: 'bankAccountId required' });
  const bankAcct = (db.data.bank_accounts || []).find(a => a.id === bankAccountId);
  if (!bankAcct) return res.status(404).json({ error: 'Bank account not found' });

  const cheques = (db.data.cheques || []).filter(c => c.bankAccountId === bankAccountId && c.status !== 'void');
  const payments = (db.data.payments || []).filter(p => p.bankAccountId === bankAccountId);
  const bankRecs = db.data.bank_reconciliation || [];
  const matchedIds = new Set(bankRecs.map(r => r.chequeId || r.paymentId));

  const matches = [];
  // Match cheques
  cheques.forEach(ch => {
    if (matchedIds.has(ch.id)) return;
    const match = bankRecs.find(r => r.reference === ch.chequeNumber || r.reference === ch.number);
    if (match) {
      match.status = 'matched'; match.matchedAt = new Date().toISOString();
      matches.push({ type: 'cheque', id: ch.id, number: ch.chequeNumber, amount: ch.amount, matchId: match.id });
      matchedIds.add(ch.id);
    }
  });
  // Match payments
  payments.forEach(pmt => {
    if (matchedIds.has(pmt.id)) return;
    const match = bankRecs.find(r => r.reference === pmt.number || Math.abs((r.amount || 0) - (pmt.amount || 0)) < 0.01);
    if (match) {
      match.status = 'matched'; match.matchedAt = new Date().toISOString();
      matches.push({ type: 'payment', id: pmt.id, number: pmt.number, amount: pmt.amount, matchId: match.id });
      matchedIds.add(pmt.id);
    }
  });

  db.save();
  res.json({ matched: matches.length, matches, unmatchedCheques: cheques.filter(c => !matchedIds.has(c.id)).length, unmatchedPayments: payments.filter(p => !matchedIds.has(p.id)).length });
});

// Bank Accounts
app.get('/api/banking/accounts', auth, (req, res) => { res.json(db.data.bank_accounts || []); });
app.post('/api/banking/accounts', auth, (req, res) => {
  const entry = { id: `ba-${Date.now()}-${Math.random().toString(36).slice(2,6)}`, ...req.body, status: 'active', createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() };
  db.data.bank_accounts.push(entry); db.save(); res.json(entry);
});

// Banking Dashboard
app.get('/api/banking/dashboard', auth, (req, res) => {
  const accounts = db.data.bank_accounts || [];
  const cheques = db.data.cheques || [];
  const totalBalance = accounts.reduce((a, acc) => a + (acc.balance || 0), 0);
  const pendingCheques = cheques.filter(c => c.status === 'pending' || c.status === 'outstanding');
  const totalPending = pendingCheques.reduce((a, c) => a + (c.amount || 0), 0);
  const recentCheques = cheques.sort((a, b) => (b.date || '').localeCompare(a.date || '')).slice(0, 10);
  res.json({ accounts, totalBalance, pendingCheques: pendingCheques.length, totalPending, recentCheques });
});

seedBanking();

// ====== CROSS-MODULE GL INTEGRATION ======

// Sales Order → Invoice → AR → GL
app.post('/api/integration/so-to-invoice/:soId', auth, (req, res) => {
  const so = (db.data.sales_orders || []).find(o => o.id === req.params.soId);
  if (!so) return res.status(404).json({ error: 'Sales order not found' });
  if (so.status === 'invoiced') return res.status(400).json({ error: 'Already invoiced' });
  // Get SO lines for three-way matching
  const soLines = (db.data.sales_order_lines || []).filter(l => l.orderId === so.id);
  // Create invoice from SO (SAP: invoice inherits from sales order)
  const invSubtotal = so.subtotal || soLines.reduce((s, l) => s + (l.subtotal || l.qty * l.unitPrice || 0), 0);
  const invTax = Math.round(invSubtotal * 0.15 * 100) / 100;
  const invTotal = invSubtotal + invTax;
  const inv = {
    id: `inv-${Date.now()}-${Math.random().toString(36).slice(2,5)}`,
    number: `INV-${Date.now()}`,
    customerId: so.customerId, customerName: so.customerName,
    orderId: so.id,
    date: req.body.date || new Date().toISOString().slice(0,10),
    dueDate: req.body.dueDate || new Date(Date.now() + 30*86400000).toISOString().slice(0,10),
    subtotal: invSubtotal, tax: invTax, amount: invTotal, paid: 0, balance: invTotal,
    status: 'pending', glPosted: false, createdAt: new Date().toISOString()
  };
  db.data.invoices.push(inv);
  // Create invoice lines from SO lines
  soLines.forEach(l => {
    db.data.invoice_lines = db.data.invoice_lines || [];
    db.data.invoice_lines.push({
      id: `il-${Date.now()}-${Math.random().toString(36).slice(2,5)}`,
      invoiceId: inv.id, productId: l.productId, productName: l.productName,
      sku: l.sku, qty: l.qty, unitPrice: l.unitPrice,
      subtotal: l.subtotal || l.qty * l.unitPrice,
      vatRate: 15, vatAmount: Math.round((l.subtotal || l.qty * l.unitPrice) * 0.15 * 100) / 100,
      total: (l.subtotal || l.qty * l.unitPrice) * 1.15
    });
  });
  // Update SO status
  so.status = 'invoiced'; so.invoiceId = inv.id;
  // SAP GL Posting: Dr AR (1100), Cr Revenue (4000) + Cr Output VAT (2010)
  const arAcct = (db.data.accounts || []).find(a => a.subtype === 'receivable');
  const revAcct = (db.data.accounts || []).find(a => a.code === '4000');
  const outputTaxAcct = (db.data.accounts || []).find(a => a.code === '2010');
  if (arAcct && revAcct && inv.amount > 0) {
    const journalLines = [
      { accountId: arAcct.id, accountCode: arAcct.code, description: `AR - ${so.customerName}`, debit: inv.amount, credit: 0 }
    ];
    if (inv.tax && inv.tax > 0 && outputTaxAcct) {
      journalLines.push({ accountId: revAcct.id, accountCode: revAcct.code, description: `Revenue - ${so.number}`, debit: 0, credit: inv.subtotal });
      journalLines.push({ accountId: outputTaxAcct.id, accountCode: outputTaxAcct.code, description: `Output VAT - ${so.number}`, debit: 0, credit: inv.tax });
    } else {
      journalLines.push({ accountId: revAcct.id, accountCode: revAcct.code, description: `Revenue - ${so.number}`, debit: 0, credit: inv.amount });
    }
    postJournalAuto(inv.date, `Invoice from ${so.number} - ${so.customerName}`, inv.number, inv.date?.substring(0,7), journalLines, 'ar', inv.id, req.user?.username || 'admin');
    inv.glPosted = true;
  }
  // Record output tax in GST return
  if (inv.tax && inv.tax > 0) {
    const gstReturns = db.data.gst_returns || [];
    const currentPeriod = inv.date?.substring(0,7) || new Date().toISOString().substring(0,7);
    let currentReturn = gstReturns.find(r => r.period === currentPeriod && r.status === 'draft');
    if (!currentReturn) {
      currentReturn = { id: `gst-${Date.now()}`, period: currentPeriod, status: 'draft', outputTax: 0, inputTax: 0, netTax: 0, totalSales: 0, totalPurchases: 0, createdAt: new Date().toISOString() };
      gstReturns.push(currentReturn);
    }
    currentReturn.outputTax = (currentReturn.outputTax || 0) + inv.tax;
    currentReturn.totalSales = (currentReturn.totalSales || 0) + inv.subtotal;
    currentReturn.netTax = currentReturn.outputTax - (currentReturn.inputTax || 0);
    db.data.gst_returns = gstReturns;
  }
  db.save();
  eventBus.emitEvent('invoice.created', { module: 'ar', entityId: inv.id, userId: req.user?.id, username: req.user?.username || 'admin', data: { invoiceNumber: inv.number, customerName: so.customerName, amount: inv.amount, soNumber: so.number } });
  res.json({ invoice: inv, so });
});

// Purchase Order → Bill → AP → GL
app.post('/api/integration/po-to-bill/:poId', auth, (req, res) => {
  const po = (db.data.purchase_orders || []).find(o => o.id === req.params.poId);
  if (!po) return res.status(404).json({ error: 'Purchase order not found' });
  if (po.status === 'billed') return res.status(400).json({ error: 'Already billed' });
  // Get PO lines for three-way matching
  const poLines = (db.data.purchase_order_lines || []).filter(l => l.orderId === po.id);
  const billSubtotal = po.subtotal || poLines.reduce((s, l) => s + (l.subtotal || l.qty * l.unitPrice || 0), 0);
  const billTax = Math.round(billSubtotal * 0.15 * 100) / 100;
  const billTotal = billSubtotal + billTax;
  const bill = {
    id: `bill-${Date.now()}-${Math.random().toString(36).slice(2,5)}`,
    number: `BILL-${Date.now()}`,
    supplierId: po.supplierId, supplierName: po.supplierName,
    orderId: po.id,
    date: req.body.date || new Date().toISOString().slice(0,10),
    dueDate: req.body.dueDate || new Date(Date.now() + 30*86400000).toISOString().slice(0,10),
    subtotal: billSubtotal, tax: billTax, amount: billTotal, paid: 0, balance: billTotal,
    status: 'pending', glPosted: false, createdAt: new Date().toISOString()
  };
  db.data.bills.push(bill);
  // Create bill lines from PO lines
  poLines.forEach(l => {
    db.data.bill_lines = db.data.bill_lines || [];
    db.data.bill_lines.push({
      id: `bl-${Date.now()}-${Math.random().toString(36).slice(2,5)}`,
      billId: bill.id, productId: l.productId, productName: l.productName,
      sku: l.sku, qty: l.qty, unitPrice: l.unitPrice,
      subtotal: l.subtotal || l.qty * l.unitPrice,
      vatRate: 15, vatAmount: Math.round((l.subtotal || l.qty * l.unitPrice) * 0.15 * 100) / 100,
      total: (l.subtotal || l.qty * l.unitPrice) * 1.15
    });
  });
  po.status = 'billed'; po.billId = bill.id;
  // SAP GL Posting: Dr Inventory RM (1200), Dr Input VAT (1310), Cr AP (2000)
  const apAcct = (db.data.accounts || []).find(a => a.subtype === 'payable');
  const invAcct = (db.data.accounts || []).find(a => a.code === '1200');
  const inputTaxAcct = (db.data.accounts || []).find(a => a.code === '1310');
  if (apAcct && invAcct && billTotal > 0) {
    const journalLines = [
      { accountId: invAcct.id, accountCode: invAcct.code, description: `Inventory - ${po.supplierName}`, debit: billSubtotal, credit: 0 }
    ];
    if (billTax > 0 && inputTaxAcct) {
      journalLines.push({ accountId: inputTaxAcct.id, accountCode: inputTaxAcct.code, description: `Input VAT - ${po.number}`, debit: billTax, credit: 0 });
    }
    journalLines.push({ accountId: apAcct.id, accountCode: apAcct.code, description: `AP - ${po.number}`, debit: 0, credit: billTotal });
    postJournalAuto(bill.date, `Bill from ${po.number} - ${po.supplierName}`, bill.number, bill.date?.substring(0,7), journalLines, 'ap', bill.id, req.user?.username || 'admin');
    bill.glPosted = true;
  }
  // Record input tax in GST return
  if (billTax > 0) {
    const gstReturns = db.data.gst_returns || [];
    const currentPeriod = bill.date?.substring(0,7) || new Date().toISOString().substring(0,7);
    let currentReturn = gstReturns.find(r => r.period === currentPeriod && r.status === 'draft');
    if (!currentReturn) {
      currentReturn = { id: `gst-${Date.now()}`, period: currentPeriod, status: 'draft', outputTax: 0, inputTax: 0, netTax: 0, totalSales: 0, totalPurchases: 0, createdAt: new Date().toISOString() };
      gstReturns.push(currentReturn);
    }
    currentReturn.inputTax = (currentReturn.inputTax || 0) + billTax;
    currentReturn.totalPurchases = (currentReturn.totalPurchases || 0) + billSubtotal;
    currentReturn.netTax = (currentReturn.outputTax || 0) - currentReturn.inputTax;
    db.data.gst_returns = gstReturns;
  }
  db.save();
  eventBus.emitEvent('bill.created', { module: 'ap', entityId: bill.id, userId: req.user?.id, username: req.user?.username || 'admin', data: { billNumber: bill.number, supplierName: po.supplierName, amount: bill.amount, poNumber: po.number } });
  res.json({ bill, po });
});

// ====== MANUFACTURING ORDER → INVENTORY + GL ======
// MO status update with SAP-style inventory + GL integration
app.patch('/api/manufacturing-orders/:id/status', auth, (req, res) => {
  const mo = (db.data.manufacturing_orders || []).find(m => m.id === req.params.id);
  if (!mo) return res.status(404).json({ error: 'Manufacturing order not found' });
  const { status, laborCost, overheadCost } = req.body;
  const oldStatus = mo.status;
  mo.status = status;

  // On start → post GL: Dr WIP (1400), Cr Materials (1200) + Cr Wages Accrual (2130) + Cr OH Accrual (2140)
  if (oldStatus !== 'in_progress' && status === 'in_progress') {
    const bom = (db.data.bills_of_materials || []).find(b => b.productId === mo.productId);
    const bomLines = (db.data.bom_lines || []).filter(l => l.bomId === (bom ? bom.id : null));
    // Calculate material cost from BOM lines
    const matCost = bomLines.reduce((s, l) => s + (l.qty * l.unitCost * (mo.qty || 1)), 0);
    const laborAmt = laborCost || matCost * 0.35;
    const overheadAmt = overheadCost || matCost * 0.15;
    mo.materialCost = matCost;
    mo.laborCost = laborAmt;
    mo.overheadCost = overheadAmt;
    mo.cost = matCost + laborAmt + overheadAmt;

    const wipAcct = (db.data.accounts || []).find(a => a.code === '1400');
    const matAcct = (db.data.accounts || []).find(a => a.code === '1200');
    const wagesAcct = (db.data.accounts || []).find(a => a.code === '6000');
    const ohAcct = (db.data.accounts || []).find(a => a.code === '6100');
    if (wipAcct && matAcct) {
      const journalLines = [
        { accountId: wipAcct.id, accountCode: wipAcct.code, description: `WIP - ${mo.number}`, debit: mo.cost, credit: 0 },
        { accountId: matAcct.id, accountCode: matAcct.code, description: `Materials - ${mo.number}`, debit: 0, credit: matCost }
      ];
      if (wagesAcct && laborAmt > 0) journalLines.push({ accountId: wagesAcct.id, accountCode: wagesAcct.code, description: `Labor - ${mo.number}`, debit: 0, credit: laborAmt });
      if (ohAcct && overheadAmt > 0) journalLines.push({ accountId: ohAcct.id, accountCode: ohAcct.code, description: `Overhead - ${mo.number}`, debit: 0, credit: overheadAmt });
      postJournalAuto(mo.startDate || new Date().toISOString().slice(0,10), `MO Start - ${mo.number} - ${mo.productName}`, mo.number, mo.startDate?.substring(0,7), journalLines, 'manufacturing', mo.id, req.user?.username || 'admin');
      mo.glStarted = true;
    }
  }

  // On completion → add finished product to inventory + post GL: Dr Finished Goods (1300), Cr WIP (1400)
  if (oldStatus !== 'completed' && status === 'completed') {
    // Add finished goods to inventory
    const products = db.data.products || [];
    const product = products.find(p => p.id === mo.productId || p.name === mo.productName);
    if (product) {
      product.stock = (product.stock || 0) + (mo.qty || 1);
      product.cost = mo.cost ? Math.round(mo.cost / (mo.qty || 1) * 100) / 100 : product.cost;
    } else {
      products.push({
        id: `prod-mo-${Date.now()}`, name: mo.productName, sku: `SKU-MO-${Date.now()}`,
        category: 'Manufactured', unit: 'pcs', stock: mo.qty || 1, minStock: 10,
        cost: mo.cost ? Math.round(mo.cost / (mo.qty || 1) * 100) / 100 : 0,
        price: mo.cost ? Math.round(mo.cost / (mo.qty || 1) * 100) / 100 * 1.5 : 0,
        description: `Manufactured via ${mo.number}`, status: 'active', createdAt: new Date().toISOString()
      });
    }
    // Record stock movement
    db.data.stock_movements = db.data.stock_movements || [];
    db.data.stock_movements.push({
      id: `sm-${Date.now()}-${Math.random().toString(36).slice(2,5)}`,
      productId: mo.productId || `prod-mo-${Date.now()}`, productName: mo.productName,
      type: 'manufacturing', qty: mo.qty || 1, fromLocation: null, toLocation: 'finished-goods',
      reference: mo.number, referenceId: mo.id,
      date: new Date().toISOString().slice(0,10), notes: `MO completion - ${mo.number}`,
      createdAt: new Date().toISOString()
    });
    // GL: Dr Finished Goods, Cr WIP
    const finishedAcct = (db.data.accounts || []).find(a => a.code === '1300');
    const wipAcct = (db.data.accounts || []).find(a => a.code === '1400');
    if (finishedAcct && wipAcct && mo.cost > 0) {
      const journalLines = [
        { accountId: finishedAcct.id, accountCode: finishedAcct.code, description: `Finished Goods - ${mo.number}`, debit: mo.cost, credit: 0 },
        { accountId: wipAcct.id, accountCode: wipAcct.code, description: `WIP Clearing - ${mo.number}`, debit: 0, credit: mo.cost }
      ];
      postJournalAuto(mo.endDate || new Date().toISOString().slice(0,10), `MO Complete - ${mo.number} - ${mo.productName}`, mo.number, (mo.endDate || new Date().toISOString().slice(0,10)).substring(0,7), journalLines, 'manufacturing', mo.id, req.user?.username || 'admin');
      mo.glCompleted = true;
    }
  }

  // On cancellation → reverse any GL posted and remove from stock
  if (oldStatus !== 'cancelled' && status === 'cancelled') {
    if (mo.glCompleted) {
      const finishedAcct = (db.data.accounts || []).find(a => a.code === '1300');
      const wipAcct = (db.data.accounts || []).find(a => a.code === '1400');
      if (finishedAcct && wipAcct && mo.cost > 0) {
        const journalLines = [
          { accountId: wipAcct.id, accountCode: wipAcct.code, description: `MO Cancel WIP - ${mo.number}`, debit: mo.cost, credit: 0 },
          { accountId: finishedAcct.id, accountCode: finishedAcct.code, description: `MO Cancel FG - ${mo.number}`, debit: 0, credit: mo.cost }
        ];
        postJournalAuto(new Date().toISOString().slice(0,10), `MO Cancelled - ${mo.number} - ${mo.productName}`, mo.number, new Date().toISOString().slice(0,7), journalLines, 'manufacturing', mo.id, req.user?.username || 'admin');
      }
      mo.glCompleted = false;
    }
    mo.status = 'cancelled';
  }

  db.save();
  auditLog(req.user.id, req.user.username, 'UPDATE', 'manufacturing_orders', mo.id, mo.number, { status: oldStatus }, { status: mo.status });
  const eventType = status === 'in_progress' ? 'mo.started' : status === 'completed' ? 'mo.completed' : status === 'cancelled' ? 'mo.cancelled' : 'mo.updated';
  eventBus.emitEvent(eventType, { module: 'manufacturing', entityId: mo.id, userId: req.user?.id, username: req.user?.username, data: { moNumber: mo.number, productName: mo.productName, oldStatus, newStatus: status } });
  res.json(mo);
});

// BOM cost rollup endpoint
app.get('/api/bom/:id/cost-rollup', auth, (req, res) => {
  const bom = (db.data.bills_of_materials || []).find(b => b.id === req.params.id);
  if (!bom) return res.status(404).json({ error: 'BOM not found' });
  const bomLines = (db.data.bom_lines || []).filter(l => l.bomId === bom.id);
  const materialCost = bomLines.reduce((s, l) => s + (l.qty * l.unitCost), 0);
  const laborCost = materialCost * 0.35;
  const overheadCost = materialCost * 0.15;
  const totalCost = materialCost + laborCost + overheadCost;
  res.json({
    bomId: bom.id, bomName: bom.name, productId: bom.productId,
    lines: bomLines.map(l => ({ ...l, totalCost: l.qty * l.unitCost })),
    materialCost, laborCost, overheadCost, totalCost,
    costPerUnit: bom.qty ? Math.round(totalCost / bom.qty * 100) / 100 : totalCost
  });
});

// Payroll → GL
app.post('/api/integration/payroll-to-gl', auth, (req, res) => {
  const { employeeId, period, grossSalary, paye, uif, otherDeductions, netPay } = req.body;
  const emp = (db.data.employees || []).find(e => e.id === employeeId);
  if (!emp) return res.status(400).json({ error: 'Employee not found' });
  const wagesAcct = (db.data.accounts || []).find(a => a.code === '6000');
  const payeAcct = (db.data.accounts || []).find(a => a.code === '2110');
  const uifAcct = (db.data.accounts || []).find(a => a.code === '2120');
  const cashAcct = (db.data.accounts || []).find(a => a.subtype === 'bank');
  if (wagesAcct && cashAcct) {
    const lines = [
      { accountId: wagesAcct.id, accountCode: wagesAcct.code, description: `Wages - ${emp.firstName} ${emp.lastName}`, debit: grossSalary || emp.salary, credit: 0 }
    ];
    if (paye > 0 && payeAcct) lines.push({ accountId: payeAcct.id, accountCode: payeAcct.code, description: `PAYE - ${emp.firstName}`, debit: 0, credit: paye });
    if (uif > 0 && uifAcct) lines.push({ accountId: uifAcct.id, accountCode: uifAcct.code, description: `UIF - ${emp.firstName}`, debit: 0, credit: uif });
    const netAmt = netPay || (grossSalary || emp.salary) - (paye || 0) - (uif || 0) - (otherDeductions || 0);
    lines.push({ accountId: cashAcct.id, accountCode: cashAcct.code, description: `Net pay - ${emp.firstName}`, debit: 0, credit: netAmt });
    postJournalAuto(period || new Date().toISOString().slice(0,7) + '-01', `Payroll - ${emp.firstName} ${emp.lastName} (${period})`, `PAY-${Date.now()}`, period, lines, 'payroll', employeeId, req.user?.username || 'admin');
    // Save payroll record
    const pRec = { id: `pay-${Date.now()}`, employeeId, employeeName: `${emp.firstName} ${emp.lastName}`, period, grossSalary: grossSalary || emp.salary, paye: paye || 0, uif: uif || 0, otherDeductions: otherDeductions || 0, netPay: netAmt, status: 'paid', createdAt: new Date().toISOString() };
    if (!db.data.payroll) db.data.payroll = [];
    db.data.payroll.push(pRec);
    db.save();
    eventBus.emitEvent('payroll.processed', { module: 'payroll', entityId: pRec.id, userId: req.user?.id, username: req.user?.username || 'admin', data: { employeeName: pRec.employeeName, period, grossSalary: pRec.grossSalary, netPay: pRec.netPay } });
    res.json(pRec);
  } else {
    res.status(400).json({ error: 'Wages or Cash account not found' });
  }
});

// --- Bulk Payroll → GL for all employees in a period ---
app.post('/api/integration/payroll-bulk-to-gl', auth, (req, res) => {
  const { period } = req.body;
  if (!period) return res.status(400).json({ error: 'Period required (e.g. 2026-07)' });
  const employees = (db.data.employees || []).filter(e => e.status !== 'terminated');
  if (!employees.length) return res.status(400).json({ error: 'No active employees' });
  const wagesAcct = (db.data.accounts || []).find(a => a.code === '6000');
  const payeAcct = (db.data.accounts || []).find(a => a.code === '2110');
  const uifAcct = (db.data.accounts || []).find(a => a.code === '2120');
  const cashAcct = (db.data.accounts || []).find(a => a.subtype === 'bank');
  if (!wagesAcct || !cashAcct) return res.status(400).json({ error: 'Wages or Cash account not found' });
  const results = [];
  let totalGross = 0, totalPaye = 0, totalUif = 0, totalNet = 0;
  employees.forEach(emp => {
    const gross = emp.salary || emp.grossSalary || 0;
    const paye = gross * 0.18; // Simplified PAYE
    const uif = Math.min(gross * 0.01, 177.12); // UIF 1% capped
    const net = gross - paye - uif;
    const lines = [
      { accountId: wagesAcct.id, accountCode: wagesAcct.code, description: `Wages - ${emp.firstName} ${emp.lastName}`, debit: gross, credit: 0 }
    ];
    if (paye > 0 && payeAcct) lines.push({ accountId: payeAcct.id, accountCode: payeAcct.code, description: `PAYE - ${emp.firstName}`, debit: 0, credit: paye });
    if (uif > 0 && uifAcct) lines.push({ accountId: uifAcct.id, accountCode: uifAcct.code, description: `UIF - ${emp.firstName}`, debit: 0, credit: uif });
    lines.push({ accountId: cashAcct.id, accountCode: cashAcct.code, description: `Net pay - ${emp.firstName}`, debit: 0, credit: net });
    postJournalAuto(`${period}-01`, `Payroll - ${emp.firstName} ${emp.lastName} (${period})`, `PAY-${emp.id}-${period}`, period, lines, 'payroll', emp.id, req.user?.username || 'admin');
    const pRec = { id: `pay-${Date.now()}-${emp.id}`, employeeId: emp.id, employeeName: `${emp.firstName} ${emp.lastName}`, period, grossSalary: gross, paye, uif, otherDeductions: 0, netPay: net, status: 'paid', createdAt: new Date().toISOString() };
    if (!db.data.payroll) db.data.payroll = [];
    db.data.payroll.push(pRec);
    totalGross += gross; totalPaye += paye; totalUif += uif; totalNet += net;
    results.push(pRec);
  });
  db.save();
  eventBus.emitEvent('payroll.bulk_processed', { module: 'payroll', userId: req.user?.id, username: req.user?.username || 'admin', data: { period, employeeCount: results.length, totalGross, totalPaye, totalUif, totalNet } });
  res.json({ processed: results.length, totalGross, totalPaye, totalUif, totalNet, records: results });
});

// BVA Actuals → sync from GL
app.get('/api/integration/bva-actuals-from-gl', auth, (req, res) => {
  const year = parseInt(req.query.year) || 2026;
  const entries = (db.data.journal_entries || []).filter(e => e.status === 'posted' && e.date && e.date.startsWith(String(year)));
  const entryIds = new Set(entries.map(e => e.id));
  const lines = (db.data.journal_lines || []).filter(l => entryIds.has(l.entryId));
  const accounts = db.data.accounts || [];
  const monthlyActuals = {};
  for (let m = 1; m <= 12; m++) {
    const mm = String(m).padStart(2, '0');
    const periodKey = `${year}-${mm}`;
    monthlyActuals[periodKey] = { revenue: 0, cogs: 0, operating: 0, financial: 0, total: 0 };
  }
  lines.forEach(l => {
    const acct = accounts.find(a => a.id === l.accountId);
    if (!acct) return;
    const period = entries.find(e => e.id === l.entryId)?.period;
    if (!period || !monthlyActuals[period]) return;
    const amt = (l.debit || 0) - (l.credit || 0);
    if (acct.type === 'income') monthlyActuals[period].revenue += Math.abs(amt);
    else if (acct.subtype === 'cogs') monthlyActuals[period].cogs += Math.abs(amt);
    else if (acct.type === 'expense' && acct.subtype === 'operating') monthlyActuals[period].operating += Math.abs(amt);
    else if (acct.type === 'expense' && acct.subtype === 'financial') monthlyActuals[period].financial += Math.abs(amt);
  });
  Object.keys(monthlyActuals).forEach(k => {
    monthlyActuals[k].total = monthlyActuals[k].revenue - monthlyActuals[k].cogs - monthlyActuals[k].operating - monthlyActuals[k].financial;
  });
  res.json(monthlyActuals);
});

// ====== GST/TAX COMPLIANCE ======
function seedTax() {
  ['gst_returns','gst_transactions','tax_compliance'].forEach(t => { if (!db.data[t]) db.data[t] = []; });
  if (!db.data.tax_rates || db.data.tax_rates.length === 0) {
    db.data.tax_rates = [
      { id: 'tr-1', name: 'Standard Rate', rate: 15, type: 'output', status: 'active', createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() },
      { id: 'tr-2', name: 'Zero Rate', rate: 0, type: 'output', status: 'active', createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() },
      { id: 'tr-3', name: 'Exempt', rate: 0, type: 'exempt', status: 'active', createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() },
      { id: 'tr-4', name: 'Input Tax', rate: 15, type: 'input', status: 'active', createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() }
    ];
  }
  if (!db.data.gst_returns || db.data.gst_returns.length === 0) {
    db.data.gst_returns = [
      { id: 'gst-1', period: '2026-Q1', startDate: '2026-01-01', endDate: '2026-03-31', totalSales: 2500000, totalPurchases: 1800000, outputTax: 375000, inputTax: 270000, netTax: 105000, status: 'filed', filedDate: '2026-04-25', createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() },
      { id: 'gst-2', period: '2026-Q2', startDate: '2026-04-01', endDate: '2026-06-30', totalSales: 2800000, totalPurchases: 2100000, outputTax: 420000, inputTax: 315000, netTax: 105000, status: 'pending', createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() }
    ];
  }
  db.save();
}

// Tax Rates
app.get('/api/tax/rates', auth, (req, res) => { res.json(db.data.tax_rates || []); });
app.post('/api/tax/rates', auth, (req, res) => {
  const entry = { id: `tr-${Date.now()}-${Math.random().toString(36).slice(2,6)}`, ...req.body, status: 'active', createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() };
  db.data.tax_rates.push(entry); db.save(); res.json(entry);
});
app.put('/api/tax/rates/:id', auth, (req, res) => {
  const rate = (db.data.tax_rates || []).find(r => r.id === req.params.id);
  if (rate) { Object.assign(rate, req.body, { updatedAt: new Date().toISOString() }); db.save(); }
  res.json(rate || { error: 'Not found' });
});

// --- PAYE/UIF Settlement Endpoint ---
app.post('/api/accounting/statutory/settle', auth, (req, res) => {
  const { type, amount, date, reference } = req.body;
  if (!type || !amount) return res.status(400).json({ error: 'Type and amount required' });
  const typeMap = {
    paye: { liabilityCode: '2110', label: 'PAYE' },
    uif: { liabilityCode: '2120', label: 'UIF' },
    vat: { liabilityCode: '2020', label: 'VAT' },
    income_tax: { liabilityCode: '2100', label: 'Income Tax' }
  };
  const config = typeMap[type];
  if (!config) return res.status(400).json({ error: 'Invalid type. Use: paye, uif, vat, income_tax' });
  const liabilityAcct = (db.data.accounts || []).find(a => a.code === config.liabilityCode);
  const cashAcct = (db.data.accounts || []).find(a => a.subtype === 'bank');
  if (!liabilityAcct || !cashAcct) return res.status(500).json({ error: 'Account not found' });
  const settleDate = date || new Date().toISOString().slice(0,10);
  const settlePeriod = settleDate.substring(0,7);
  // Dr Liability, Cr Cash
  postJournalAuto(settleDate, `${config.label} settlement - ${reference || ''}`,
    `SETTLE-${type.toUpperCase()}-${Date.now()}`, settlePeriod, [
      { accountId: liabilityAcct.id, description: `${config.label} settlement`, debit: amount, credit: 0 },
      { accountId: cashAcct.id, description: 'Cash payment', debit: 0, credit: amount }
    ], 'statutory', `stat-${Date.now()}`, req.user?.username || 'admin');
  res.json({ success: true, type, amount, date: settleDate, reference });
});

// GST Returns
app.get('/api/tax/gst-returns', auth, (req, res) => {
  let returns = db.data.gst_returns || [];
  if (req.query.status) returns = returns.filter(r => r.status === req.query.status);
  res.json(returns);
});
// Recalculate GST from GL for a given period
app.get('/api/tax/gst-recalculate/:period', auth, (req, res) => {
  const period = req.params.period; // e.g. '2026-07'
  const journalLines = db.data.journal_lines || [];
  const journalEntries = db.data.journal_entries || [];
  // Find all entries for this period
  const periodEntries = journalEntries.filter(e => e.period === period && e.status !== 'void');
  const periodEntryIds = new Set(periodEntries.map(e => e.id));
  // Calculate output tax (credit to 2010 - Output VAT)
  const outputTaxLines = journalLines.filter(l => periodEntryIds.has(l.entryId) && l.accountCode === '2010' && l.credit > 0);
  const outputTax = outputTaxLines.reduce((s, l) => s + (l.credit || 0), 0);
  // Calculate input tax (debit to 1310 - Input VAT)
  const inputTaxLines = journalLines.filter(l => periodEntryIds.has(l.entryId) && l.accountCode === '1310' && l.debit > 0);
  const inputTax = inputTaxLines.reduce((s, l) => s + (l.debit || 0), 0);
  // Find or create GST return for this period
  const gstReturns = db.data.gst_returns || [];
  let ret = gstReturns.find(r => r.period === period);
  if (!ret) {
    ret = { id: `gst-${period}`, period, status: 'draft', createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() };
    gstReturns.push(ret);
  }
  ret.outputTax = outputTax;
  ret.inputTax = inputTax;
  ret.netTax = outputTax - inputTax;
  ret.calculatedAt = new Date().toISOString();
  db.data.gst_returns = gstReturns;
  db.save();
  res.json({ period, outputTax, inputTax, netTax: outputTax - inputTax, entryCount: periodEntries.length, ret });
});
app.post('/api/tax/gst-returns', auth, (req, res) => {
  const entry = { id: `gst-${Date.now()}-${Math.random().toString(36).slice(2,6)}`, ...req.body, status: req.body.status || 'draft', createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() };
  db.data.gst_returns.push(entry); db.save(); res.json(entry);
});
app.put('/api/tax/gst-returns/:id', auth, (req, res) => {
  const ret = (db.data.gst_returns || []).find(r => r.id === req.params.id);
  if (ret) { Object.assign(ret, req.body, { updatedAt: new Date().toISOString() }); db.save(); }
  res.json(ret || { error: 'Not found' });
});

// GST Dashboard
app.get('/api/tax/dashboard', auth, (req, res) => {
  const returns = db.data.gst_returns || [];
  const rates = db.data.tax_rates || [];
  const filed = returns.filter(r => r.status === 'filed');
  const pending = returns.filter(r => r.status === 'pending');
  const totalOutputTax = returns.reduce((a, r) => a + (r.outputTax || 0), 0);
  const totalInputTax = returns.reduce((a, r) => a + (r.inputTax || 0), 0);
  const totalNetTax = returns.reduce((a, r) => a + (r.netTax || 0), 0);
  res.json({ totalReturns: returns.length, filed: filed.length, pending: pending.length, totalOutputTax, totalInputTax, totalNetTax, rates });
});

// Tax Compliance
app.get('/api/tax/compliance', auth, (req, res) => { res.json(db.data.tax_compliance || []); });
app.post('/api/tax/compliance', auth, (req, res) => {
  const entry = { id: `tc-${Date.now()}-${Math.random().toString(36).slice(2,6)}`, ...req.body, createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() };
  db.data.tax_compliance.push(entry); db.save(); res.json(entry);
});

seedTax();

// ====== SECURITY & ACCESS CONTROL ======
function seedSecurity() {
  ['roles','permissions','user_roles','login_history'].forEach(t => { if (!db.data[t]) db.data[t] = []; });
  // Seed default roles
  if (!db.data.roles || db.data.roles.length === 0) {
    db.data.roles = [
      { id: 'role-admin', name: 'Administrator', description: 'Full system access', color: '#ff4060', status: 'active', createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() },
      { id: 'role-manager', name: 'Manager', description: 'Department management access', color: '#00d4ff', status: 'active', createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() },
      { id: 'role-finance', name: 'Finance', description: 'Financial modules access', color: '#00ff88', status: 'active', createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() },
      { id: 'role-hr', name: 'HR', description: 'Human resources access', color: '#a855f7', status: 'active', createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() },
      { id: 'role-viewer', name: 'Viewer', description: 'Read-only access', color: '#6b7280', status: 'active', createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() }
    ];
  }
  // Seed default permissions
  if (!db.data.permissions || db.data.permissions.length === 0) {
    db.data.permissions = [
      { id: 'perm-dashboard', module: 'dashboard', action: 'view', name: 'View Dashboard', status: 'active' },
      { id: 'perm-inventory', module: 'inventory', action: 'manage', name: 'Manage Inventory', status: 'active' },
      { id: 'perm-finance', module: 'finance', action: 'manage', name: 'Manage Finance', status: 'active' },
      { id: 'perm-hr', module: 'hr', action: 'manage', name: 'Manage HR', status: 'active' },
      { id: 'perm-dms', module: 'dms', action: 'manage', name: 'Manage Documents', status: 'active' },
      { id: 'perm-bva', module: 'bva', action: 'view', name: 'View BVA Reports', status: 'active' },
      { id: 'perm-settings', module: 'settings', action: 'manage', name: 'Manage Settings', status: 'active' },
      { id: 'perm-users', module: 'users', action: 'manage', name: 'Manage Users', status: 'active' }
    ];
  }
  // Assign admin role to admin user
  if (!db.data.user_roles || db.data.user_roles.length === 0) {
    const adminUser = (db.data.users || []).find(u => u.username === 'admin');
    if (adminUser) {
      db.data.user_roles.push({ id: `ur-${Date.now()}`, userId: adminUser.id, roleId: 'role-admin', createdAt: new Date().toISOString() });
    }
  }
  db.save();
}

// Roles CRUD
app.get('/api/security/roles', auth, (req, res) => { res.json(db.data.roles || []); });
app.post('/api/security/roles', auth, (req, res) => {
  const entry = { id: `role-${Date.now()}-${Math.random().toString(36).slice(2,6)}`, ...req.body, status: 'active', createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() };
  db.data.roles.push(entry); db.save(); res.json(entry);
});
app.put('/api/security/roles/:id', auth, (req, res) => {
  const role = (db.data.roles || []).find(r => r.id === req.params.id);
  if (role) { Object.assign(role, req.body, { updatedAt: new Date().toISOString() }); db.save(); }
  res.json(role || { error: 'Not found' });
});
app.delete('/api/security/roles/:id', auth, (req, res) => {
  db.data.roles = (db.data.roles || []).filter(r => r.id !== req.params.id);
  db.data.user_roles = (db.data.user_roles || []).filter(ur => ur.roleId !== req.params.id);
  db.save(); res.json({ success: true });
});

// Permissions
app.get('/api/security/permissions', auth, (req, res) => { res.json(db.data.permissions || []); });

// User Roles
app.get('/api/security/user-roles', auth, (req, res) => {
  let urs = db.data.user_roles || [];
  if (req.query.userId) urs = urs.filter(ur => ur.userId === req.query.userId);
  const enriched = urs.map(ur => {
    const user = (db.data.users || []).find(u => u.id === ur.userId);
    const role = (db.data.roles || []).find(r => r.id === ur.roleId);
    return { ...ur, userName: user?.username || '—', roleName: role?.name || '—' };
  });
  res.json(enriched);
});
app.post('/api/security/user-roles', auth, (req, res) => {
  const existing = (db.data.user_roles || []).find(ur => ur.userId === req.body.userId && ur.roleId === req.body.roleId);
  if (existing) return res.json({ error: 'Role already assigned' });
  const entry = { id: `ur-${Date.now()}-${Math.random().toString(36).slice(2,6)}`, ...req.body, createdAt: new Date().toISOString() };
  db.data.user_roles.push(entry); db.save(); res.json(entry);
});
app.delete('/api/security/user-roles/:id', auth, (req, res) => {
  db.data.user_roles = (db.data.user_roles || []).filter(ur => ur.id !== req.params.id); db.save(); res.json({ success: true });
});

// Login History
app.get('/api/security/login-history', auth, (req, res) => {
  let hist = db.data.login_history || [];
  if (req.query.userId) hist = hist.filter(h => h.userId === req.query.userId);
  res.json(hist.slice(-50).reverse());
});

// Security Dashboard
app.get('/api/security/dashboard', auth, (req, res) => {
  const users = db.data.users || [];
  const roles = db.data.roles || [];
  const loginHistory = db.data.login_history || [];
  const recentLogins = loginHistory.slice(-10).reverse();
  const failedLogins = loginHistory.filter(h => h.status === 'failed').length;
  const activeUsers = users.filter(u => u.status === 'active').length;
  res.json({ totalUsers: users.length, activeUsers, totalRoles: roles.length, recentLogins, failedLogins, totalLogins: loginHistory.length });
});

// Enhanced auth with login history tracking
const origLoginHandler = app._router.stack.find(l => l.route && l.route.path === '/api/auth/login');
if (origLoginHandler) {
  const origHandle = origLoginHandler.handle;
  origLoginHandler.handle = function(req, res, next) {
    const origJson = res.json.bind(res);
    res.json = function(data) {
      if (data && data.token) {
        const user = (db.data.users || []).find(u => u.username === req.body?.username);
        if (!db.data.login_history) db.data.login_history = [];
        db.data.login_history.push({
          id: `lh-${Date.now()}-${Math.random().toString(36).slice(2,6)}`,
          userId: user?.id || 'unknown',
          username: req.body?.username || 'unknown',
          ip: req.ip || req.connection?.remoteAddress || 'unknown',
          userAgent: req.headers?.['user-agent']?.substring(0, 100) || 'unknown',
          status: 'success',
          timestamp: new Date().toISOString()
        });
        if (db.data.login_history.length > 200) db.data.login_history = db.data.login_history.slice(-200);
        db.save();
      }
      return origJson(data);
    };
    origHandle(req, res, next);
  };
}

seedSecurity();

// =================== EVENT BUS API ===================
app.get('/api/events', auth, (req, res) => {
  const { module, type, entityId, since, limit } = req.query;
  const events = eventBus.getEvents({ module, type, entityId, since, limit: parseInt(limit) || 50 });
  res.json(events);
});
app.get('/api/events/stats', auth, (req, res) => {
  const events = db.data.events || [];
  const byModule = {};
  const byType = {};
  events.forEach(e => { byModule[e.module] = (byModule[e.module] || 0) + 1; byType[e.type] = (byType[e.type] || 0) + 1; });
  res.json({ total: events.length, byModule, byType, recent: events.slice(-10).reverse() });
});

// =================== WORKFLOW ENGINE API ===================
// Seed default workflows
function seedWorkflows() {
  if (!db.data.workflows) db.data.workflows = [];
  if (!db.data.workflow_instances) db.data.workflow_instances = [];
  if (db.data.workflows.length === 0) {
    db.data.workflows.push(
      { id: 'wf-1', name: 'Sales Order Approval', module: 'sales', trigger: 'so.created', active: true, steps: [
        { stepId: 's1', name: 'Credit Check', type: 'approval', assignee: 'admin', conditions: [{ field: 'amount', op: '>', value: 10000 }] },
        { stepId: 's2', name: 'Manager Approval', type: 'approval', assignee: 'admin', conditions: [{ field: 'amount', op: '>', value: 50000 }] }
      ], createdAt: now(), updatedAt: now() },
      { id: 'wf-2', name: 'Purchase Order Approval', module: 'procurement', trigger: 'po.created', active: true, steps: [
        { stepId: 's1', name: 'Budget Check', type: 'approval', assignee: 'admin', conditions: [{ field: 'amount', op: '>', value: 5000 }] },
        { stepId: 's2', name: 'Finance Approval', type: 'approval', assignee: 'admin', conditions: [{ field: 'amount', op: '>', value: 25000 }] }
      ], createdAt: now(), updatedAt: now() },
      { id: 'wf-3', name: 'Journal Entry Approval', module: 'accounting', trigger: 'journal.posted', active: true, steps: [
        { stepId: 's1', name: 'Senior Accountant Review', type: 'approval', assignee: 'admin', conditions: [{ field: 'totalDebit', op: '>', value: 100000 }] }
      ], createdAt: now(), updatedAt: now() }
    );
  }
}
seedWorkflows();

// List workflow definitions
app.get('/api/workflows', auth, (req, res) => {
  let workflows = db.data.workflows || [];
  if (req.query.module) workflows = workflows.filter(w => w.module === req.query.module);
  res.json(workflows);
});
// Create workflow
app.post('/api/workflows', auth, (req, res) => {
  const wf = { id: genId(), ...req.body, active: req.body.active !== false, createdAt: now(), updatedAt: now() };
  if (!db.data.workflows) db.data.workflows = [];
  db.data.workflows.push(wf);
  db.save();
  eventBus.emitEvent('workflow.created', { module: 'system', userId: req.user?.id, username: req.user?.username, data: { workflowName: wf.name, module: wf.module } });
  res.json(wf);
});
// Update workflow
app.put('/api/workflows/:id', auth, (req, res) => {
  const wf = (db.data.workflows || []).find(w => w.id === req.params.id);
  if (!wf) return res.status(404).json({ error: 'Workflow not found' });
  Object.assign(wf, req.body, { updatedAt: now() });
  db.save(); res.json(wf);
});
// Delete workflow
app.delete('/api/workflows/:id', auth, (req, res) => {
  db.data.workflows = (db.data.workflows || []).filter(w => w.id !== req.params.id);
  db.save(); res.json({ success: true });
});
// Start workflow instance
app.post('/api/workflows/:id/start', auth, (req, res) => {
  const inst = WorkflowEngine.startInstance(req.params.id, req.body.entityId, req.body.module, req.user?.username || 'admin');
  if (!inst) return res.status(400).json({ error: 'Workflow not found or inactive' });
  db.save(); res.json(inst);
});
// Approve current step
app.post('/api/workflows/instances/:id/approve', auth, (req, res) => {
  const inst = WorkflowEngine.approveStep(req.params.id, req.user?.id, req.user?.username || 'admin', req.body.comment);
  if (!inst) return res.status(400).json({ error: 'Instance not found or not pending' });
  db.save(); res.json(inst);
});
// Reject current step
app.post('/api/workflows/instances/:id/reject', auth, (req, res) => {
  const inst = WorkflowEngine.rejectStep(req.params.id, req.user?.id, req.user?.username || 'admin', req.body.comment);
  if (!inst) return res.status(400).json({ error: 'Instance not found or not pending' });
  db.save(); res.json(inst);
});
// Get pending approvals
app.get('/api/workflows/pending', auth, (req, res) => {
  const pending = (db.data.workflow_instances || []).filter(i => i.status === 'pending');
  res.json(pending);
});
// Get all workflow instances
app.get('/api/workflows/instances', auth, (req, res) => {
  let instances = db.data.workflow_instances || [];
  if (req.query.status) instances = instances.filter(i => i.status === req.query.status);
  if (req.query.module) instances = instances.filter(i => i.module === req.query.module);
  instances.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
  res.json(instances);
});
// Get single instance
app.get('/api/workflows/instances/:id', auth, (req, res) => {
  const inst = (db.data.workflow_instances || []).find(i => i.id === req.params.id);
  res.json(inst || { error: 'Not found' });
});

// Start
server.listen(PORT, () => {
  console.log(`Ecoplanet Management ERP running on http://localhost:${PORT}`);
  console.log(`Modules loaded: ${MODULES.length}`);
  console.log(`API: http://localhost:${PORT}/api`);
  console.log(`Health: http://localhost:${PORT}/health`);
});

process.on('SIGTERM', () => { console.log('Shutting down...'); db.save(); server.close(() => process.exit(0)); });