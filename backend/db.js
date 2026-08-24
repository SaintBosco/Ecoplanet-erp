const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

class JsonDB {
  constructor(filePath) {
    this.filePath = filePath;
    this.data = this.load();
  }

  load() {
    try {
      if (fs.existsSync(this.filePath)) {
        return JSON.parse(fs.readFileSync(this.filePath, 'utf-8'));
      }
    } catch (e) {
      console.error('DB load error:', e);
    }
    return this.getDefaultSchema();
  }

  getDefaultSchema() {
    return {
      sessions: [],
      users: [],
      roles: [],
      permissions: [],
      user_roles: [],
      login_history: [],
      companies: [],
      products: [],
      categories: [],
      warehouses: [],
      stock_movements: [],
      stock_levels: [],
      suppliers: [],
      customers: [],
      leads: [],
      opportunities: [],
      activities: [],
      quotations: [],
      sales_orders: [],
      sales_order_lines: [],
      purchase_orders: [],
      purchase_order_lines: [],
      invoices: [],
      invoice_lines: [],
      payments: [],
      expenses: [],
      accounts: [],
      journal_entries: [],
      journal_lines: [],
      tax_rates: [],
      gst_returns: [],
      gst_transactions: [],
      tax_compliance: [],
      bills: [],
      bill_lines: [],
      manufacturing_orders: [],
      work_orders: [],
      work_order_operations: [],
      bill_of_materials: [],
      bom_lines: [],
      work_centers: [],
      routings: [],
      routing_operations: [],
      quality_checks: [],
      quality_alerts: [],
      projects: [],
      project_tasks: [],
      project_timesheets: [],
      employees: [],
      departments: [],
      leave_requests: [],
      attendances: [],
      contracts: [],
      payroll: [],
      knowledge_articles: [],
      knowledge_categories: [],
      support_tickets: [],
      automations: [],
      reports: [],
      settings: {},
      dashboard_widgets: [],
      notifications: [],
      attachments: [],
      tags: [],
      currencies: [],
      exchange_rates: [],
      bank_accounts: [],
      bank_statements: [],
      reconciliation: [],
      cheques: [],
      cheque_registers: [],
      bank_reconciliation: [],
      asset_categories: [],
      assets: [],
      asset_depreciation: [],
      fleet_vehicles: [],
      fleet_maintenance: [],
      fleet_fuel: [],
      pos_sessions: [],
      pos_orders: [],
      pos_order_lines: [],
      pos_payments: [],
      // AI/BI
      bi_datasets: [],
      bi_charts: [],
      bi_dashboards: [],
      // CRM extensions
      campaigns: [],
      campaign_members: [],
      email_templates: [],
      // Procurement
      rfqs: [],
      rfq_lines: [],
      vendor_bids: [],
      vendor_bid_lines: [],
      // Quality
      inspection_plans: [],
      inspection_results: [],
      non_conformances: [],
      corrective_actions: [],
      // Maintenance
      maintenance_requests: [],
      maintenance_orders: [],
      equipment: [],
      equipment_categories: [],
      // Fleet
      // Project
      // HR
      // Accounting extensions
      budget_lines: [],
      financial_reports: [],
      // Inventory extensions
      lots: [],
      serial_numbers: [],
      packages: [],
      // Sales extensions
      delivery_orders: [],
      delivery_order_lines: [],
      returns: [],
      return_lines: [],
      // Purchase extensions
      receipts: [],
      receipt_lines: [],
      // Manufacturing extensions
      scrap_orders: [],
      // Knowledge
      article_views: [],
      article_votes: [],
      // Support
      sla_policies: [],
      ticket_comments: [],
      // Automation
      automation_logs: [],
      // Reports
      scheduled_reports: [],
      // Settings
      user_preferences: [],
      company_settings: [],
      number_sequences: [],
      setup_wizard: [],
      onboarding_launchpad: [],
      audit_trail: [],
      // DMS - Document Management
      dms_documents: [],
      dms_categories: [],
      dms_versions: [],
      dms_tags: [],
      dms_links: [],
      dms_workflows: [],
      dms_workflow_steps: [],
      dms_approvals: [],
      dms_stamps: [],
      // BVA Report Tables
      bva_categories: [],
      bva_budgets: [],
      bva_actuals: [],
      bva_cash_flows: [],
      bva_funding_sources: [],
      bva_workforce: [],
      bva_field_ops: [],
      bva_alerts: [],
      bva_scenarios: [],
      bva_forecasts: [],
      bva_notes: [],
      // AIS - Accounting Information System
      fiscal_periods: [],
      account_balances: [],
      manufacturingSettings: {},
      brandKit: {},
      // Attachments
      // Tags
    };
  }

  save() {
    try {
      const dir = path.dirname(this.filePath);
      if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
      }
      fs.writeFileSync(this.filePath, JSON.stringify(this.data, null, 2));
    } catch (e) {
      console.error('DB save error:', e);
    }
  }

  // Generic CRUD
  findAll(table) { if (!this.data[table]) this.data[table] = []; return this.data[table]; }
  findById(table, id) { return (this.data[table] || []).find(r => r.id === id); }
  findOne(table, predicate) { return (this.data[table] || []).find(predicate); }
  findMany(table, predicate) { return (this.data[table] || []).filter(predicate); }

  insert(table, record) {
    if (!this.data[table]) this.data[table] = [];
    const newRecord = { ...record, id: record.id || crypto.randomUUID(), createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() };
    this.data[table].push(newRecord);
    this.save();
    return newRecord;
  }

  update(table, id, updates) {
    const idx = this.data[table].findIndex(r => r.id === id);
    if (idx === -1) return null;
    this.data[table][idx] = { ...this.data[table][idx], ...updates, updatedAt: new Date().toISOString() };
    this.save();
    return this.data[table][idx];
  }

  delete(table, id) {
    const idx = this.data[table].findIndex(r => r.id === id);
    if (idx === -1) return false;
    this.data[table].splice(idx, 1);
    this.save();
    return true;
  }

  // Query helpers
  query(table) {
    const records = this.data[table] || [];
    return {
      filter: (predicate) => records.filter(predicate),
      find: (predicate) => records.find(predicate),
      sort: (fn) => [...records].sort(fn),
      paginate: (page, limit) => records.slice((page-1)*limit, page*limit),
      count: () => records.length,
      all: () => records
    };
  }
}

module.exports = { JsonDB };