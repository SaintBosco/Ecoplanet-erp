const { PrismaClient } = require('@prisma/client');
const { JsonDB } = require('../db');
const path = require('path');

const prisma = new PrismaClient();
const dbPath = path.join(__dirname, '..', 'data', 'app.db');
const oldDb = new JsonDB(dbPath);

async function migrate() {
  console.log('Starting migration to PostgreSQL...');
  try {
    // Migrate Users
    console.log('Migrating Users...');
    const users = oldDb.findAll('users');
    for (const u of users) {
      await prisma.user.upsert({
        where: { id: u.id },
        update: {},
        create: {
          id: u.id,
          username: u.username,
          email: u.email,
          passwordHash: u.passwordHash,
          name: u.name,
          role: u.role,
          department: u.department,
          jobTitle: u.jobTitle,
          phone: u.phone,
          status: u.status,
          createdAt: u.createdAt ? new Date(u.createdAt) : new Date(),
        }
      });
    }

    // Migrate Accounts
    console.log('Migrating Accounts...');
    const accounts = oldDb.findAll('accounts');
    for (const a of accounts) {
      await prisma.account.upsert({
        where: { id: a.id },
        update: {},
        create: {
          id: a.id,
          code: a.code,
          name: a.name,
          type: a.type,
          subtype: a.subtype,
          balance: a.balance,
          status: a.status,
          ifrs_element: a.ifrs_element,
          ifrs_category: a.ifrs_category,
          current_non_current: a.current_non_current,
        }
      });
    }

    // Migrate Customers
    console.log('Migrating Customers...');
    const customers = oldDb.findAll('customers');
    for (const c of customers) {
      await prisma.customer.upsert({
        where: { id: c.id },
        update: {},
        create: {
          id: c.id,
          name: c.name,
          email: c.email,
          phone: c.phone,
          address: c.address,
          contactPerson: c.contactPerson,
          creditLimit: c.creditLimit,
          balance: c.balance,
          type: c.type,
          industry: c.industry,
          status: c.status,
          paymentTerms: c.paymentTerms,
          dunningLevel: c.dunningLevel || 0,
          creditUsed: c.creditUsed || 0,
        }
      });
    }

    // Migrate Suppliers
    console.log('Migrating Suppliers...');
    const suppliers = oldDb.findAll('suppliers');
    for (const s of suppliers) {
      await prisma.supplier.upsert({
        where: { id: s.id },
        update: {},
        create: {
          id: s.id,
          name: s.name,
          email: s.email,
          phone: s.phone,
          address: s.address,
          contactPerson: s.contactPerson,
          leadTime: s.leadTime,
          rating: s.rating,
          status: s.status,
          paymentTerms: s.paymentTerms,
          bankDetails: s.bankDetails,
        }
      });
    }

    console.log('Migration completed successfully!');
  } catch (error) {
    console.error('Error during migration:', error);
  } finally {
    await prisma.$disconnect();
  }
}

migrate();
