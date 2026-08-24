const { PrismaClient } = require('@prisma/client');
const prisma = new PrismaClient();
const AccountingService = require('./AccountingService');

class InvoiceService {
  /**
   * Create an invoice and atomically post the corresponding journal entry
   */
  static async createInvoice(data, username) {
    // We use a Prisma interactive transaction to ensure both the invoice 
    // and its journal entry are created, or neither are.
    return await prisma.$transaction(async (tx) => {
      // 1. Create the Invoice
      const invoice = await tx.invoice.create({
        data: {
          number: `INV-${Date.now()}`,
          customerId: data.customerId,
          customerName: data.customerName,
          orderId: data.orderId,
          date: data.date ? new Date(data.date) : new Date(),
          dueDate: data.dueDate ? new Date(data.dueDate) : null,
          amount: data.amount,
          subtotal: data.subtotal,
          tax: data.tax,
          currency: data.currency || 'ZAR',
          status: 'pending',
          notes: data.notes,
          lines: {
            create: data.lines.map(line => ({
              productName: line.productName,
              productId: line.productId,
              sku: line.sku,
              qty: line.qty,
              unitPrice: line.unitPrice,
              subtotal: line.subtotal,
              vatRate: line.vatRate,
              vatAmount: line.vatAmount,
              total: line.total
            }))
          }
        }
      });

      // 2. Determine Accounts for the Journal Entry
      const arAcct = await tx.account.findFirst({ where: { subtype: 'receivable' } });
      const revAcct = data.revenueAccountId 
        ? await tx.account.findUnique({ where: { id: data.revenueAccountId } })
        : await tx.account.findFirst({ where: { subtype: 'revenue' } });
      const vatAcct = await tx.account.findFirst({ where: { code: '2010' } }); // Output VAT

      if (!arAcct || !revAcct) {
        throw new Error('Required accounting accounts (AR or Revenue) not found.');
      }

      // 3. Prepare Journal Lines
      const journalLines = [
        {
          accountId: arAcct.id,
          accountCode: arAcct.code,
          description: `AR - ${invoice.customerName}`,
          debit: invoice.amount,
          credit: 0
        },
        {
          accountId: revAcct.id,
          accountCode: revAcct.code,
          description: `Revenue - ${invoice.number}`,
          debit: 0,
          credit: invoice.subtotal
        }
      ];

      if (invoice.tax > 0 && vatAcct) {
        journalLines.push({
          accountId: vatAcct.id,
          accountCode: vatAcct.code,
          description: `Output VAT - ${invoice.number}`,
          debit: 0,
          credit: invoice.tax
        });
      }

      // 4. Post the Journal Entry
      // Note: We are passing the transaction object `tx` into a refactored version of postJournalAuto 
      // or we can inline it here if AccountingService doesn't accept a transaction context yet.
      // Since AccountingService uses the global prisma client, we will inline the logic here 
      // to ensure it executes WITHIN the same transaction.

      const entryNum = `JE-${Date.now()}`;
      const entry = await tx.journalEntry.create({
        data: {
          number: entryNum,
          date: invoice.date,
          description: `Invoice ${invoice.number} - ${invoice.customerName}`,
          period: invoice.date.toISOString().substring(0, 7),
          module: 'ar',
          entityId: invoice.id,
          createdBy: username,
          status: 'posted',
        }
      });

      for (const line of journalLines) {
        await tx.journalLine.create({
          data: {
            journalEntryId: entry.id,
            accountId: line.accountId,
            accountCode: line.accountCode,
            description: line.description,
            debit: line.debit,
            credit: line.credit,
          }
        });

        // Update Account Balances
        await tx.account.update({
          where: { id: line.accountId },
          data: {
            balance: {
              increment: line.debit - line.credit
            }
          }
        });
      }

      return invoice;
    });
  }
}

module.exports = InvoiceService;
