const { PrismaClient } = require('@prisma/client');
const prisma = new PrismaClient();

class AccountingService {
  /**
   * Post a journal entry with strict transactional guarantees.
   * Ensures that debits strictly equal credits.
   */
  static async postJournalAuto({
    date,
    description,
    reference,
    period,
    lines,
    sourceModule = 'auto',
    sourceId = null,
    username = 'system'
  }) {
    // 1. Validate accounting equation
    const totalDebit = lines.reduce((sum, line) => sum + (line.debit || 0), 0);
    const totalCredit = lines.reduce((sum, line) => sum + (line.credit || 0), 0);

    // Using a tiny epsilon for float comparison
    if (Math.abs(totalDebit - totalCredit) > 0.001) {
      throw new Error(`Journal unbalanced: Debits (${totalDebit}) != Credits (${totalCredit})`);
    }

    // 2. Perform transactional update
    const result = await prisma.$transaction(async (tx) => {
      const entryNum = `JE-${Date.now()}`;
      const entry = await tx.journalEntry.create({
        data: {
          number: entryNum,
          date: date ? new Date(date) : new Date(),
          description,
          period: period || (date ? date.substring(0, 7) : new Date().toISOString().substring(0, 7)),
          module: sourceModule,
          entityId: sourceId,
          createdBy: username,
          status: 'posted',
        }
      });

      for (const line of lines) {
        // Create journal line
        await tx.journalLine.create({
          data: {
            journalEntryId: entry.id,
            accountId: line.accountId,
            accountCode: line.accountCode,
            description: line.description || '',
            debit: line.debit || 0,
            credit: line.credit || 0,
          }
        });

        // Update Account Balance
        // Normal balance rules:
        // Asset/Expense: debit increases balance, credit decreases.
        // Liability/Equity/Income: credit increases balance (stored negatively in old system).
        // For backwards compatibility with the legacy system, we just add debit and subtract credit.
        const acct = await tx.account.findUnique({ where: { id: line.accountId } });
        if (acct) {
          await tx.account.update({
            where: { id: line.accountId },
            data: {
              balance: {
                increment: (line.debit || 0) - (line.credit || 0)
              }
            }
          });
        }
      }

      return entry;
    });

    // 3. Emit Event (assuming an event bus exists)
    // eventBus.emitEvent('journal.posted', { ... })
    
    return result;
  }
}

module.exports = AccountingService;
