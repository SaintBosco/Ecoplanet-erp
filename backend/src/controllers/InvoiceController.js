const InvoiceService = require('../services/InvoiceService');

class InvoiceController {
  static async createInvoice(req, res) {
    try {
      // Input validation could be added here (e.g. via Zod or Joi)
      const data = req.body;
      const username = req.user ? req.user.username : 'system';

      // Pass to service layer which handles atomicity
      const invoice = await InvoiceService.createInvoice(data, username);
      
      res.status(201).json(invoice);
    } catch (error) {
      console.error('Failed to create invoice:', error);
      res.status(400).json({ error: error.message || 'Failed to create invoice' });
    }
  }

  // Other CRUD operations would go here:
  // static async getInvoices(req, res) { ... }
  // static async getInvoiceById(req, res) { ... }
}

module.exports = InvoiceController;
