const express = require('express');
const router = express.Router();
const InvoiceController = require('../controllers/InvoiceController');
const { auth } = require('../middleware/auth');

// Apply auth middleware to all routes in this router
router.use(auth);

// Invoice Routes
router.post('/', InvoiceController.createInvoice);
// router.get('/', InvoiceController.getInvoices);
// router.get('/:id', InvoiceController.getInvoiceById);

module.exports = router;
