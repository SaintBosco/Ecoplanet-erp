#!/usr/bin/env node
/**
 * EcoPlanet Management ERP - Comprehensive Seed Script
 * Reads Excel files and populates the JSON database with real farm data.
 * 
 * Usage: node seed-ecoplanet.js
 * 
 * Data sources:
 *   - CAO - Copy.xlsx: Chart of Accounts (311 accounts)
 *   - SA SR Budget & APO 2026: Sand River farm budget
 *   - SA HT Budget & APO 2026: Hilton Farm budget
 *   - SA VK Budget & APO 2026: Vaalklip farm budget
 *   - SA Spekboom Financial Summary: Actuals YTD June 2026
 *   - Wages Calc SR Jun 2026: Employee/payroll data
 *   - Tax and Travel Calculator 2026-2027: SA tax tables
 */

const fs = require('fs');
const path = require('path');
const XLSX = require('xlsx');

// ============================================================================
// CONFIGURATION
// ============================================================================
const DB_PATH = process.env.DB_PATH || path.join(__dirname, '..', 'data', 'app.db');
const DESKTOP = path.join('C:\\Users\\lunga\\OneDrive\\Desktop\\2026 BVA Report');
const EXCEL_PATHS = {
  coa: path.join(DESKTOP, 'CAO - Copy.xlsx'),
  srBudget: path.join(DESKTOP, 'Budget', 'SA SR Budget & APO 2026 Revised 18062026.xlsx'),
  htBudget: path.join(DESKTOP, 'Budget', 'SA HT Budget & APO 2026 Revised 18062026.xlsx'),
  vkBudget: path.join(DESKTOP, 'Budget', 'SA VK Budget & APO 2026 Revised 18062026.xlsx'),
  actuals: path.join(DESKTOP, 'SA Spekboom Financial Summary - YTD June 2026.xlsx'),
  wages: path.join(DESKTOP, 'Budget', 'Wages Calc SR Jun 2026.xlsm'),
  taxCalc: path.join(DESKTOP, 'Payrol tax calculator', 'Tax and Travel Calculator 2026-2027.xlsx'),
};

const MONTHS = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
const MONTH_FULL = ['January','February','March','April','May','June','July','August','September','October','November','December'];
const NOW = new Date().toISOString();

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================
function readExcel(filePath, sheetName) {
  try {
    if (!fs.existsSync(filePath)) {
      console.warn(`  [SKIP] File not found: ${filePath}`);
      return null;
    }
    const wb = XLSX.readFile(filePath);
    const name = sheetName || wb.SheetNames[0];
    if (!wb.Sheets[name]) {
      console.warn(`  [SKIP] Sheet "${name}" not found in ${path.basename(filePath)}. Available: ${wb.SheetNames.join(', ')}`);
      return null;
    }
    return XLSX.utils.sheet_to_json(wb.Sheets[name], { defval: null });
  } catch (e) {
    console.error(`  [ERROR] Reading ${filePath}: ${e.message}`);
    return null;
  }
}

function readAllSheets(filePath) {
  try {
    if (!fs.existsSync(filePath)) return {};
    const wb = XLSX.readFile(filePath);
    const result = {};
    wb.SheetNames.forEach(name => {
      result[name] = XLSX.utils.sheet_to_json(wb.Sheets[name], { defval: null });
    });
    return result;
  } catch (e) {
    console.error(`  [ERROR] Reading ${filePath}: ${e.message}`);
    return {};
  }
}

function num(v) { return typeof v === 'number' ? v : (parseFloat(v) || 0); }
function str(v) { return v != null ? String(v).trim() : ''; }
function id() { return `eco-${Date.now()}-${Math.random().toString(36).slice(2,8)}`; }

// ============================================================================
// STEP 1: Read Excel Data
// ============================================================================
console.log('=== EcoPlanet Management ERP - Seed Script ===\n');

console.log('[1/8] Reading Chart of Accounts...');
const coaRows = readExcel(EXCEL_PATHS.coa) || [];
console.log(`  Found ${coaRows.length} accounts`);

console.log('[2/8] Reading farm budgets...');
const srBudgetSheets = readAllSheets(EXCEL_PATHS.srBudget);
const htBudgetSheets = readAllSheets(EXCEL_PATHS.htBudget);
const vkBudgetSheets = readAllSheets(EXCEL_PATHS.vkBudget);
console.log(`  SR: ${Object.keys(srBudgetSheets).length} sheets`);
console.log(`  HT: ${Object.keys(htBudgetSheets).length} sheets`);
console.log(`  VK: ${Object.keys(vkBudgetSheets).length} sheets`);

console.log('[3/8] Reading financial actuals...');
const actualsSheets = readAllSheets(EXCEL_PATHS.actuals);
console.log(`  Found ${Object.keys(actualsSheets).length} sheets`);

console.log('[4/8] Reading employee/payroll data...');
const wagesSheets = readAllSheets(EXCEL_PATHS.wages);
console.log(`  Found ${Object.keys(wagesSheets).length} sheets`);

console.log('[5/8] Reading tax tables...');
const taxSheets = readAllSheets(EXCEL_PATHS.taxCalc);
console.log(`  Found ${Object.keys(taxSheets).length} sheets`);

// Map CAO categories to GL account types
function mapCategoryToGLType(category) {
  const cat = str(category).toLowerCase();
  if (cat.includes('administration') || cat.includes('finance') || cat.includes('legal')) return { type: 'expense', subtype: 'operating', ifrs_category: 'operating' };
  if (cat.includes('nursery')) return { type: 'expense', subtype: 'operating', ifrs_category: 'operating' };
  if (cat.includes('establishment')) return { type: 'expense', subtype: 'operating', ifrs_category: 'operating' };
  if (cat.includes('maintenance')) return { type: 'expense', subtype: 'operating', ifrs_category: 'operating' };
  if (cat.includes('capex') || cat.includes('infrastructure')) return { type: 'asset', subtype: 'fixed', ifrs_category: 'investing' };
  if (cat.includes('ehs') || cat.includes('impact')) return { type: 'expense', subtype: 'operating', ifrs_category: 'operating' };
  if (cat.includes('contingency')) return { type: 'expense', subtype: 'operating', ifrs_category: 'operating' };
  if (cat.includes('depreciation')) return { type: 'asset', subtype: 'contra_asset', ifrs_category: 'investing' };
  if (cat.includes('salary') || cat.includes('wage')) return { type: 'expense', subtype: 'operating', ifrs_category: 'operating' };
  return { type: 'expense', subtype: 'operating', ifrs_category: 'operating' };
}

function mapActivityToIFRS(activity) {
  const act = str(activity).toLowerCase();
  if (act.includes('salary') || act.includes('wage') || act.includes('bonus') || act.includes('uif') || act.includes('paye') || act.includes('pension') || act.includes('medical')) return 'ifrs-full_EmployeeBenefitsExpense';
  if (act.includes('depreciation')) return 'ifrs-full_DepreciationAndAmortisationExpense';
  if (act.includes('fuel') || act.includes('oil') || act.includes('diesel')) return 'ifrs-full_RawMaterialsAndConsumablesUsed';
  if (act.includes('fertiliz') || act.includes('chemical') || act.includes('herbicide') || act.includes('pesticide')) return 'ifrs-full_RawMaterialsAndConsumablesUsed';
  if (act.includes('seedling') || act.includes('seed') || act.includes('plant') || act.includes('cutting')) return 'ifrs-full_RawMaterialsAndConsumablesUsed';
  if (act.includes('electricity') || act.includes('water') || act.includes('sewerage')) return 'ifrs-full_OtherOperatingIncomeExpense';
  if (act.includes('repair') || act.includes('maintenance')) return 'ifrs-full_OtherOperatingIncomeExpense';
  if (act.includes('vehicle') || act.includes('tyre') || act.includes('transport')) return 'ifrs-full_OtherOperatingIncomeExpense';
  if (act.includes('insurance')) return 'ifrs-full_OtherOperatingIncomeExpense';
  if (act.includes('security') || act.includes('guard')) return 'ifrs-full_AdministrativeExpense';
  if (act.includes('audit') || act.includes('account') || act.includes('legal') || act.includes('consult')) return 'ifrs-full_AdministrativeExpense';
  if (act.includes('rent') || act.includes('lease')) return 'ifrs-full_OtherOperatingIncomeExpense';
  if (act.includes('telephone') || act.includes('internet') || act.includes('communication')) return 'ifrs-full_AdministrativeExpense';
  if (act.includes('travel') || act.includes('accommodation') || act.includes('per diem')) return 'ifrs-full_AdministrativeExpense';
  if (act.includes('uniform') || act.includes('ppe') || act.includes('safety')) return 'ifrs-full_AdministrativeExpense';
  if (act.includes('nursery') || act.includes('propagat')) return 'ifrs-full_RawMaterialsAndConsumablesUsed';
  if (act.includes('infrastructure') || act.includes('fence') || act.includes('borehole') || act.includes('dam') || act.includes('pipeline')) return 'ifrs-full_PropertyPlantAndEquipment';
  return 'ifrs-full_OtherOperatingIncomeExpense';
}

// ============================================================================
// STEP 2: Process Chart of Accounts
// ============================================================================
console.log('\n[6/8] Processing Chart of Accounts...');

// Core GL accounts that server.js depends on (hardcoded lookups for subtypes and codes)
const coreAccounts = [
  // ASSETS - Current
  { id: 'a1', code: '1000', name: 'Cash - Main Operating', type: 'asset', subtype: 'bank', balance: 250000, status: 'active', ifrs_element: 'ifrs-full_CashAndCashEquivalents', ifrs_category: 'operating', current_non_current: 'current' },
  { id: 'a1b', code: '1010', name: 'Cash - Savings', type: 'asset', subtype: 'bank', balance: 150000, status: 'active', ifrs_element: 'ifrs-full_CashAndCashEquivalents', ifrs_category: 'operating', current_non_current: 'current' },
  { id: 'a1c', code: '1020', name: 'Cash - Carbon Credits', type: 'asset', subtype: 'bank', balance: 75000, status: 'active', ifrs_element: 'ifrs-full_CashAndCashEquivalents', ifrs_category: 'operating', current_non_current: 'current' },
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
  // LIABILITIES - Current
  { id: 'a4', code: '2000', name: 'Accounts Payable', type: 'liability', subtype: 'payable', balance: 0, status: 'active', ifrs_element: 'ifrs-full_TradeAndOtherPayables', ifrs_category: 'operating', current_non_current: 'current' },
  { id: 'a13', code: '2010', name: 'Output VAT', type: 'liability', subtype: 'tax', balance: 0, status: 'active', ifrs_element: 'ifrs-full_TradeAndOtherPayables', ifrs_category: 'operating', current_non_current: 'current' },
  { id: 'a14', code: '2020', name: 'Accrued Expenses', type: 'liability', subtype: 'accrued', balance: 0, status: 'active', ifrs_element: 'ifrs-full_TradeAndOtherPayables', ifrs_category: 'operating', current_non_current: 'current' },
  { id: 'a10', code: '2100', name: 'Income Tax Payable', type: 'liability', subtype: 'tax', balance: 0, status: 'active', ifrs_element: 'ifrs-full_CurrentTaxLiabilitiesCurrent', ifrs_category: 'operating', current_non_current: 'current' },
  { id: 'a15', code: '2110', name: 'PAYE Payable', type: 'liability', subtype: 'tax', balance: 0, status: 'active', ifrs_element: 'ifrs-full_ProvisionsForEmployeeBenefits', ifrs_category: 'operating', current_non_current: 'current' },
  { id: 'a16', code: '2120', name: 'UIF Payable', type: 'liability', subtype: 'tax', balance: 0, status: 'active', ifrs_element: 'ifrs-full_ProvisionsForEmployeeBenefits', ifrs_category: 'operating', current_non_current: 'current' },
  { id: 'a17', code: '2200', name: 'Short-term Loan', type: 'liability', subtype: 'loan', balance: -25000, status: 'active', ifrs_element: 'ifrs-full_OtherFinancialLiabilities', ifrs_category: 'financing', current_non_current: 'current' },
  // LIABILITIES - Non-Current
  { id: 'a18', code: '2500', name: 'Long-term Loan', type: 'liability', subtype: 'loan', balance: -120000, status: 'active', ifrs_element: 'ifrs-full_OtherFinancialLiabilities', ifrs_category: 'financing', current_non_current: 'non-current' },
  // EQUITY
  { id: 'a5', code: '3000', name: 'Share Capital', type: 'equity', subtype: 'capital', balance: -300000, status: 'active', ifrs_element: 'ifrs-full_IssuedCapital', ifrs_category: 'equity', current_non_current: 'equity' },
  { id: 'a19', code: '3100', name: 'Retained Earnings', type: 'equity', subtype: 'retained', balance: -292500, status: 'active', ifrs_element: 'ifrs-full_RetainedEarnings', ifrs_category: 'equity', current_non_current: 'equity' },
  { id: 'a20', code: '3200', name: 'Current Year Earnings', type: 'equity', subtype: 'current', balance: 0, status: 'active', ifrs_element: 'ifrs-full_RetainedEarnings', ifrs_category: 'equity', current_non_current: 'equity' },
  // INCOME
  { id: 'a6', code: '4000', name: 'Sales Revenue', type: 'income', subtype: 'revenue', balance: 0, status: 'active', ifrs_element: 'ifrs-full_Revenue', ifrs_category: 'operating', current_non_current: 'pnl' },
  { id: 'a21', code: '4100', name: 'Service Income', type: 'income', subtype: 'revenue', balance: 0, status: 'active', ifrs_element: 'ifrs-full_Revenue', ifrs_category: 'operating', current_non_current: 'pnl' },
  { id: 'a22', code: '4200', name: 'Interest Income', type: 'income', subtype: 'other', balance: 0, status: 'active', ifrs_element: 'ifrs-full_InterestRevenueCalculatedUsingEffectiveInterestMethodInvesting', ifrs_category: 'investing', current_non_current: 'pnl' },
  { id: 'a23', code: '4300', name: 'Discount Received', type: 'income', subtype: 'other', balance: 0, status: 'active', ifrs_element: 'ifrs-full_OtherOperatingIncomeExpense', ifrs_category: 'operating', current_non_current: 'pnl' },
  { id: 'a23b', code: '4400', name: 'Carbon Credit Revenue', type: 'income', subtype: 'revenue', balance: 0, status: 'active', ifrs_element: 'ifrs-full_Revenue', ifrs_category: 'operating', current_non_current: 'pnl' },
  // EXPENSES - Cost of Sales
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
  // EcoPlanet-specific core accounts
  { id: 'a39', code: '4500', name: 'Carbon Credit Sales', type: 'income', subtype: 'revenue', balance: 0, status: 'active', ifrs_element: 'ifrs-full_Revenue', ifrs_category: 'operating', current_non_current: 'pnl' },
  { id: 'a40', code: '4600', name: 'Nursery Sales', type: 'income', subtype: 'revenue', balance: 0, status: 'active', ifrs_element: 'ifrs-full_Revenue', ifrs_category: 'operating', current_non_current: 'pnl' },
  { id: 'a41', code: '4700', name: 'Biomass Sales', type: 'income', subtype: 'revenue', balance: 0, status: 'active', ifrs_element: 'ifrs-full_Revenue', ifrs_category: 'operating', current_non_current: 'pnl' },
  { id: 'a42', code: '6010', name: 'Nursery Costs', type: 'expense', subtype: 'operating', balance: 0, status: 'active', ifrs_element: 'ifrs-full_RawMaterialsAndConsumablesUsed', ifrs_category: 'operating', current_non_current: 'pnl' },
  { id: 'a43', code: '6020', name: 'Establishment Costs', type: 'expense', subtype: 'operating', balance: 0, status: 'active', ifrs_element: 'ifrs-full_RawMaterialsAndConsumablesUsed', ifrs_category: 'operating', current_non_current: 'pnl' },
  { id: 'a44', code: '6030', name: 'Maintenance Costs', type: 'expense', subtype: 'operating', balance: 0, status: 'active', ifrs_element: 'ifrs-full_OtherOperatingIncomeExpense', ifrs_category: 'operating', current_non_current: 'pnl' },
  { id: 'a45', code: '6040', name: 'Vehicle Costs', type: 'expense', subtype: 'operating', balance: 0, status: 'active', ifrs_element: 'ifrs-full_OtherOperatingIncomeExpense', ifrs_category: 'operating', current_non_current: 'pnl' },
  { id: 'a46', code: '6050', name: 'Infrastructure Costs', type: 'expense', subtype: 'operating', balance: 0, status: 'active', ifrs_element: 'ifrs-full_OtherOperatingIncomeExpense', ifrs_category: 'operating', current_non_current: 'pnl' },
  { id: 'a47', code: '6060', name: 'EHS Costs', type: 'expense', subtype: 'operating', balance: 0, status: 'active', ifrs_element: 'ifrs-full_AdministrativeExpense', ifrs_category: 'operating', current_non_current: 'pnl' },
];

// CAO detail accounts (prefixed with 'cao-' to avoid collision with core GL)
const caoAccounts = coaRows.map((row, i) => {
  const code = str(row['Code'] || row['code'] || '');
  const category = str(row['Category'] || row['category'] || '');
  const subCategory = str(row['SubCategory'] || row['SubCategory'] || '');
  const activity = str(row['Activity'] || row['activity'] || '');
  const description = str(row['Description'] || row['description'] || '');
  const accountName = str(row['Account Name'] || row['Account Name'] || '');
  if (!code || !accountName) return null;
  const glType = mapCategoryToGLType(category);
  return {
    id: `cao-${code}`,
    code: `D-${code}`,
    name: accountName || description || `${category} - ${subCategory}`,
    type: glType.type, subtype: glType.subtype,
    balance: 0, status: 'active',
    ifrs_element: mapActivityToIFRS(activity),
    ifrs_category: glType.ifrs_category,
    current_non_current: 'pnl',
    cao_category: category, cao_subcategory: subCategory, cao_activity: activity,
    entity: category.includes('SR') ? 'Sand River' : category.includes('HT') ? 'Hilton Farm' : category.includes('VK') ? 'Vaalklip' : 'All Farms'
  };
}).filter(Boolean);

console.log(`  Core GL: ${coreAccounts.length} accounts`);
console.log(`  CAO Detail: ${caoAccounts.length} accounts`);

// Merge: core accounts first, then CAO detail
const accounts = [...coreAccounts, ...caoAccounts];

// ============================================================================
// STEP 3: Process Budget Data
// ============================================================================
console.log('\n[7/8] Processing farm budgets...');

function parseBudgetSheet(sheetData, farmName, farmCode) {
  if (!sheetData || sheetData.length === 0) return [];
  
  const budgets = [];
  
  sheetData.forEach(row => {
    const vals = Object.values(row);
    const keys = Object.keys(row);
    
    // Category name is typically in column 1 (index 1), or column 0
    const itemName = str(vals[1] || vals[0]);
    if (!itemName) return;
    
    // Skip summary/total/header rows
    const lower = itemName.toLowerCase();
    if (lower.includes('total') || lower.includes('grand') || lower.includes('summary') || lower.includes('budget') || lower.includes('fx') || lower.includes('classification')) return;
    
    // Extract monthly values starting from column 2 (index 2)
    const monthly = [];
    for (let m = 0; m < 12; m++) {
      let val = num(vals[m + 2]); // +2 because first col is blank, second is category name
      monthly.push(val);
    }
    
    // Also try to get total from column 14 (index 14) if available
    const totalFromFile = num(vals[14]);
    const total = totalFromFile || monthly.reduce((a, v) => a + v, 0);
    if (total === 0 && monthly.every(v => v === 0)) return; // Skip empty rows
    
    budgets.push({
      id: `bud-${farmCode}-${Date.now()}-${Math.random().toString(36).slice(2,6)}`,
      year: 2026,
      farm: farmName,
      farmCode: farmCode,
      category: itemName,
      month1: monthly[0], month2: monthly[1], month3: monthly[2], month4: monthly[3],
      month5: monthly[4], month6: monthly[5], month7: monthly[6], month8: monthly[7],
      month9: monthly[8], month10: monthly[9], month11: monthly[10], month12: monthly[11],
      total: total,
      status: 'active',
      createdAt: NOW, updatedAt: NOW
    });
  });
  
  return budgets;
}

// Process SR budget - use Summary sheet if available
let srBudgets = [];
const srSummary = srBudgetSheets['Summary'] || srBudgetSheets['DETAILED SUMMARY'] || srBudgetSheets[Object.keys(srBudgetSheets)[0]];
srBudgets = parseBudgetSheet(srSummary, 'Sand River', 'SR');
console.log(`  SR: ${srBudgets.length} budget lines`);

// Process HT budget
let htBudgets = [];
const htSummary = htBudgetSheets['Summary'] || htBudgetSheets['DETAILED SUMMARY'] || htBudgetSheets[Object.keys(htBudgetSheets)[0]];
htBudgets = parseBudgetSheet(htSummary, 'Hilton Farm', 'HT');
console.log(`  HT: ${htBudgets.length} budget lines`);

// Process VK budget
let vkBudgets = [];
const vkSummary = vkBudgetSheets['Summary'] || vkBudgetSheets['DETAILED SUMMARY'] || vkBudgetSheets[Object.keys(vkBudgetSheets)[0]];
vkBudgets = parseBudgetSheet(vkSummary, 'Vaalklip', 'VK');
console.log(`  VK: ${vkBudgets.length} budget lines`);

const allBudgets = [...srBudgets, ...htBudgets, ...vkBudgets];

// ============================================================================
// STEP 4: Process Actuals
// ============================================================================
console.log('\n[8/8] Processing financial actuals...');

const actuals = [];
// Look for "Budget vs Actuals" or per-farm actuals sheets
const actualsSheetNames = Object.keys(actualsSheets);
const budgetVsActualsSheet = actualsSheets['Budget vs Actuals'] || actualsSheets['Budget Vs Actuals'] || null;

// Process per-farm sheets
['SR', 'HT', 'VK'].forEach(farmCode => {
  const farmName = farmCode === 'SR' ? 'Sand River' : farmCode === 'HT' ? 'Hilton Farm' : 'Vaalklip';
  const sheetKey = actualsSheetNames.find(k => k.includes(farmCode));
  if (!sheetKey) return;
  
  const data = actualsSheets[sheetKey];
  data.forEach(row => {
    const vals = Object.values(row);
    const itemName = str(vals[0]);
    if (!itemName) return;
    const lower = itemName.toLowerCase();
    if (lower.includes('total') || lower.includes('grand')) return;
    
    // Extract monthly actuals (Jan-Jun 2026)
    for (let m = 0; m < 6; m++) {
      let val = num(vals[m + 1]);
      if (val !== 0) {
        actuals.push({
          id: `act-${farmCode}-${Date.now()}-${Math.random().toString(36).slice(2,6)}`,
          year: 2026, month: m + 1, farm: farmName, farmCode: farmCode,
          category: itemName, amount: val,
          source: 'actuals', status: 'active',
          createdAt: NOW, updatedAt: NOW
        });
      }
    }
  });
});
console.log(`  Actuals: ${actuals.length} records`);

// ============================================================================
// STEP 5: Process Employees from Wages Calc
// ============================================================================
console.log('\n[BONUS] Processing employee data...');

const payrollFileData = wagesSheets['PayrollFile'] || wagesSheets['SystemPayrollDetail'] || [];
const employees = [];
const payrollRecords = [];

if (payrollFileData.length > 0) {
  // Try to extract employee list from PayrollFile
  const empMap = new Map();
  payrollFileData.forEach(row => {
    const vals = Object.values(row);
    // Look for employee number and name fields
    const empNo = str(row['Employee No'] || row['EmpNo'] || row['employee_no'] || vals[0]);
    const empName = str(row['Employee Name'] || row['Name'] || row['name'] || vals[1]);
    if (empNo && empName && !empMap.has(empNo)) {
      empMap.set(empNo, { empNo, empName });
    }
  });
  
  empMap.forEach(({ empNo, empName }, key) => {
    const parts = empName.split(/[\s,]+/);
    const firstName = parts[0] || '';
    const lastName = parts.slice(1).join(' ') || '';
    
    employees.push({
      id: `emp-${empNo}`,
      employeeId: empNo,
      firstName: firstName,
      lastName: lastName,
      email: `${firstName.toLowerCase()}.${lastName.toLowerCase().replace(/\s+/g, '.')}@ecoplanetbamboo.co.za`,
      department: 'Operations',
      jobTitle: 'Farm Worker',
      hireDate: '2024-01-01',
      salary: 0, // Will be filled from payroll data
      status: 'active',
      farm: 'Sand River',
      farmCode: 'SR',
      createdAt: NOW, updatedAt: NOW
    });
  });
}

// Also create EcoPlanet-specific employees if none found
if (employees.length === 0) {
  const ecoEmployees = [
    { id: 'emp-ECO-001', employeeId: 'ECO-001', firstName: 'Pieter', lastName: 'Van Der Merwe', email: 'pieter.vdm@ecoplanetbamboo.co.za', department: 'Management', jobTitle: 'Managing Director', hireDate: '2020-01-01', salary: 1200000, status: 'active', farm: 'All', farmCode: 'ALL' },
    { id: 'emp-ECO-002', employeeId: 'ECO-002', firstName: 'Thandi', lastName: 'Nkosi', email: 'thandi.nkosi@ecoplanetbamboo.co.za', department: 'Finance', jobTitle: 'Financial Manager', hireDate: '2020-06-01', salary: 840000, status: 'active', farm: 'All', farmCode: 'ALL' },
    { id: 'emp-ECO-003', employeeId: 'ECO-003', firstName: 'Johan', lastName: 'Botes', email: 'johan.botes@ecoplanetbamboo.co.za', department: 'Operations', jobTitle: 'Operations Manager', hireDate: '2020-03-01', salary: 780000, status: 'active', farm: 'Sand River', farmCode: 'SR' },
    { id: 'emp-ECO-004', employeeId: 'ECO-004', firstName: 'Sipho', lastName: 'Mkhize', email: 'sipho.mkhize@ecoplanetbamboo.co.za', department: 'Operations', jobTitle: 'Farm Manager - Sand River', hireDate: '2021-01-15', salary: 540000, status: 'active', farm: 'Sand River', farmCode: 'SR' },
    { id: 'emp-ECO-005', employeeId: 'ECO-005', firstName: 'David', lastName: 'Pretorius', email: 'david.pretorius@ecoplanetbamboo.co.za', department: 'Operations', jobTitle: 'Farm Manager - Hilton Farm', hireDate: '2021-03-01', salary: 540000, status: 'active', farm: 'Hilton Farm', farmCode: 'HT' },
    { id: 'emp-ECO-006', employeeId: 'ECO-006', firstName: 'Nomsa', lastName: 'Dlamini', email: 'nomsa.dlamini@ecoplanetbamboo.co.za', department: 'Operations', jobTitle: 'Farm Manager - Vaalklip', hireDate: '2021-06-01', salary: 540000, status: 'active', farm: 'Vaalklip', farmCode: 'VK' },
    { id: 'emp-ECO-007', employeeId: 'ECO-007', firstName: 'Willem', lastName: 'Joubert', email: 'willem.joubert@ecoplanetbamboo.co.za', department: 'Nursery', jobTitle: 'Nursery Manager', hireDate: '2021-09-01', salary: 480000, status: 'active', farm: 'Sand River', farmCode: 'SR' },
    { id: 'emp-ECO-008', employeeId: 'ECO-008', firstName: 'Lerato', lastName: 'Mokoena', email: 'lerato.mokoena@ecoplanetbamboo.co.za', department: 'Finance', jobTitle: 'Accountant', hireDate: '2022-01-01', salary: 420000, status: 'active', farm: 'All', farmCode: 'ALL' },
    { id: 'emp-ECO-009', employeeId: 'ECO-009', firstName: 'Rikus', lastName: 'Steyn', email: 'rikus.steyn@ecoplanetbamboo.co.za', department: 'EHS', jobTitle: 'EHS Officer', hireDate: '2022-04-01', salary: 396000, status: 'active', farm: 'All', farmCode: 'ALL' },
    { id: 'emp-ECO-010', employeeId: 'ECO-010', firstName: 'Amahle', lastName: 'Zulu', email: 'amahle.zulu@ecoplanetbamboo.co.za', department: 'HR', jobTitle: 'HR Officer', hireDate: '2022-07-01', salary: 360000, status: 'active', farm: 'All', farmCode: 'ALL' },
  ];
  ecoEmployees.forEach(e => { e.createdAt = NOW; e.updatedAt = NOW; });
  employees.push(...ecoEmployees);
}

console.log(`  Employees: ${employees.length} records`);

// ============================================================================
// STEP 6: Build Complete Database
// ============================================================================
console.log('\n=== Building Database ===\n');

// Load existing DB schema to preserve structure
const existingDbPath = DB_PATH;
let existingData = {};
try {
  existingData = JSON.parse(fs.readFileSync(existingDbPath, 'utf-8'));
} catch(e) {
  console.log('  No existing DB, starting fresh');
}

// Build the new database
const db = {
  ...existingData,
  
  // Users (keep existing admin)
  users: existingData.users?.length ? existingData.users : [
    { id: 'u1', username: 'admin', passwordHash: '$2a$10$rQEY5zQ5zQ5zQ5zQ5zQ5zOKhJhJhJhJhJhJhJhJhJhJhJhJhJhJ', email: 'admin@ecoplanetbamboo.co.za', firstName: 'Admin', lastName: 'User', role: 'admin', status: 'active', createdAt: NOW, updatedAt: NOW },
    { id: 'u2', username: 'manager', passwordHash: '$2a$10$rQEY5zQ5zQ5zQ5zQ5zQ5zOKhJhJhJhJhJhJhJhJhJhJhJhJhJhJ', email: 'manager@ecoplanetbamboo.co.za', firstName: 'Manager', lastName: 'User', role: 'manager', status: 'active', createdAt: NOW, updatedAt: NOW },
  ],
  
  // Chart of Accounts
  accounts: accounts,
  
  // Fiscal Periods (2026)
  fiscal_periods: MONTH_FULL.map((m, i) => ({
    id: `fp-2026-${String(i + 1).padStart(2, '0')}`,
    name: `${m} 2026`, year: 2026, month: i + 1,
    startDate: `2026-${String(i + 1).padStart(2, '0')}-01`,
    endDate: `2026-${String(i + 1).padStart(2, '0')}-${String(new Date(2026, i + 1, 0).getDate()).padStart(2, '0')}`,
    status: i < 7 ? 'closed' : 'open',
    closedBy: i < 7 ? 'admin' : null,
    closedAt: i < 7 ? NOW : null
  })),
  
  // Employees
  employees: employees,
  
  // Contracts for all employees
  contracts: employees.map(e => ({
    id: `ctr-${e.employeeId}`,
    employeeId: e.id,
    type: 'permanent',
    startDate: e.hireDate || '2020-01-01',
    endDate: null,
    probationEnd: null,
    salary: e.salary || 0,
    status: 'active',
    noticePeriod: '1 month',
    createdAt: NOW, updatedAt: NOW
  })),
  
  // BVA Budgets (all farms)
  bva_budgets: allBudgets,
  
  // BVA Actuals
  bva_actuals: actuals,
  
  // BVA Categories (from COA)
  bva_categories: [
    { id: 'bva-cat-nursery', name: 'Nursery', code: 'NUR', color: '#4ade80', status: 'active', createdAt: NOW, updatedAt: NOW },
    { id: 'bva-cat-establishment', name: 'Establishment', code: 'EST', color: '#22c55e', status: 'active', createdAt: NOW, updatedAt: NOW },
    { id: 'bva-cat-maintenance', name: 'Maintenance', code: 'MAINT', color: '#16a34a', status: 'active', createdAt: NOW, updatedAt: NOW },
    { id: 'bva-cat-salaries', name: 'Salaries & Wages', code: 'SAL', color: '#86efac', status: 'active', createdAt: NOW, updatedAt: NOW },
    { id: 'bva-cat-admin', name: 'Administration', code: 'ADMIN', color: '#fbbf24', status: 'active', createdAt: NOW, updatedAt: NOW },
    { id: 'bva-cat-finance', name: 'Finance & Legal', code: 'FIN', color: '#f87171', status: 'active', createdAt: NOW, updatedAt: NOW },
    { id: 'bva-cat-vehicle', name: 'Vehicle', code: 'VEH', color: '#c084fc', status: 'active', createdAt: NOW, updatedAt: NOW },
    { id: 'bva-cat-infra', name: 'Infrastructure', code: 'INFRA', color: '#38bdf8', status: 'active', createdAt: NOW, updatedAt: NOW },
    { id: 'bva-cat-ehs', name: 'EHS & Impact', code: 'EHS', color: '#fb923c', status: 'active', createdAt: NOW, updatedAt: NOW },
    { id: 'bva-cat-capex', name: 'Capital Expenditure', code: 'CAPEX', color: '#a78bfa', status: 'active', createdAt: NOW, updatedAt: NOW },
  ],
  
  // BVA Funding Sources
  bva_funding_sources: [
    { id: 'bva-fs-1', name: 'EcoPlanet Core Carbon', status: 'active', createdAt: NOW, updatedAt: NOW },
    { id: 'bva-fs-2', name: 'Carbon Credits Revenue', status: 'active', createdAt: NOW, updatedAt: NOW },
    { id: 'bva-fs-3', name: 'Bamboo Product Sales', status: 'active', createdAt: NOW, updatedAt: NOW },
    { id: 'bva-fs-4', name: 'Government Grants', status: 'active', createdAt: NOW, updatedAt: NOW },
  ],
  
  // Cost Centers per farm
  cost_centers: [
    { id: 'cc-sr', code: 'CC-SR', name: 'Sand River Farm', description: 'Sand River bamboo plantation operations', status: 'active' },
    { id: 'cc-ht', code: 'CC-HT', name: 'Hilton Farm', description: 'Hilton Farm bamboo plantation operations', status: 'active' },
    { id: 'cc-vk', code: 'CC-VK', name: 'Vaalklip Farm', description: 'Vaalklip bamboo plantation operations', status: 'active' },
    { id: 'cc-ho', code: 'CC-HO', name: 'Head Office', description: 'Corporate head office and administration', status: 'active' },
    { id: 'cc-nur', code: 'CC-NUR', name: 'Nursery Operations', description: 'Plant nursery and propagation', status: 'active' },
  ],
  
  // Profit Centers
  profit_centers: [
    { id: 'pc-bam', code: 'PC-BAM', name: 'Bamboo Production', description: 'Bamboo cultivation and harvesting', status: 'active' },
    { id: 'pc-ind', code: 'PC-IND', name: 'Indigenous Trees', description: 'Indigenous tree cultivation', status: 'active' },
    { id: 'pc-spk', code: 'PC-SPK', name: 'Spekboom', description: 'Spekboom cultivation for carbon credits', status: 'active' },
    { id: 'pc-ccr', code: 'PC-CCR', name: 'Carbon Credits', description: 'Carbon credit registration and sales', status: 'active' },
  ],
  
  // Products (EcoPlanet bamboo/tree products)
  products: [
    { id: 'p1', name: 'Bamboo Seedlings - Bambusa Balcooa', sku: 'BAM-BAL-001', price: 45, cost: 25, stock: 50000, minStock: 10000, category: 'Nursery', unit: 'each', status: 'active', description: 'Bamboo seedlings for commercial planting', createdAt: NOW, updatedAt: NOW },
    { id: 'p2', name: 'Bamboo Seedlings - Dendrocalamus Asper', sku: 'BAM-ASP-002', price: 55, cost: 30, stock: 35000, minStock: 8000, category: 'Nursery', unit: 'each', status: 'active', description: 'Giant bamboo seedlings', createdAt: NOW, updatedAt: NOW },
    { id: 'p3', name: 'Indigenous Tree Seedlings - Spekboom', sku: 'IND-SPK-001', price: 35, cost: 18, stock: 80000, minStock: 15000, category: 'Nursery', unit: 'each', status: 'active', description: 'Spekboom for carbon sequestration', createdAt: NOW, updatedAt: NOW },
    { id: 'p4', name: 'Carbon Credits - Verified', sku: 'CCR-VCS-001', price: 2500, cost: 800, stock: 5000, minStock: 1000, category: 'Carbon Credits', unit: 'tonne CO2e', status: 'active', description: 'Verified carbon credits per tonne CO2e', createdAt: NOW, updatedAt: NOW },
    { id: 'p5', name: 'Bamboo Culms - Mature', sku: 'BAM-CUL-001', price: 120, cost: 40, stock: 2000, minStock: 500, category: 'Harvest', unit: 'each', status: 'active', description: 'Mature bamboo culms for processing', createdAt: NOW, updatedAt: NOW },
    { id: 'p6', name: 'Biomass Chips', sku: 'BAM-BIO-001', price: 800, cost: 350, stock: 500, minStock: 100, category: 'Harvest', unit: 'tonne', status: 'active', description: 'Bamboo biomass chips', createdAt: NOW, updatedAt: NOW },
    { id: 'p7', name: 'Indigenous Seedlings - Mixed', sku: 'IND-MIX-001', price: 40, cost: 20, stock: 25000, minStock: 5000, category: 'Nursery', unit: 'each', status: 'active', description: 'Mixed indigenous tree seedlings', createdAt: NOW, updatedAt: NOW },
    { id: 'p8', name: 'Fertilizer - Organic', sku: 'MNT-FRT-001', price: 350, cost: 200, stock: 200, minStock: 50, category: 'Inputs', unit: 'bag (50kg)', status: 'active', description: 'Organic fertilizer for plantation', createdAt: NOW, updatedAt: NOW },
  ],
  
  // Customers (carbon credit buyers, timber merchants)
  customers: [
    { id: 'c1', name: 'South Pole Carbon', contactPerson: 'Janet Hughes', email: 'janet@southpole.com', phone: '+27-21-1001', city: 'Cape Town', country: 'South Africa', taxId: 'ZA4120265890', paymentTerms: 'Net 45', creditLimit: 5000000, status: 'active', createdAt: NOW, updatedAt: NOW },
    { id: 'c2', name: 'Natural Carbon Co', contactPerson: 'Michael O\'Brien', email: 'michael@naturalcarbon.co', phone: '+27-11-2002', city: 'Johannesburg', country: 'South Africa', taxId: 'ZA4120267891', paymentTerms: 'Net 30', creditLimit: 3000000, status: 'active', createdAt: NOW, updatedAt: NOW },
    { id: 'c3', name: 'Vera (VCS)', contactPerson: 'Registry Team', email: 'registry@vera.org', phone: '+1-202-3003', city: 'Washington DC', country: 'USA', taxId: '', paymentTerms: 'Net 60', creditLimit: 10000000, status: 'active', createdAt: NOW, updatedAt: NOW },
    { id: 'c4', name: 'Timber Solutions SA', contactPerson: 'David van Niekerk', email: 'david@timbersolutions.co.za', phone: '+27-33-4004', city: 'Pietermaritzburg', country: 'South Africa', taxId: 'ZA4120269892', paymentTerms: 'Net 30', creditLimit: 2000000, status: 'active', createdAt: NOW, updatedAt: NOW },
    { id: 'c5', name: 'Green Building Materials', contactPerson: 'Nthabiseng Moloi', email: 'nthabiseng@greenbuilding.co.za', phone: '+27-12-5005', city: 'Pretoria', country: 'South Africa', taxId: 'ZA4120271893', paymentTerms: 'Net 30', creditLimit: 1500000, status: 'active', createdAt: NOW, updatedAt: NOW },
  ],
  
  // Suppliers
  suppliers: [
    { id: 's1', name: 'Nulandis (Pty) Ltd', contactPerson: 'Hennie', email: 'orders@nulandis.co.za', phone: '+27-21-9001', city: 'Cape Town', country: 'South Africa', paymentTerms: 'Net 30', status: 'active', category: 'Agricultural Inputs', createdAt: NOW, updatedAt: NOW },
    { id: 's2', name: 'Omnia Fertilizer', contactPerson: 'Sales Team', email: 'sales@omnia.co.za', phone: '+27-11-7002', city: 'Johannesburg', country: 'South Africa', paymentTerms: 'Net 30', status: 'active', category: 'Fertilizer', createdAt: NOW, updatedAt: NOW },
    { id: 's3', name: 'AGRI Beef & Animal Feed', contactPerson: 'Orders', email: 'orders@agribeef.co.za', phone: '+27-51-8003', city: 'Bloemfontein', country: 'South Africa', paymentTerms: 'Net 30', status: 'active', category: 'Animal Feed', createdAt: NOW, updatedAt: NOW },
    { id: 's4', name: 'Bell Equipment', contactPerson: 'Sales', email: 'sales@bell.co.za', phone: '+27-31-9004', city: 'Richards Bay', country: 'South Africa', paymentTerms: 'Net 60', status: 'active', category: 'Equipment', createdAt: NOW, updatedAt: NOW },
    { id: 's5', name: 'Vexor Chemicals', contactPerson: 'Orders', email: 'orders@vexor.co.za', phone: '+27-11-6005', city: 'Johannesburg', country: 'South Africa', paymentTerms: 'Net 30', status: 'active', category: 'Chemicals', createdAt: NOW, updatedAt: NOW },
  ],
  
  // BVA Workforce (sample data from SR actuals)
  bva_workforce: [
    { id: 'bva-wf-1', date: '2026-01-10', scannedIn: 60, fertSpray: 39, teamLeaders: 3, drivers: 1, workshop: 6, infra: 5, grounds: 1, impact: 2, other: 17, totalMandays: 60, mandaysBudget: 70, mandaysBalance: 10, rainDay: false, rainDayCount: 0, status: 'active', createdAt: NOW, updatedAt: NOW },
    { id: 'bva-wf-2', date: '2026-02-10', scannedIn: 70, fertSpray: 48, teamLeaders: 3, drivers: 1, workshop: 6, infra: 5, grounds: 1, impact: 2, other: 6, totalMandays: 70, mandaysBudget: 70, mandaysBalance: 0, rainDay: true, rainDayCount: 1, status: 'active', createdAt: NOW, updatedAt: NOW },
    { id: 'bva-wf-3', date: '2026-03-10', scannedIn: 70, fertSpray: 49, teamLeaders: 1, drivers: 1, workshop: 5, infra: 5, grounds: 1, impact: 2, other: 5, totalMandays: 70, mandaysBudget: 70, mandaysBalance: 0, rainDay: false, rainDayCount: 0, status: 'active', createdAt: NOW, updatedAt: NOW },
    { id: 'bva-wf-4', date: '2026-04-10', scannedIn: 70, fertSpray: 48, teamLeaders: 5, drivers: 1, workshop: 6, infra: 5, grounds: 1, impact: 1, other: 5, totalMandays: 70, mandaysBudget: 70, mandaysBalance: 0, rainDay: false, rainDayCount: 0, status: 'active', createdAt: NOW, updatedAt: NOW },
    { id: 'bva-wf-5', date: '2026-05-10', scannedIn: 73, fertSpray: 51, teamLeaders: 0, drivers: 1, workshop: 6, infra: 5, grounds: 1, impact: 3, other: 4, totalMandays: 73, mandaysBudget: 70, mandaysBalance: -3, rainDay: false, rainDayCount: 0, status: 'active', createdAt: NOW, updatedAt: NOW },
    { id: 'bva-wf-6', date: '2026-06-10', scannedIn: 68, fertSpray: 47, teamLeaders: 0, drivers: 1, workshop: 6, infra: 5, grounds: 1, impact: 2, other: 5, totalMandays: 68, mandaysBudget: 70, mandaysBalance: 2, rainDay: false, rainDayCount: 0, status: 'active', createdAt: NOW, updatedAt: NOW },
  ],
  
  // BVA Field Operations
  bva_field_ops: [
    { id: 'bva-fo-1', date: '2026-01-10', hectaresFertilized: 28.33, aminoKUsage: 104.85, maintenanceFertilizingHa: 25, nurseryMaintenanceHa: 3.33, establishmentHa: 0, apoTarget: 16.5, status: 'active', createdAt: NOW, updatedAt: NOW },
    { id: 'bva-fo-2', date: '2026-02-10', hectaresFertilized: 51.16, aminoKUsage: 144, maintenanceFertilizingHa: 45, nurseryMaintenanceHa: 6.16, establishmentHa: 0, apoTarget: 16.5, status: 'active', createdAt: NOW, updatedAt: NOW },
    { id: 'bva-fo-3', date: '2026-03-10', hectaresFertilized: 41.63, aminoKUsage: 90, maintenanceFertilizingHa: 35, nurseryMaintenanceHa: 6.63, establishmentHa: 0, apoTarget: 16.5, status: 'active', createdAt: NOW, updatedAt: NOW },
    { id: 'bva-fo-4', date: '2026-04-10', hectaresFertilized: 45.94, aminoKUsage: 180, maintenanceFertilizingHa: 40, nurseryMaintenanceHa: 5.94, establishmentHa: 0, apoTarget: 16.5, status: 'active', createdAt: NOW, updatedAt: NOW },
    { id: 'bva-fo-5', date: '2026-05-10', hectaresFertilized: 49.89, aminoKUsage: 213, maintenanceFertilizingHa: 44, nurseryMaintenanceHa: 5.89, establishmentHa: 0, apoTarget: 16.5, status: 'active', createdAt: NOW, updatedAt: NOW },
    { id: 'bva-fo-6', date: '2026-06-10', hectaresFertilized: 47.73, aminoKUsage: 230, maintenanceFertilizingHa: 42, nurseryMaintenanceHa: 5.73, establishmentHa: 0, apoTarget: 16.5, status: 'active', createdAt: NOW, updatedAt: NOW },
  ],
  
  // Tax Rates (SA 15% VAT)
  tax_rates: [
    { id: 'tr-1', name: 'Standard VAT', rate: 15, type: 'output', status: 'active', createdAt: NOW, updatedAt: NOW },
    { id: 'tr-2', name: 'Zero-rated', rate: 0, type: 'output', status: 'active', createdAt: NOW, updatedAt: NOW },
    { id: 'tr-3', name: 'Exempt', rate: 0, type: 'output', status: 'active', createdAt: NOW, updatedAt: NOW },
    { id: 'tr-4', name: 'Input VAT', rate: 15, type: 'input', status: 'active', createdAt: NOW, updatedAt: NOW },
  ],
  
  // Bank Accounts
  bank_accounts: [
    { id: 'ba-1', name: 'FNB - Main Operating', bank: 'First National Bank', accountNumber: '62001234567', branchCode: '250-655', glAccountId: 'acoa-1010', balance: 0, status: 'active', createdAt: NOW, updatedAt: NOW },
    { id: 'ba-2', name: 'ABSA - Carbon Credits', bank: 'ABSA Bank', accountNumber: '4012345678', branchCode: '632-005', glAccountId: 'acoa-1020', balance: 0, status: 'active', createdAt: NOW, updatedAt: NOW },
  ],
  
  // Settings
  settings: {
    ...existingData.settings,
    companyName: 'Ecoplanet Management ERP',
    tradingName: 'EcoPlanet Bamboo',
    currency: 'ZAR',
    taxRate: 15,
    vatNumber: 'ZA4120261234',
    registrationNumber: '2020/123456/07',
    address: 'Sand River Farm, Eastern Cape, South Africa',
    phone: '+27-43-0001',
    email: 'info@ecoplanetbamboo.co.za',
    website: 'ecoplanetbamboo.co.za',
  },
  
  // Brands
  brandKit: {
    primaryColor: '#22c55e',
    secondaryColor: '#16a34a',
    accentColor: '#86efac',
    darkColor: '#15803d',
    logo: null,
    companyName: 'EcoPlanet Bamboo',
  },
  
  // Overwrite timestamp
  _lastSeed: NOW,
  _seedVersion: '2.0-ecoplanet',
};

// ============================================================================
// STEP 7: Write Database
// ============================================================================
console.log('\n=== Writing Database ===\n');

// Backup existing DB
if (fs.existsSync(DB_PATH)) {
  const backupPath = DB_PATH + `.backup-${Date.now()}`;
  fs.copyFileSync(DB_PATH, backupPath);
  console.log(`  Backed up existing DB to: ${path.basename(backupPath)}`);
}

// Ensure data directory exists
const dataDir = path.dirname(DB_PATH);
if (!fs.existsSync(dataDir)) {
  fs.mkdirSync(dataDir, { recursive: true });
}

fs.writeFileSync(DB_PATH, JSON.stringify(db, null, 2));
console.log(`  Database written to: ${DB_PATH}`);
console.log(`  Size: ${(fs.statSync(DB_PATH).size / 1024).toFixed(1)} KB`);

// ============================================================================
// SUMMARY
// ============================================================================
console.log('\n=== Seed Complete ===');
console.log(`  Accounts: ${accounts.length}`);
console.log(`  Budgets: ${allBudgets.length} (SR: ${srBudgets.length}, HT: ${htBudgets.length}, VK: ${vkBudgets.length})`);
console.log(`  Actuals: ${actuals.length}`);
console.log(`  Employees: ${employees.length}`);
console.log(`  Products: ${db.products.length}`);
console.log(`  Customers: ${db.customers.length}`);
console.log(`  Suppliers: ${db.suppliers.length}`);
console.log(`  Fiscal Periods: ${db.fiscal_periods.length}`);
console.log(`  Cost Centers: ${db.cost_centers.length}`);
console.log(`  Profit Centers: ${db.profit_centers.length}`);
console.log('\n  Restart the backend server to apply changes.');
console.log('  Default login: admin / admin123');
