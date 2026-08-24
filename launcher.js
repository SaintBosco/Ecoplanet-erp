const { spawn } = require('child_process');
const path = require('path');

const BACKEND_DIR = path.join(__dirname, 'resources', 'app', 'backend');
const ELECTRON_EXE = path.join(__dirname, 'Carbon ERP.exe');

console.log('Starting Carbon ERP Local Server...');
console.log('Backend:', BACKEND_DIR);
console.log('Electron:', ELECTRON_EXE);

const backend = spawn('node', ['server.js'], {
  cwd: BACKEND_DIR,
  stdio: 'inherit',
  env: { ...process.env, PORT: '3001' }
});

backend.on('error', (err) => {
  console.error('Failed to start backend:', err);
  process.exit(1);
});

setTimeout(() => {
  console.log('Starting Electron app...');
  const electron = spawn(ELECTRON_EXE, [], {
    cwd: __dirname,
    stdio: 'inherit',
    detached: true
  });

  electron.on('error', (err) => {
    console.error('Failed to start Electron:', err);
  });

  process.on('SIGINT', () => {
    console.log('\nShutting down...');
    backend.kill();
    electron.kill();
    process.exit(0);
  });

  process.on('SIGTERM', () => {
    console.log('\nShutting down...');
    backend.kill();
    electron.kill();
    process.exit(0);
  });
}, 3000);