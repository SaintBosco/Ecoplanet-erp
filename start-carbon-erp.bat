@echo off
title Carbon ERP Local Server

set BACKEND_DIR=%~dp0resources\app\backend
set ELECTRON_EXE=%~dp0Carbon ERP.exe

echo Starting Carbon ERP Local Server...
echo Backend directory: %BACKEND_DIR%
echo Electron app: %ELECTRON_EXE%

if not exist "%BACKEND_DIR%\server.js" (
    echo ERROR: Backend server.js not found at %BACKEND_DIR%\server.js
    pause
    exit /b 1
)

if not exist "%ELECTRON_EXE%" (
    echo ERROR: Electron executable not found at %ELECTRON_EXE%
    pause
    exit /b 1
)

echo.
echo Starting backend server on http://localhost:3001...
start "Carbon ERP Backend" /B node "%BACKEND_DIR%\server.js"

echo Waiting for backend to start...
timeout /t 3 /nobreak >nul

echo.
echo Starting Carbon ERP application...
start "" "%ELECTRON_EXE%"

echo.
echo Both processes started successfully!
echo Backend: http://localhost:3001
echo Dashboard: http://localhost:3001/dashboard.html
echo.
echo Press any key to stop both processes...
pause >nul

echo Stopping processes...
taskkill /F /IM "node.exe" /FI "WINDOWTITLE eq Carbon ERP Backend*" 2>nul
taskkill /F /IM "Carbon ERP.exe" 2>nul
echo Done.