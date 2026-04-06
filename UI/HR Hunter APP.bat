@echo off
setlocal

set "ROOT_DIR=%~dp0"
for %%I in ("%ROOT_DIR%..") do set "WORKSPACE=%%~fI"
set "PORT=8765"
set "APP_URL=http://127.0.0.1:%PORT%/"
set "HEALTH_URL=http://127.0.0.1:%PORT%/healthz"

if not exist "%WORKSPACE%\.venv\Scripts\hr-hunter.exe" (
  echo HR Hunter app launcher could not find the cloned workspace venv.
  echo Expected: "%WORKSPACE%\.venv\Scripts\hr-hunter.exe"
  pause
  exit /b 1
)

powershell -NoProfile -Command "try { Invoke-WebRequest -UseBasicParsing '%HEALTH_URL%' -TimeoutSec 2 | Out-Null; exit 0 } catch { exit 1 }"
if errorlevel 1 (
  start "HR Hunter Backend" powershell -NoExit -ExecutionPolicy Bypass -Command "Set-Location '%WORKSPACE%'; & '.\.venv\Scripts\hr-hunter.exe' serve --host 127.0.0.1 --port %PORT%"
)

set "READY="
for /L %%I in (1,1,30) do (
  powershell -NoProfile -Command "try { Invoke-WebRequest -UseBasicParsing '%HEALTH_URL%' -TimeoutSec 2 | Out-Null; exit 0 } catch { exit 1 }"
  if not errorlevel 1 (
    set "READY=1"
    goto :open_app
  )
  timeout /t 1 /nobreak >nul
)

:open_app
if defined READY (
  start "" "%APP_URL%"
  exit /b 0
)

echo HR Hunter backend did not start in time.
echo Open the backend window and check for dependency or port errors.
pause
exit /b 1
