@echo off
REM start_demo.cmd — Start the Moniker service and launch Jupyter
REM
REM Usage:
REM   start_demo.cmd               starts server + jupyter
REM   start_demo.cmd --no-jupyter  starts server only
REM
setlocal enabledelayedexpansion

set "SVC_DIR=%USERPROFILE%\open-moniker-svc"
set "NOTEBOOK_DIR=%~dp0"
set "CLIENT_DIR=%USERPROFILE%\open-moniker-client"
set "PORT=8050"
set "SERVER_PID="
set "NO_JUPYTER=0"

if "%~1"=="--no-jupyter" set "NO_JUPYTER=1"

REM Start the Moniker service in the background
echo Starting Moniker service on port %PORT%...
set "PYTHONPATH=%SVC_DIR%\src;%SVC_DIR%\external\moniker-data\src"
start /B "moniker-svc" python -m uvicorn moniker_svc.main:app --host 0.0.0.0 --port %PORT% > "%TEMP%\moniker-svc.log" 2>&1

REM Wait for the service to become healthy (up to 30 seconds)
echo Waiting for /health...
set "HEALTHY=0"
for /L %%i in (1,1,30) do (
    if !HEALTHY!==0 (
        curl -sf "http://localhost:%PORT%/health" >nul 2>&1
        if !errorlevel!==0 (
            echo Service is healthy.
            set "HEALTHY=1"
        ) else (
            timeout /t 1 /nobreak >nul 2>&1
        )
    )
)

if %HEALTHY%==0 (
    echo ERROR: Service did not become healthy within 30 seconds.
    echo Check %TEMP%\moniker-svc.log for details.
    exit /b 1
)

REM Launch Jupyter or wait
if %NO_JUPYTER%==1 (
    echo Server running on http://localhost:%PORT%
    echo Press Ctrl+C to stop.
    echo.
    echo To stop the server later, run:
    echo   for /f "tokens=5" %%%%a in ^('netstat -ano ^| findstr ":%PORT% " ^| findstr "LISTENING"'^) do taskkill /F /PID %%%%a
    pause >nul
) else (
    echo Launching Jupyter...
    set "PYTHONPATH=%SVC_DIR%\src;%SVC_DIR%\external\moniker-data\src;%CLIENT_DIR%"
    cd /d "%NOTEBOOK_DIR%"
    python -m jupyter notebook showcase.ipynb
)

REM Cleanup: kill the server when Jupyter exits
echo.
echo Shutting down Moniker service...
for /f "tokens=5" %%a in ('netstat -ano 2^>nul ^| findstr ":%PORT% " ^| findstr "LISTENING"') do (
    taskkill /F /PID %%a >nul 2>&1
)
echo Done.

endlocal
