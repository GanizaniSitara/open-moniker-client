@echo off
REM Reload Jupyter notebook — kill, relaunch, open browser.
REM Usage: relaunch.cmd [port]
setlocal enabledelayedexpansion

set "PORT=%~1"
if "%PORT%"=="" set "PORT=8888"

set "NB=notebooks\showcase.ipynb"
set "DIR=%~dp0"
set "LOG=%TEMP%\jupyter-moniker.log"

REM Kill any existing Jupyter on this port
echo Stopping any existing Jupyter on port %PORT%...
for /f "tokens=5" %%a in ('netstat -ano 2^>nul ^| findstr ":%PORT% " ^| findstr "LISTENING"') do (
    taskkill /F /PID %%a >nul 2>&1
)
timeout /t 1 /nobreak >nul 2>&1

REM Launch Jupyter in background
echo Starting Jupyter on port %PORT%...
start /B "" python -m jupyter notebook "%DIR%%NB%" --no-browser --port %PORT% --ip 127.0.0.1 > "%LOG%" 2>&1

REM Wait for token (up to 20 attempts, 0.5s each ≈ 10s)
set "TOKEN="
for /L %%i in (1,1,20) do (
    if "!TOKEN!"=="" (
        for /f "delims=" %%t in ('python -c "import re; f=open(r'%LOG%'); m=re.search(r'token=([a-f0-9]+)',f.read()); print(m.group(1) if m else '')" 2^>nul') do (
            if not "%%t"=="" set "TOKEN=%%t"
        )
        if "!TOKEN!"=="" timeout /t 1 /nobreak >nul 2>&1
    )
)

if "%TOKEN%"=="" (
    echo Jupyter didn't start in time. Check %LOG%
    exit /b 1
)

set "URL=http://127.0.0.1:%PORT%/notebooks/showcase.ipynb?token=%TOKEN%"
echo %URL%
start "" "%URL%"

endlocal
