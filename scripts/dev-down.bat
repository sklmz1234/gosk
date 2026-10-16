@echo off
rem gosk dev environment stopper - double click to run.
rem Options: -Volumes (also delete data volumes)  -KillFrontend (free port 5173/5174)
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0dev-down.ps1" %*
echo.
pause
