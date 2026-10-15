@echo off
rem gosk dev environment launcher - double click to run.
rem Options: -Build (rebuild images)  -Seed (load test data)  -SkipDocker  -NoFrontend  -NoBrowser
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0dev-up.ps1" %*
echo.
pause
