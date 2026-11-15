@echo off
rem AccessSentinel - stop all containers (data is kept; start.bat starts it again).
cd /d "%~dp0"
docker compose down
pause
