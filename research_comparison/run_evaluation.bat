@echo off
cd /d "%~dp0\.."
python -m research_comparison.run_evaluation %*
pause
