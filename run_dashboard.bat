@echo off
title 3C E-Commerce AI Lakehouse Dashboard
echo ============================================================
echo   3C E-Commerce Data Lakehouse - AI Analytics Platform
echo   PostgreSQL + MongoDB + Apache PySpark + Streamlit
echo ============================================================
echo.
echo Starting Dashboard server...
echo Please wait, then open http://localhost:8501 in your browser.
echo DO NOT close this window while using the Dashboard!
echo.
python -m streamlit run "%~dp0app.py" --server.headless=true
pause
