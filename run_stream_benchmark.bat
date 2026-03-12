@echo off
title 3C E-Commerce Stream Generator
echo ================================================================
echo    Real-Time Streaming Data Generator for Benchmark
echo    PostgreSQL + MongoDB + PySpark Streaming
echo ================================================================
echo.
echo Choose mode:
echo   1. Continuous Stream (100 TPS for 5 minutes)
echo   2. Quick Test (10 TPS for 30 seconds)
echo   3. Long Run (50 TPS for 10 minutes)
echo.
set /p choice="Enter choice (1/2/3): "

if "%choice%"=="1" (
    echo Starting continuous stream at 100 TPS...
    python stream_generator_host.py --tps=100 --duration=300
) else if "%choice%"=="2" (
    echo Starting quick test...
    python stream_generator_host.py --tps=10 --duration=30
) else if "%choice%"=="3" (
    echo Starting long run...
    python stream_generator_host.py --tps=50 --duration=600
) else (
    echo Invalid choice!
)
pause
