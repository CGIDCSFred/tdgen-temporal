@echo off
start "" http://localhost:8501
"%~dp0.venv\Scripts\streamlit" run "%~dp0ui\app.py" --server.headless=false %*
