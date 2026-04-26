@echo off
setlocal
set "PYTHONHOME=C:\Program Files\MySQL\MySQL Shell 8.0\lib\Python3.13"
set "PATH=C:\Program Files\MySQL\MySQL Shell 8.0\bin;%PATH%"
set "PYTHONPATH=D:\SellerOptic_Trail_APP\.venv\Lib\site-packages"
set "FLASK_ENV=development"
set "FLASK_DEBUG=1"
set "PORT=5000"
cd /d D:\SellerOptic_Trail_APP
"C:\Program Files\MySQL\MySQL Shell 8.0\lib\Python3.13\Lib\venv\scripts\nt\python.exe" -u run.py
