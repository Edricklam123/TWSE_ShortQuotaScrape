REM Author: Edrick Lam
REM Create date: 11/27/2022
REM Automated Batch Script to drive the scraper

SET project_dir=C:\Users\Edrick\PycharmProjects\pythonProject\twse_sq_scraper
CALL %project_dir%"\venv\Scripts\activate.bat"

CD %project_dir%

CALL %project_dir%"\venv\Scripts\python.exe" %project_dir%"\main\dataScraping.py"

PAUSE