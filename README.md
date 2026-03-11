# SQL-проект (PostgreSQL)

## 1. Требования
- Python 3.10+
- PostgreSQL (локально)
- База данных: `db_1`
- Файл `.env` в корне проекта:

```env
DATABASE_USER=postgres
DATABASE_PASSWORD=12345678
# необязательно:
# SOURCE_SCHEMA=public
```

Установка зависимостей:

```powershell
pip install pandas openpyxl psycopg2-binary python-dotenv
```

## 2. Файлы проекта
- `init_sql_project.py` - создает схему и базовые таблицы DWH.
- `main.py` - ETL-пайплайн (`data` -> `STG` -> `DWH` -> `REP_FRAUD` -> `archive`).
- `run_qa.py` - запускает `sql_scripts/qa_checks.sql` и выводит результаты в виде таблиц в терминале.
- `reset_data_load.py` - очищает таблицы, загружаемые из файлов, и `META_FILE_LOAD`.
- `restore_data_from_archive.py` - возвращает файлы `*.backup` из `archive` обратно в `data`.
- `sql_scripts/build_rep_fraud.sql` - логика построения витрины фрода.
- `sql_scripts/qa_checks.sql` - QA-проверки после ETL.

## 3. Первичный запуск
```powershell
python init_sql_project.py
python main.py
python run_qa.py
```

## 4. Повторный запуск на тех же входных файлах
Если файлы уже были обработаны и перемещены в `archive`:

```powershell
python restore_data_from_archive.py
python reset_data_load.py
python main.py
python run_qa.py
```

## 5. Подготовка к сдаче
Собрать чистую папку только с нужными файлами:

```powershell
python prepare_submission.py
```

Итоговая папка: `submission_ready/`
