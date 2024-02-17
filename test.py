import os
import psycopg2
from psycopg2 import sql

# Загружаем переменные из .env
from dotenv import load_dotenv
load_dotenv('/home/mle-user/mle_projects/beta/.env_template')


conn = psycopg2.connect(
    host=os.getenv('DB_SOURCE_HOST'),
    port=os.getenv('DB_SOURCE_PORT'),
    user=os.getenv('DB_SOURCE_USER'),
    password=os.getenv('DB_SOURCE_PASSWORD'),
    dbname=os.getenv('DB_SOURCE_NAME')
)

cur = conn.cursor()

# Выполним запрос на вывод всех таблиц в текущей базе данных
cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")

print(cur.fetchall())

cur.close()
conn.close()