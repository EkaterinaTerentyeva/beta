# dags/churn.py

import pendulum
from airflow.decorators import dag, task
from sqlalchemy import MetaData, Table, Column, String, Integer, Float, UniqueConstraint, DateTime
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

@dag(
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ETL"]
)
def prepare_churn_dataset():

    @task()
    def create_table():
        from sqlalchemy import MetaData, Table, Column, String, Integer, Float, UniqueConstraint, DateTime
        metadata = MetaData()
        hook = PostgresHook('destination_db')
        engine = hook.get_sqlalchemy_engine()

        users_churn = Table(
            'users_churn',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('customer_id', String, unique=True),
            Column('begin_date', DateTime),
            Column('end_date', DateTime),
            Column('type', String),
            Column('paperless_billing', Integer),
            Column('payment_method', String),
            Column('monthly_charges', Float),
            Column('total_charges', Float),
            Column('internet_service', String),
            Column('online_security', Integer),
            Column('online_backup', Integer),
            Column('device_protection', Integer),
            Column('tech_support', Integer),
            Column('streaming_tv', Integer),
            Column('streaming_movies', Integer),
            Column('gender', String),
            Column('senior_citizen', Integer),
            Column('partner', Integer),
            Column('dependents', Integer),
            Column('multiple_lines', Integer),
            Column('target', Integer),
            UniqueConstraint('customer_id', name='uq_customer_id')
        )

        metadata.create_all(engine)

    create_table()

    @task()
    def extract(**kwargs):
        hook = PostgresHook('source_db')
        conn = hook.get_conn()
        sql = f"""
        SELECT
            c.customer_id, c.begin_date, c.end_date, c.type, c.paperless_billing, c.payment_method, c.monthly_charges, c.total_charges,
            i.internet_service, i.online_security, i.online_backup, i.device_protection, i.tech_support, i.streaming_tv, i.streaming_movies,
            p.gender, p.senior_citizen, p.partner, p.dependents,
            ph.multiple_lines
        FROM contracts as c
        LEFT JOIN internet as i ON i.customer_id = c.customer_id
        LEFT JOIN personal as p ON p.customer_id = c.customer_id
        LEFT JOIN phone as ph ON ph.customer_id = c.customer_id
        """
        data = pd.read_sql(sql, conn)
        conn.close()
        return data

    @task()
    def transform(data: pd.DataFrame):
        data['target'] = (data['end_date'] != 'No').astype(int)
        data['end_date'].replace({'No': None}, inplace=True)
        return data

    @task()
    def load(data: pd.DataFrame):
        hook = PostgresHook('destination_db')
        hook.insert_rows(
            table="users_churn",
            replace=True,
            target_fields=data.columns.tolist(),
            replace_index=['customer_id'],
            rows=data.values.tolist()
        )

    data = extract()
    transformed_data = transform(data)
    load(transformed_data)