import os
import time
from datetime import datetime

import pandas as pd
import psycopg2

conn = psycopg2.connect(
    dbname="banking-etl-db",
    user="ats",
    password="ats",
    host="localhost",
    port="5432",
)

cur = conn.cursor()


def start_log():
    start_time = datetime.now()

    cur.execute(
        """
        insert into logs.logs (start_time, status)
        values (%s, %s) returning log_id;
        """,
        (start_time, "started"),
    )
    conn.commit()
    log_id = cur.fetchone()[0]

    time.sleep(5)

    return log_id, start_time


def end_log(log_id, start_time, total_records):
    end_time = datetime.now()
    status = "SUCCESS"
    try:
        # Логируем завершение процесса
        cur.execute(
            """
            UPDATE logs.logs 
            SET end_time = %s, total_records = %s, status = %s 
            WHERE log_id = %s;
        """,
            (end_time, total_records, status, log_id),
        )
        conn.commit()
    except Exception as e:
        status = "ERROR"
        # В случае ошибки записываем сообщение об ошибке
        error_message = str(e)
        cur.execute(
            """
            UPDATE logs.logs 
            SET end_time = %s, total_records = %s, status = %s, message = %s 
            WHERE log_id = %s;
        """,
            (end_time, total_records, status, error_message, log_id),
        )
        conn.commit()


def load_csv_to_db(csv_file):
    table_name = os.path.splitext(os.path.basename(csv_file))[0]
    df = pd.read_csv(csv_file)

    pass


def run_etl():
    try:
        # Логируем начало
        log_id, start_time = start_log()

        total_records = 0

        # Загружаем все CSV из папки files
        files = [
            "files/ft_balance_f.csv",
            "files/ft_posting_f.csv",
            "files/md_account_d.csv",
            "files/md_currency_d.csv",
            "files/md_exchange_rate_d.csv",
            "files/md_ledger_account_s.csv",
        ]

        for file in files:
            total_records += load_csv_to_db(
                file
            )  # Загружаем данные из каждого CSV

        # Логируем завершение
        end_log(log_id, start_time, total_records)

    except Exception as e:
        # В случае ошибки логируем ошибку
        end_log(log_id, start_time, 0)
        print(f"Error: {e}")


if __name__ == "__main__":
    run_etl()
    cur.close()
    conn.close()
