import time
from datetime import datetime

import pandas as pd
import psycopg2

conn = psycopg2.connect(  # создаем подключение к бд
    dbname="banking-etl-db",
    user="ats",
    password="ats",
    host="localhost",
    port="5432",
)

cur = conn.cursor()  # создаем курсор

files_path = "files/"  # путь к таблицам


def start_log():
    # функция начала логирования
    start_time = datetime.now()

    cur.execute(
        """
        INSERT INTO logs.logs (start_time, status)
        VALUES (%s, %s) returning log_id;
        """,
        (start_time, "started"),
    )
    conn.commit()
    log_id = cur.fetchone()[0]  # берем первый элемент курсора (log_id)

    time.sleep(5)

    return log_id


def end_log(log_id, records_count, error_message=None):
    # функция конца логирования
    end_time = datetime.now()
    status = "success"

    try:
        if error_message:
            status = "error"
        cur.execute(
            """
            UPDATE logs.logs
            SET end_time = %s, records_count = %s, status = %s, error_message = %s
            WHERE log_id = %s;
            """,
            (end_time, records_count, status, error_message, log_id),
        )
        conn.commit()
    except Exception as e:
        # ошибка в логировании
        conn.rollback()
        status = "error"
        error_message = f"Ошибка в логировании: {str(e)}"
        cur.execute(
            """
            UPDATE logs.logs
            SET end_time = %s, records_count = %s, status = %s, error_message = %s
            WHERE log_id = %s;
            """,
            (end_time, records_count, status, error_message, log_id),
        )
        conn.commit()


def load_ft_balance_f(csv_file, log_id):
    # функция загрузки таблицы ft_balance_f
    try:
        df = pd.read_csv(csv_file, delimiter=";")

        # проверить наличие пропусков в обязательных столбцах
        df = df.dropna(subset=["ON_DATE", "ACCOUNT_RK", "BALANCE_OUT"])

        # Преобразуем типы данных
        df["ON_DATE"] = pd.to_datetime(df["ON_DATE"], format="%d.%m.%Y")
        df["ACCOUNT_RK"] = df["ACCOUNT_RK"].astype(int)
        df["CURRENCY_RK"] = df["CURRENCY_RK"].astype(int)
        df["BALANCE_OUT"] = df["BALANCE_OUT"].astype(float)

        # sql
        insert_query = """
            INSERT INTO ds.ft_balance_f (on_date, account_rk, currency_rk, balance_out)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (on_date, account_rk) DO UPDATE 
            SET balance_out = EXCLUDED.balance_out;
            """

        for index, row in df.iterrows():
            cur.execute(
                insert_query,
                (
                    row["ON_DATE"],
                    row["ACCOUNT_RK"],
                    row["CURRENCY_RK"],
                    row["BALANCE_OUT"],
                ),
            )

        conn.commit()
        df_len = len(df)  # кол-во записей
        print(f"Загружено {df_len} записей в таблицу ds.ft_balance_f.")
        return df_len

    except Exception as e:
        conn.rollback()
        end_log(
            log_id,
            0,
            f"Ошибка загрузки таблицы ft_balance_f: {str(e)}",
        )
        raise e


def run_etl():
    try:
        # логируем начало etl процесса
        log_id = start_log()

        records_count = 0

        records_count += (
            load_ft_balance_f(  # выполнение функции и подсчет кол-ва записей
                f"{files_path}ft_balance_f.csv", log_id
            )
        )
        end_log(log_id, records_count)

    except Exception as e:
        end_log(log_id, 0, f"Ошибка в процессе ETL: {str(e)}")
        print(f"Ошибка: {e}")


if __name__ == "__main__":
    run_etl()
    cur.close()
    conn.close()
