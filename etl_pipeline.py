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

    time.sleep(5)  # ждем 5 сек по условиям задачи

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

        # преобразуем типы данных
        df["ON_DATE"] = pd.to_datetime(df["ON_DATE"], format="%d.%m.%Y")
        df["ACCOUNT_RK"] = df["ACCOUNT_RK"].astype(int)
        df["CURRENCY_RK"] = df["CURRENCY_RK"].astype(int)
        df["BALANCE_OUT"] = df["BALANCE_OUT"].astype(float)

        # sql, для корректного обновления данных при вставке в уникальные
        # строки обновляем данные (excluded)
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


def load_ft_posting_f(csv_file, log_id):
    # функция загрузки таблицы ft_posting_f
    try:
        # очищаем таблицу
        cur.execute("TRUNCATE TABLE ds.ft_posting_f;")
        conn.commit()

        df = pd.read_csv(csv_file, delimiter=";")

        df["OPER_DATE"] = pd.to_datetime(df["OPER_DATE"], format="%d-%m-%Y")

        df = df.dropna(
            subset=[
                "OPER_DATE",
                "CREDIT_ACCOUNT_RK",
                "DEBET_ACCOUNT_RK",
                "CREDIT_AMOUNT",
                "DEBET_AMOUNT",
            ]
        )

        df["CREDIT_ACCOUNT_RK"] = df["CREDIT_ACCOUNT_RK"].astype(int)
        df["DEBET_ACCOUNT_RK"] = df["DEBET_ACCOUNT_RK"].astype(int)
        df["CREDIT_AMOUNT"] = df["CREDIT_AMOUNT"].astype(float)
        df["DEBET_AMOUNT"] = df["DEBET_AMOUNT"].astype(float)

        insert_query = """
            INSERT INTO ds.ft_posting_f (oper_date, credit_account_rk, 
                debet_account_rk, credit_amount, debet_amount)
            VALUES (%s, %s, %s, %s, %s);
            """

        for index, row in df.iterrows():
            cur.execute(
                insert_query,
                (
                    row["OPER_DATE"],
                    row["CREDIT_ACCOUNT_RK"],
                    row["DEBET_ACCOUNT_RK"],
                    row["CREDIT_AMOUNT"],
                    row["DEBET_AMOUNT"],
                ),
            )

        conn.commit()
        df_len = len(df)
        print(f"Загружено {df_len} записей в таблицу ds.ft_posting_f.")
        return df_len

    except Exception as e:
        conn.rollback()
        end_log(
            log_id,
            0,
            f"Ошибка загрузки таблицы ft_posting_f: {str(e)}",
        )
        raise e


def load_md_account_d(csv_file, log_id):
    try:
        df = pd.read_csv(csv_file, delimiter=";")

        df["DATA_ACTUAL_DATE"] = pd.to_datetime(
            df["DATA_ACTUAL_DATE"], format="%Y-%m-%d"
        )
        df["DATA_ACTUAL_END_DATE"] = pd.to_datetime(
            df["DATA_ACTUAL_END_DATE"], format="%Y-%m-%d"
        )

        df = df.dropna(
            subset=["DATA_ACTUAL_DATE", "ACCOUNT_RK", "CURRENCY_RK"]
        )

        df["ACCOUNT_RK"] = df["ACCOUNT_RK"].astype(int)
        df["CURRENCY_RK"] = df["CURRENCY_RK"].astype(int)

        insert_query = """
            INSERT INTO ds.md_account_d (data_actual_date, 
            data_actual_end_date, account_rk, account_number, char_type, currency_rk, currency_code)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (data_actual_date, account_rk) DO UPDATE 
            SET account_number = EXCLUDED.account_number,
                char_type = EXCLUDED.char_type,
                currency_rk = EXCLUDED.currency_rk,
                currency_code = EXCLUDED.currency_code;
        """

        for index, row in df.iterrows():
            cur.execute(
                insert_query,
                (
                    row["DATA_ACTUAL_DATE"],
                    row["DATA_ACTUAL_END_DATE"],
                    row["ACCOUNT_RK"],
                    row["ACCOUNT_NUMBER"],
                    row["CHAR_TYPE"],
                    row["CURRENCY_RK"],
                    row["CURRENCY_CODE"],
                ),
            )

        conn.commit()
        df_len = len(df)
        print(f"Загружено {df_len} записей в таблицу ds.md_account_d.")
        return df_len

    except Exception as e:
        conn.rollback()
        end_log(
            log_id,
            0,
            f"Ошибка загрузки таблицы md_account_d: {str(e)}",
        )
        raise e


def run_etl():
    try:
        # логируем начало etl процесса
        log_id = start_log()

        records_count = 0

        # выполнение функций и подсчет кол-ва записей
        records_count += load_ft_balance_f(
            f"{files_path}ft_balance_f.csv", log_id
        )
        # records_count += load_ft_posting_f(
        #     f"{files_path}ft_posting_f.csv", log_id
        # )
        records_count += load_md_account_d(
            f"{files_path}md_account_d.csv", log_id
        )

        end_log(log_id, records_count)

    except Exception as e:
        end_log(log_id, 0, f"Ошибка в процессе ETL: {str(e)}")
        print(f"Ошибка: {e}")


if __name__ == "__main__":
    # запускаем etl
    run_etl()
    # закрываем подключение и курсор
    cur.close()
    conn.close()
