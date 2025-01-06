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
        # df = df.dropna(subset=["ON_DATE", "ACCOUNT_RK", "BALANCE_OUT"])

        # преобразуем дату
        df["ON_DATE"] = pd.to_datetime(df["ON_DATE"], format="%d.%m.%Y")

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
    try:
        # очищаем таблицу
        cur.execute("TRUNCATE TABLE ds.ft_posting_f;")
        conn.commit()

        df = pd.read_csv(csv_file, delimiter=";")

        df["OPER_DATE"] = pd.to_datetime(df["OPER_DATE"], format="%d-%m-%Y")

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


def load_md_currency_d(csv_file, log_id):
    try:
        # меняем кодировку, поскольку стандартная utf-8 вызывала ошибку
        df = pd.read_csv(
            csv_file,
            delimiter=";",
            dtype={
                "CURRENCY_CODE": str
            },  # сразу приводим к строке, иначе будет float
            encoding="cp1252",
            encoding_errors="ignore",
        )

        # наличие пропусков
        df = df.dropna(subset=["DATA_ACTUAL_DATE", "CURRENCY_RK"])

        df["DATA_ACTUAL_DATE"] = pd.to_datetime(
            df["DATA_ACTUAL_DATE"], format="%Y-%m-%d"
        )
        df["DATA_ACTUAL_END_DATE"] = pd.to_datetime(
            df["DATA_ACTUAL_END_DATE"], format="%Y-%m-%d"
        )

        # обрежем до 3 символов, как этого требует ограничение таблицы
        df["CURRENCY_CODE"] = df["CURRENCY_CODE"].str[:3]
        df["CODE_ISO_CHAR"] = df["CODE_ISO_CHAR"].str[:3]

        insert_query = """
            INSERT INTO ds.md_currency_d (currency_rk, data_actual_date,
            data_actual_end_date, currency_code, code_iso_char)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (currency_rk, data_actual_date) DO UPDATE
            SET currency_code = EXCLUDED.currency_code,
                code_iso_char = EXCLUDED.code_iso_char;
            """

        for index, row in df.iterrows():
            cur.execute(
                insert_query,
                (
                    row["CURRENCY_RK"],
                    row["DATA_ACTUAL_DATE"],
                    row["DATA_ACTUAL_END_DATE"],
                    row["CURRENCY_CODE"],
                    row["CODE_ISO_CHAR"],
                ),
            )

        conn.commit()
        df_len = len(df)
        print(f"Загружено {df_len} записей в таблицу ds.md_currency_d.")
        return df_len

    except Exception as e:
        conn.rollback()
        end_log(
            log_id,
            0,
            f"Ошибка загрузки таблицы md_currency_d: {str(e)}",
        )
        raise e


def load_md_exchange_rate_d(csv_file, log_id):
    try:
        df = pd.read_csv(
            csv_file,
            delimiter=";",
            dtype={"CODE_ISO_NUM": str},
        )

        df["DATA_ACTUAL_DATE"] = pd.to_datetime(
            df["DATA_ACTUAL_DATE"], format="%Y-%m-%d"
        )
        df["DATA_ACTUAL_END_DATE"] = pd.to_datetime(
            df["DATA_ACTUAL_END_DATE"], format="%Y-%m-%d"
        )

        insert_query = """
            INSERT INTO ds.md_exchange_rate_d (data_actual_date, 
                data_actual_end_date, currency_rk, reduced_cource, code_iso_num)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (currency_rk, data_actual_date) DO UPDATE 
            SET reduced_cource = EXCLUDED.reduced_cource,
                code_iso_num = EXCLUDED.code_iso_num;
        """

        for index, row in df.iterrows():
            cur.execute(
                insert_query,
                (
                    row["DATA_ACTUAL_DATE"],
                    row["DATA_ACTUAL_END_DATE"],
                    row["CURRENCY_RK"],
                    row["REDUCED_COURCE"],
                    row["CODE_ISO_NUM"],
                ),
            )

        conn.commit()
        df_len = len(df)
        print(f"Загружено {df_len} записей в таблицу ds.md_exchange_rate_d.")
        return df_len

    except Exception as e:
        conn.rollback()
        end_log(
            log_id,
            0,
            f"Ошибка загрузки таблицы md_exchange_rate_d: {str(e)}",
        )
        raise e


def load_md_ledger_account_s(csv_file, log_id):
    # в исходных данных есть следующие колонки (12):
    # CHAPTER;CHAPTER_NAME;SECTION_NUMBER;SECTION_NAME;SUBSECTION_NAME;LEDGER1_ACCOUNT;
    # LEDGER1_ACCOUNT_NAME;LEDGER_ACCOUNT;LEDGER_ACCOUNT_NAME;CHARACTERISTIC;START_DATE;END_DATE

    try:
        df = pd.read_csv(csv_file, delimiter=";")

        df["START_DATE"] = pd.to_datetime(df["START_DATE"], format="%Y-%m-%d")
        df["END_DATE"] = pd.to_datetime(
            df["END_DATE"], format="%Y-%m-%d", errors="coerce"
        )


        insert_query = """
            INSERT INTO ds.md_ledger_account_s (chapter, chapter_name,
                section_number, section_name, subsection_name,
            ledger1_account, ledger1_account_name, ledger_account,
                ledger_account_name, characteristic, start_date, end_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ledger_account, start_date) DO UPDATE
            SET chapter = EXCLUDED.chapter,
                chapter_name = EXCLUDED.chapter_name,
                section_number = EXCLUDED.section_number,
                section_name = EXCLUDED.section_name,
                subsection_name = EXCLUDED.subsection_name,
                ledger1_account = EXCLUDED.ledger1_account,
                ledger1_account_name = EXCLUDED.ledger1_account_name,
                ledger_account_name = EXCLUDED.ledger_account_name,
                characteristic = EXCLUDED.characteristic,
                end_date = EXCLUDED.end_date
            """

        for index, row in df.iterrows():
            cur.execute(
                insert_query,
                (
                    row["CHAPTER"],
                    row["CHAPTER_NAME"],
                    row["SECTION_NUMBER"],
                    row["SECTION_NAME"],
                    row["SUBSECTION_NAME"],
                    row["LEDGER1_ACCOUNT"],
                    row["LEDGER1_ACCOUNT_NAME"],
                    row["LEDGER_ACCOUNT"],
                    row["LEDGER_ACCOUNT_NAME"],
                    row["CHARACTERISTIC"],
                    row["START_DATE"],
                    row["END_DATE"],
                ),
            )

        conn.commit()
        df_len = len(df)
        print(f"Загружено {df_len} записей в таблицу ds.md_ledger_account_s.")
        return df_len

    except Exception as e:
        conn.rollback()
        end_log(
            log_id,
            0,
            f"Ошибка загрузки таблицы md_ledger_account_s: {str(e)}",
        )
        raise e


def run_etl():
    try:
        # логируем начало etl
        log_id = start_log()

        records_count = 0

        # выполнение функций и подсчет кол-ва записей
        records_count += load_ft_balance_f(
            f"{files_path}ft_balance_f.csv", log_id
        )
        records_count += load_ft_posting_f(
            f"{files_path}ft_posting_f.csv", log_id
        )
        records_count += load_md_account_d(
            f"{files_path}md_account_d.csv", log_id
        )
        records_count += load_md_currency_d(
            f"{files_path}md_currency_d.csv", log_id
        )
        records_count += load_md_exchange_rate_d(
            f"{files_path}md_exchange_rate_d.csv", log_id
        )
        records_count += load_md_ledger_account_s(
            f"{files_path}md_ledger_account_s.csv", log_id
        )

        # логируем конец etl
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
