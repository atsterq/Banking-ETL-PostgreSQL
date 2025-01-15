import os
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv

from utils.connection import close_db, connect_db
from utils.logging import log

load_dotenv()


FILES_PATH = os.getenv("FILES_PATH")


def load_ft_balance_f(conn, cur, csv_file):
    # функция загрузки таблицы ft_balance_f
    start_time = datetime.now()
    table_name = "ds.ft_balance_f"

    try:
        df = pd.read_csv(csv_file, delimiter=";")

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
        records_count = len(df)  # кол-во записей
        status = "success"
        log(
            conn,
            cur,
            start_time,
            records_count,
            table_name=table_name,
            status=status,
        )
        print(f"Загружено {records_count} записей в таблицу {table_name}.")

    except Exception as e:
        conn.rollback()
        status = "error"
        log(
            conn,
            cur,
            start_time,
            message=f"Ошибка загрузки таблицы {table_name}: {str(e)}",
            table_name=table_name,
            status=status,
        )
        raise e


def load_ft_posting_f(conn, cur, csv_file):
    start_time = datetime.now()
    table_name = "ds.ft_posting_f"

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
        records_count = len(df)
        status = "success"
        log(
            conn,
            cur,
            start_time,
            records_count,
            table_name=table_name,
            status=status,
        )
        print(f"Загружено {records_count} записей в таблицу {table_name}.")

    except Exception as e:
        conn.rollback()
        status = "error"
        log(
            conn,
            cur,
            start_time,
            message=f"Ошибка загрузки таблицы {table_name}: {str(e)}",
            table_name=table_name,
            status=status,
        )
        raise e


def load_md_account_d(conn, cur, csv_file):
    start_time = datetime.now()
    table_name = "ds.md_account_d"

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
        records_count = len(df)
        status = "success"
        log(
            conn,
            cur,
            start_time,
            records_count,
            table_name=table_name,
            status=status,
        )
        print(f"Загружено {records_count} записей в таблицу {table_name}.")

    except Exception as e:
        conn.rollback()
        status = "error"
        log(
            conn,
            cur,
            start_time,
            message=f"Ошибка загрузки таблицы {table_name}: {str(e)}",
            table_name=table_name,
            status=status,
        )
        raise e


def load_md_currency_d(conn, cur, csv_file):
    start_time = datetime.now()
    table_name = "ds.md_currency_d"

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
        records_count = len(df)
        status = "success"
        log(
            conn,
            cur,
            start_time,
            records_count,
            table_name=table_name,
            status=status,
        )
        print(f"Загружено {records_count} записей в таблицу {table_name}.")

    except Exception as e:
        conn.rollback()
        status = "error"
        log(
            conn,
            cur,
            start_time,
            message=f"Ошибка загрузки таблицы {table_name}: {str(e)}",
            table_name=table_name,
            status=status,
        )
        raise e


def load_md_exchange_rate_d(conn, cur, csv_file):
    start_time = datetime.now()
    table_name = "ds.md_exchange_rate_d"

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
        records_count = len(df)
        status = "success"
        log(
            conn,
            cur,
            start_time,
            records_count,
            table_name=table_name,
            status=status,
        )
        print(f"Загружено {records_count} записей в таблицу {table_name}.")

    except Exception as e:
        conn.rollback()
        status = "error"
        log(
            conn,
            cur,
            start_time,
            message=f"Ошибка загрузки таблицы {table_name}: {str(e)}",
            table_name=table_name,
            status=status,
        )
        raise e


def load_md_ledger_account_s(conn, cur, csv_file):
    start_time = datetime.now()
    table_name = "ds.md_ledger_account_s"

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
        records_count = len(df)
        status = "success"
        log(
            conn,
            cur,
            start_time,
            records_count,
            table_name=table_name,
            status=status,
        )
        print(f"Загружено {records_count} записей в таблицу {table_name}.")

    except Exception as e:
        conn.rollback()
        status = "error"
        log(
            conn,
            cur,
            start_time,
            message=f"Ошибка загрузки таблицы {table_name}: {str(e)}",
            table_name=table_name,
            status=status,
        )
        raise e


def run_etl():
    try:
        # открываем подключение и курсор
        conn, cur = connect_db()

        # cписок таблиц для загрузки с их файлами и функциями
        tables_to_load = [
            (f"{FILES_PATH}ft_balance_f.csv", load_ft_balance_f),
            (f"{FILES_PATH}ft_posting_f.csv", load_ft_posting_f),
            (f"{FILES_PATH}md_account_d.csv", load_md_account_d),
            (f"{FILES_PATH}md_currency_d.csv", load_md_currency_d),
            (f"{FILES_PATH}md_exchange_rate_d.csv", load_md_exchange_rate_d),
            (f"{FILES_PATH}md_ledger_account_s.csv", load_md_ledger_account_s),
        ]

        # Загружаем каждую таблицу
        for csv_file, load_function in tables_to_load:
            try:
                load_function(conn, cur, csv_file)

            except Exception as e:
                print(f"Ошибка загрузки {csv_file}: {str(e)}")
                raise

    except Exception as e:
        print(f"Ошибка в run_etl: {str(e)}")
        raise

    finally:
        # закрываем подключение и курсор
        close_db(conn, cur)


if __name__ == "__main__":
    # запускаем etl
    run_etl()
