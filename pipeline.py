# import os
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


def end_log(log_id, records_count, error_message=None):
    end_time = datetime.now()
    status = "success"

    # if error_message:
    #     status = "error"

    try:
        cur.execute(
            """
            update logs.logs
            set end_time = %s, records_count = %s, status = %s, error_message = %s
            where log_id = %s;
            """,
            (end_time, records_count, status, log_id),
        )
        conn.commit()
    except Exception as e:
        status = "error"
        error_message = str(e)
        cur.execute(
            """
            update logs.logs
            set end_time = %s, records_count = %s, status = %s, error_message = %s
            where log_id = %s;
            """,
            (end_time, records_count, status, error_message, log_id),
        )
        conn.commit()


# def load_csv_to_db(csv_file):
#     table_name = os.path.splitext(os.path.basename(csv_file))[0]
#     df = pd.read_csv(csv_file)

#     pass


# def run_etl():
#     try:
#         log_id, start_time = start_log()

#         records_count = 0

#         files = [
#             "files/ft_balance_f.csv",
#             "files/ft_posting_f.csv",
#             "files/md_account_d.csv",
#             "files/md_currency_d.csv",
#             "files/md_exchange_rate_d.csv",
#             "files/md_ledger_account_s.csv",
#         ]

#         for file in files:
#             records_count += load_csv_to_db(
#                 file
#             )

#         end_log(log_id, start_time, records_count)

#     except Exception as e:
#         end_log(log_id, start_time, 0)
#         print(f"Error: {e}")


def load_ft_balance_f(csv_file, log_id):
    try:
        df = pd.read_csv(csv_file, delimiter=';')
        for index, row in df.iterrows():
            cur.execute(
                """
                insert into ds.ft_balance_f (on_date, account_rk, currency_rk, balance_out)
                values (%s, %s, %s, %s)
                on conflict (on_date, account_rk) do update
                set balance_out = excluded.balance_out;
                """,
                (
                    row["on_date"],
                    row["account_rk"],
                    row["currency_rk"],
                    row["balance_out"],
                ),
            )

        conn.commit()
        return len(df)

    except Exception as e:
        end_log(
            log_id,
            0,
            f"Ошибка загрузки таблицы ft_balance_f: {str(e)}",
        )
        raise e


def run_etl():
    try:
        # Логируем начало процесса
        log_id, start_time = start_log()

        records_count = 0

        # Загружаем все CSV из папки files
        records_count += load_ft_balance_f("files/ft_balance_f.csv", log_id)
        # records_count += load_ft_posting_f("files/ft_posting_f.csv", log_id)
        # records_count += load_md_account_d("files/md_account_d.csv", log_id)
        # records_count += load_md_currency_d("files/md_currency_d.csv", log_id)
        # records_count += load_md_exchange_rate_d(
        #     "files/md_exchange_rate_d.csv", log_id
        # )
        # records_count += load_md_ledger_account_s(
        #     "files/md_ledger_account_s.csv", log_id
        # )

        end_log(log_id, start_time, records_count)

    except Exception as e:
        end_log(log_id, 0, f"Ошибка в процессе ETL: {str(e)}")
        print(f"Ошибка: {e}")


if __name__ == "__main__":
    run_etl()
    cur.close()
    conn.close()
