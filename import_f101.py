import os
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv

from utils.connection import close_db, connect_db
from utils.logging import log

load_dotenv()


def import_f101_data(conn, cur, input_path):
    # импорт в копию
    start_time = datetime.now()
    table_name = "dm.dm_f101_round_f_v2"

    try:
        df = pd.read_csv(input_path, sep=";")

        # очистим таблицу
        cur.execute(f"TRUNCATE TABLE {table_name};")

        insert_query = f"""
            INSERT INTO {table_name} (from_date, to_date, chapter, ledger_account, 
            characteristic, balance_in_rub, r_balance_in_rub, balance_in_val, 
            r_balance_in_val, balance_in_total, r_balance_in_total, turn_deb_rub, 
            r_turn_deb_rub, turn_deb_val, r_turn_deb_val, turn_deb_total, 
            r_turn_deb_total, turn_cre_rub, r_turn_cre_rub, turn_cre_val, 
            r_turn_cre_val, turn_cre_total, r_turn_cre_total, balance_out_rub, 
            r_balance_out_rub, balance_out_val, r_balance_out_val, balance_out_total)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        for index, row in df.iterrows():
            cur.execute(
                insert_query,
                (
                    row["from_date"],
                    row["to_date"],
                    row["chapter"],
                    row["ledger_account"],
                    row["characteristic"],
                    row["balance_in_rub"],
                    row["r_balance_in_rub"],
                    row["balance_in_val"],
                    row["r_balance_in_val"],
                    row["balance_in_total"],
                    row["r_balance_in_total"],
                    row["turn_deb_rub"],
                    row["r_turn_deb_rub"],
                    row["turn_deb_val"],
                    row["r_turn_deb_val"],
                    row["turn_deb_total"],
                    row["r_turn_deb_total"],
                    row["turn_cre_rub"],
                    row["r_turn_cre_rub"],
                    row["turn_cre_val"],
                    row["r_turn_cre_val"],
                    row["turn_cre_total"],
                    row["r_turn_cre_total"],
                    row["balance_out_rub"],
                    row["r_balance_out_rub"],
                    row["balance_out_val"],
                    row["r_balance_out_val"],
                    row["balance_out_total"],
                ),
            )

        conn.commit()
        records_count = len(df)
        status = "success"

        message = f"Загружено {records_count} записей из файла {input_path}"
        log(
            conn=conn,
            cur=cur,
            start_time=start_time,
            records_count=records_count,
            table_name=table_name,
            message=message,
            status=status,
        )
        print(message)

        return records_count

    except Exception as e:
        conn.rollback()
        error_message = f"Ошибка загрузки данных: {str(e)}"
        status = "error"
        log(
            conn=conn,
            cur=cur,
            start_time=start_time,
            records_count=None,
            table_name=table_name,
            message=error_message,
            status=status,
        )
        raise e


def import_f101():
    try:
        conn, cur = connect_db()

        input_path = os.getenv("EXPORT_PATH") + "f101_data.csv"

        import_f101_data(conn, cur, input_path)

    except Exception as e:
        print(f"Ошибка при выполнении скрипта: {str(e)}")
        raise e
    finally:
        close_db(conn, cur)


if __name__ == "__main__":
    import_f101()
