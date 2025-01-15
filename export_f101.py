import os
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv

from utils.connection import close_db, connect_db
from utils.logging import log

load_dotenv()


def export_f101_data(conn, cur, output_path):
    # экспортирует dm.dm_f101_round_f в CSV
    start_time = datetime.now()
    table_name = "dm.dm_f101_round_f"
    status = "success"

    try:
        cur.execute(  # заголовки
            """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'dm' 
            AND table_name = 'dm_f101_round_f'
            ORDER BY ordinal_position;
            """
        )
        columns = [col[0] for col in cur.fetchall()]

        # берем всё из таблицы
        cur.execute("SELECT * FROM dm.dm_f101_round_f;")
        data = cur.fetchall()

        df = pd.DataFrame(data, columns=columns)

        df.to_csv(output_path, index=False, sep=";", encoding="utf-8")

        records_count = len(df)
        message = f"Выгружено {records_count} записей в файл {output_path}"
        log(
            conn=conn,
            cur=cur,
            start_time=start_time,
            records_count=records_count,
            table_name=table_name,
            status=status,
            message=message,
        )
        print(message)

    except Exception as e:
        status = "error"
        error_message = f"Ошибка выгрузки данных: {str(e)}"
        log(
            conn=conn,
            cur=cur,
            start_time=start_time,
            table_name=table_name,
            message=error_message,
            records_count=None,
            status=status,
        )
        raise e


def export():
    try:
        conn, cur = connect_db()

        # берем путь для экспорта
        output_dir = os.getenv("EXPORT_PATH")
        output_file = "f101_data.csv"
        output_path = os.path.join(output_dir, output_file)

        export_f101_data(conn, cur, output_path)

    except Exception as e:
        print(f"Ошибка при выполнении скрипта: {str(e)}")
        raise e
    finally:
        close_db(conn, cur)


if __name__ == "__main__":
    export()
