from datetime import datetime


def log(
    conn,
    cur,
    start_time,
    records_count=None,
    status=None,
    table_name=None,
    method="python script",
    message=None,
):
    # функция логирования
    end_time = datetime.now()

    try:
        cur.execute(
            """
            INSERT INTO logs.logs (start_time, end_time, records_count, status, message, table_name, method)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                start_time,
                end_time,
                records_count,
                status,
                message,
                table_name,
                method,
            ),
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Ошибка в логировании: {str(e)}")
