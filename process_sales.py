
import mysql.connector

from connection import Connection

def ProcessSales():

    print("- Process sales records")

    # DB connection
    conn = Connection('localhost', 'root', '0UHC72zNvywZ', 'catBI')
    db = conn.Connect_()
    cursor = db.cursor()

    try:

        # Create segment
        cursor.execute("INSERT INTO segments (name) VALUES ('sales')")
        db.commit()
        id_segment = cursor._last_insert_id

        # JOIN and exclude records
        cursor.execute(
        """
            INSERT INTO segments_records(id_record, id_segment)
            SELECT t.id_record, """ + str(id_segment) + """
            FROM transactions t
            LEFT JOIN segments_records sr ON sr.id_record = t.id_record
            JOIN statuses s ON s.id = t.id_status
            WHERE
                sr.id_record IS NULL
                AND s.status IN ('11', '13', '600', '602', '603')
                AND t.called_at >= NOW() - INTERVAL 1 MONTH
            GROUP BY t.id_record
        """)
        db.commit()

    except mysql.connector.Error as e:
        print("- Error to process no reusable records: " + e.msg)
