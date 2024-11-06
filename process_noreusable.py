
import mysql.connector

from connection import Connection

def ProcessNoreusable():

    print("- Process no reusable records")

    # DB connection
    conn = Connection('localhost', 'root', '0UHC72zNvywZ', 'catBI')
    db = conn.Connect_()
    cursor = db.cursor()

    try:

        # Create segment
        cursor.execute("INSERT INTO segments (name) VALUES ('noreusable')")
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
                AND s.reusable = 0
                AND t.called_at >= NOW() - INTERVAL 7 DAY
            GROUP BY t.id_record
        """)
        db.commit()

    except mysql.connector.Error as e:
        print("- Error to process no reusable records: " + e.msg)
