
import mysql.connector

from connection import Connection

def ProcessOverused():

    print("- Process no duration records")

    # DB connection
    conn = Connection('localhost', 'root', '0UHC72zNvywZ', 'catBI')
    db = conn.Connect_()
    cursor = db.cursor()

    try:

        # Create segment
        cursor.execute("INSERT INTO segments (name) VALUES ('noduration')")
        db.commit()
        id_segment = cursor._last_insert_id

        # JOIN and exclude records
        cursor.execute(
        """
            INSERT INTO segments_records(id_record, id_segment)
            SELECT t.id_record, """ + str(id_segment) + """
            FROM transactions t
            LEFT JOIN segments_records sr ON sr.id_record = t.id_record
            WHERE
                sr.id_record IS NULL
                AND t.called_at >= NOW() - INTERVAL 1 MONTH
            GROUP BY t.id_record
            HAVING MAX(t.duration) = 0
        """)
        db.commit()

    except mysql.connector.Error as e:
        print("- Error to process no duration records: " + e.msg)
