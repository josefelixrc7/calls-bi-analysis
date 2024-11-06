
import mysql.connector

from connection import Connection

def ProcessOverused():

    print("- Process overused records")

    # DB connection
    conn = Connection('localhost', 'root', '0UHC72zNvywZ', 'catBI')
    db = conn.Connect_()
    cursor = db.cursor()

    try:

        # Add new records
        cursor.execute("""TRUNCATE TABLE records_overused""")
        db.commit()

        # Add transactions
        cursor.execute("""
            INSERT INTO records_overused (id_record)
            SELECT t.id_record
            FROM transactions t
            WHERE
                t.called_at >= NOW() - INTERVAL 1 MONTH
                AND t.duration >= 20
            GROUP BY t.id_record
            HAVING COUNT(t.id) >= 6
        """)
        db.commit()

    except mysql.connector.Error as e:
        print("- Error to process overused records: " + e.msg)
