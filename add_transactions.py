
import mysql.connector

from connection import Connection

def AddTransactions():

    print("- Add transactions")

    # DB connection
    conn = Connection('localhost', 'root', '0UHC72zNvywZ', 'catBI')
    db = conn.Connect_()
    cursor = db.cursor()

    try:

        # Add new records (Referidos Database)
        cursor.execute("""
            INSERT INTO records (record, id_database)
            SELECT
                DISTINCT pre.record
                ,3
            FROM transactions_pre pre
            LEFT JOIN records r ON r.record = pre.record
            WHERE
                r.record IS NULL
                AND pre.record IS NOT NULL
        """)
        db.commit()

        # Add transactions
        cursor.execute("""
            INSERT INTO transactions (id_record, duration, extra, called_at, id_status, id_nir)
            SELECT
                r.id
                ,pre.duration
                ,pre.extra
                ,pre.called_at
                ,s.id
                ,n.id
            FROM transactions_pre pre
            JOIN records r ON r.record = pre.record
            JOIN statuses s ON s.status = pre.status
            JOIN nirs n ON n.nir = SUBSTRING(pre.record, 1, 3)
        """)
        db.commit()

    except mysql.connector.Error as e:
        print("- Error to add transactions: " + e.msg)
