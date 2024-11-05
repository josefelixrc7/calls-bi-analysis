
import mysql.connector

def AddTransactions():

    print("- Add transactions")

    # DB connection
    db_host = 'localhost'
    db_user = 'root'
    db_password = '0UHC72zNvywZ'
    db_name = 'catBI'
    db = mysql.connector.connect(host=db_host, user=db_user, passwd=db_password, db=db_name)
    cursor = db.cursor()

    try:

        # Add new records
        cursor.execute("""
            INSERT INTO records (record)
            SELECT
                DISTINCT pre.record
            FROM transactions_pre pre
            LEFT JOIN records r ON r.record = pre.record
            WHERE
                r.record IS NULL
                AND pre.record IS NOT NULL
        """)
        db.commit()

        # Add transactions
        cursor.execute("""
            INSERT INTO transactions (id_record, duration, user, called_at, id_status, id_nir)
            SELECT
                r.id
                ,pre.duration
                ,pre.user
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
