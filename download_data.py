
import pandas as pd
import mysql.connector

def DownloadData():

    print("- Download Data")

    # DB connection
    db_host = 'localhost'
    db_user = 'root'
    db_password = '0UHC72zNvywZ'
    db_name = 'catBI'
    db = mysql.connector.connect(host=db_host, user=db_user, passwd=db_password, db=db_name)
    cursor = db.cursor()

    # Upload records to DB
    try:
        # Download records
        cursor.execute("SELECT id, record FROM records")
        records = cursor.fetchall()
        db.commit()

        # Download transactions 3 MONTHS
        cursor.execute("""
            SELECT
                t.id_record AS id_record
                ,t.duration AS duration
                ,t.user AS user
                ,t.called_at AS called_at
                ,s.status AS status
                ,n.nir AS nir
            FROM transactions t
            JOIN statuses s ON s.id = t.id_status
            JOIN nirs n ON n.id = t.id_nir
            WHERE called_at >= NOW() - INTERVAL 3 MONTH          
        """)
        transactions = cursor.fetchall()

        # Save to CSV
        df = pd.DataFrame(records)
        df.to_csv('~/records.csv', index=False)

        df2 = pd.DataFrame(transactions)
        df2.to_csv('~/transactions.csv', index=False)

    except mysql.connector.Error as e:
        print("- Error to download data: " + e.msg)

    cursor.close()