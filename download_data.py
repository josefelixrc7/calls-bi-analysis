
import mysql.connector
import pandas as pd

from connection import Connection

def DownloadData(conn, query, file):

    print("- Download Data: " + file)

    # DB connection
    db = conn.Connect_()
    cursor = db.cursor()

    try:
        # Download records
        cursor.execute(query)
        records = cursor.fetchall()
        print(len(records))

        # Save to CSV
        df = pd.DataFrame(records, columns=['record', 'duration', 'extra', 'status', 'called_at'])
        df.to_csv(file, index=False,sep="\t", header=['record', 'duration', 'extra', 'status', 'called_at'])

    except mysql.connector.Error as e:
        print("- Error to download data: " + e.msg)

    cursor.close()