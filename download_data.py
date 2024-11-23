
import mysql.connector
import pandas as pd

from connection import Connection

def DownloadData(conn, query, file, columns):

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
        df = pd.DataFrame(records, columns=columns)
        df.to_csv(file, index=False,sep="\t", header=columns)

    except mysql.connector.Error as e:
        print("- Error to download data: " + e.msg)

    cursor.close()