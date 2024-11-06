
import pandas as pd
import mysql.connector

from connection import Connection
import tools as t

def UploadData(csv_file):

    print("- Upload Data: " + csv_file)

    # DB connection
    conn = Connection('localhost', 'root', '0UHC72zNvywZ', 'catBI')
    db = conn.Connect_()
    cursor = db.cursor()

    # Read file
    print("- Reading file")
    data = pd.read_csv(csv_file, sep = "\t")

    # Truncate transactions_pre
    cursor.execute("TRUNCATE TABLE transactions_pre")
    db.commit()

    # Iterate over CSV records
    array = []
    for index, row in data.iterrows():

        # Values
        record = t.FormatString(str(row['record']))
        duration = t.FormatInt(str(row['duration']))
        extra = str(row['extra'])
        status = t.FormatString(str(row['status']))
        called_at = t.FormatString(str(row['called_at']))

        if record is not None and not pd.isnull(record) and record != "":
            insert_row = [record, duration, extra, status, called_at]
            array.append(tuple(insert_row))
           
    print("- Total records to upload: " + str(len(array)))

    # Upload records to DB
    try:
        query = """
            INSERT INTO transactions_pre (record, duration, extra, status, called_at)
            VALUES (%s, %s, %s, %s, %s)
        """
        cursor.executemany(query, array)
        db.commit()

    except mysql.connector.Error as e:
        print("- Error to upload records: " + e.msg)
