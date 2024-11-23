
import pandas as pd
import mysql.connector

from connection import Connection
import tools as t

def UploadRecords(csv_file):

    print("- Upload Records: " + csv_file)

    # DB connection
    conn = Connection('localhost', 'root', '0UHC72zNvywZ', 'catBI')
    db = conn.Connect_()
    cursor = db.cursor()

    try:

        # Read file
        print("- Reading file")
        data = pd.read_csv(csv_file, sep = "\t", dtype={"record": "string"})

        # Truncate transactions_pre
        cursor.execute("TRUNCATE TABLE records_pre")
        db.commit()

        # Iterate over CSV records
        array = []
        for index, row in data.iterrows():

            # Values
            record = t.FormatString(str(row['record']))

            if record is not None and not pd.isnull(record) and record != "":
                insert_row = [record]
                array.append(tuple(insert_row))
            
        print("- Total records to upload: " + str(len(array)))

        # Upload records to DB
        query = """
            INSERT INTO records_pre (record)
            VALUES (%s)
        """
        cursor.executemany(query, array)
        db.commit()

        # Add new records
        query = """
            INSERT INTO records (record)
            SELECT pre.record
            FROM records_pre pre
            LEFT JOIN records r ON r.record = pre.record
            WHERE r.record IS NULL
        """
        cursor.execute(query)
        db.commit()

    except mysql.connector.Error as e:
        print("- Error to upload records: " + e.msg)
