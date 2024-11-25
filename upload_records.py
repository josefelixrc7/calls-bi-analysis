
import pandas as pd
import mysql.connector

from connection import Connection
import tools as t

def UploadRecords(csv_file, database_type):

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

        # Create new DB
        cursor.execute("""
            INSERT INTO databases (name, type)
            SELECT CONCAT('DB_', name, '_', '""" + str(len(array)) + """'), id
            FROM databases_types
            WHERE name = '""" + database_type + """'
        """)
        db.commit()
        id_database = cursor._last_insert_id

        # Upload records to DB
        query = """
            INSERT INTO records_pre (record)
            VALUES (%s)
        """
        cursor.executemany(query, array)
        db.commit()

        # Add new records
        query = """
            INSERT INTO records (record, id_database)
            SELECT pre.record, """ + str(id_database) + """
            FROM records_pre pre
        """
        cursor.execute(query)
        db.commit()

    except mysql.connector.Error as e:
        print("- Error to upload records: " + e.msg)
