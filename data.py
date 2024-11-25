
import pandas as pd
import mysql.connector

from connection import Connection
import tools as t

class Data:

    def Download(self, conn, query, file, columns):

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
            print("- Error to Download Data: " + e.msg)

        cursor.close()

    def UploadBacklist(self, csv_file):

        print("- Upload Blacklist: " + csv_file)

        # DB connection
        conn = Connection('localhost', 'root', '0UHC72zNvywZ', 'catBI')
        db = conn.Connect_()
        cursor = db.cursor()

        # Read file
        print("- Reading file")
        data = pd.read_csv(csv_file, sep = "\t")

        # Truncate records_blacklist_pre
        cursor.execute("TRUNCATE TABLE records_blacklist_pre")
        db.commit()

        # Iterate over CSV records
        array = []
        for index, row in data.iterrows():

            # Values
            record = t.FormatString(str(row['DN']))

            if record is not None and not pd.isnull(record) and record != "":
                insert_row = [record]
                array.append(tuple(insert_row))
            
        print("- Total blacklist records to process: " + str(len(array)))

        # Upload records to DB
        try:
            query = """INSERT INTO records_blacklist_pre (record) VALUES (%s)"""
            cursor.executemany(query, array)
            db.commit()

        except mysql.connector.Error as e:
            print("- Error to upload blacklist: " + e.msg)

    def ProcessBacklist(self):

        print("- Process Blacklist")

        # DB connection
        conn = Connection('localhost', 'root', '0UHC72zNvywZ', 'catBI')
        db = conn.Connect_()
        cursor = db.cursor()

        try:
            # Truncate blacklist records
            cursor.execute("TRUNCATE TABLE records_blacklist")
            db.commit()

            # Process blacklist records
            cursor.execute("""
                INSERT INTO records_blacklist (id_record)
                SELECT r.id
                FROM records r
                JOIN records_blacklist_pre pre ON pre.record = r.record         
            """)
            db.commit()

        except mysql.connector.Error as e:
            print("- Error to process blacklist: " + e.msg)
