
import pandas as pd
import mysql.connector

from connection import Connection
import tools as t

class Records:

    def UpdateInfo(csv_file):

        print("- Records: UpdateInfo: " + csv_file)

        # DB connection
        conn = Connection('localhost', 'root', '0UHC72zNvywZ', 'catBI')
        db = conn.Connect_()
        cursor = db.cursor()

        try:

            # Read file
            data = pd.read_csv(csv_file, sep = "\t", dtype="string")

            # Truncate transactions_pre
            cursor.execute("TRUNCATE TABLE records_info_pre")
            db.commit()

            # Iterate over CSV records
            array = []
            for index, row in data.iterrows():

                # Values
                record = t.FormatString(str(row['record']))
                client = t.FormatString(str(row['client']))
                curp = t.FormatString(str(row['curp']))

                if record is not None and not pd.isnull(record) and record != "":
                    insert_row = [record, client, curp]
                    array.append(tuple(insert_row))
                
            print("-- Total records info to update: " + str(len(array)))

            # Upload records to DB
            query = """
                INSERT INTO records_info_pre (record, client, curp)
                VALUES (%s, %s, %s)
            """
            cursor.executemany(query, array)
            db.commit()

            # Add new records
            query = """
                INSERT INTO records (record)
                SELECT pre.record
                FROM records_info_pre pre
                LEFT JOIN records r ON r.record = pre.record
                WHERE r.record IS NULL
            """
            cursor.execute(query)
            db.commit()

            # Add new records_info
            query = """
                INSERT INTO records_info (id_record)
                SELECT r.id
                FROM records r
                LEFT JOIN records_info ri ON r.id_record = r.id
                WHERE r.id_record IS NULL
            """
            cursor.execute(query)
            db.commit()

            # Update info
            query = """
                UPDATE records_info ri
                JOIN records r ON r.id = ri.id_record
                JOIN records_info_pre pre ON pre.record = r.record
                SET
                    ri.info = CONCAT('{"client": "', pre.client, '", "curp": "', pre.curp, "}')
            """
            cursor.execute(query)
            db.commit()

        except mysql.connector.Error as e:
            print("- Error to Records: UpdateInfo: " + e.msg)
