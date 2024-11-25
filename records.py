
import pandas as pd
import mysql.connector

from connection import Connection
import tools as t

class Records:

    def __init__(self):
        self.conn = Connection('localhost', 'root', '0UHC72zNvywZ', 'catBI')
        self.db = self.conn.Connect_()
        self.cursor = self.db.cursor()

    def Add(self, csv_file, database_type, source):

        print("- Add Records: " + csv_file)

        try:

            # Read file
            data = pd.read_csv(csv_file, sep = "\t", dtype="string")

            # Truncate transactions_pre
            self.cursor.execute("TRUNCATE TABLE records_pre")
            self.db.commit()

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
            self.cursor.execute("""
                INSERT INTO `databases` (name, id_database_type)
                SELECT
                    CONCAT('DB_', '""" + source + """', '_', '""" + str(len(array)) + """')
                    ,id
                FROM databases_types
                WHERE name = '""" + database_type + """'
            """)
            self.db.commit()
            id_database = self.cursor._last_insert_id

            # Upload records to DB
            query = """
                INSERT INTO records_pre (record)
                VALUES (%s)
            """
            self.cursor.executemany(query, array)
            self.db.commit()

            # Add new records
            query = """
                INSERT INTO records (record, id_database)
                SELECT pre.record, """ + str(id_database) + """
                FROM records_pre pre
            """
            self.cursor.execute(query)
            self.db.commit()

        except mysql.connector.Error as e:
            print("- Error to Add records: " + e.msg)

    def UpdateInfo(self, csv_file):

        print("- Records: UpdateInfo: " + csv_file)

        try:

            # Read file
            data = pd.read_csv(csv_file, sep = ",", dtype="string")

            # Truncate transactions_pre
            self.cursor.execute("TRUNCATE TABLE records_info_pre")
            self.db.commit()

            # Iterate over CSV records
            array = []
            for index, row in data.iterrows():

                # Values
                record = str(t.FormatIntUnsigned(str(row['record'])))
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
            self.cursor.executemany(query, array)
            self.db.commit()

            # Add new records
            query = """
                INSERT INTO records (record, id_database)
                SELECT pre.record, 3
                FROM records_info_pre pre
                LEFT JOIN records r ON r.record = pre.record
                WHERE
                    r.record IS NULL
                    AND CHAR_LENGTH(pre.record) = 10
            """
            self.cursor.execute(query)
            self.db.commit()

            # Add new records_info
            query = """
                INSERT INTO records_info (id_record)
                SELECT r.id
                FROM records r
                LEFT JOIN records_info ri ON r.id_record = r.id
                WHERE r.id_record IS NULL
            """
            self.cursor.execute(query)
            self.db.commit()

            # Update info
            query = """
                UPDATE records_info ri
                JOIN records r ON r.id = ri.id_record
                JOIN records_info_pre pre ON pre.record = r.record
                SET
                    ri.info = CONCAT('{"client": "', pre.client, '", "curp": "', pre.curp, '"}')
            """
            self.cursor.execute(query)
            self.db.commit()

        except mysql.connector.Error as e:
            print("- Error to Records: UpdateInfo: " + e.msg)
