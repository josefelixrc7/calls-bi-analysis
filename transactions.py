
import pandas as pd
import mysql.connector

from connection import Connection
import tools as t
import records as r

class Transactions:

    def __init__(self):
        self.conn = Connection('localhost', 'root', '0UHC72zNvywZ', 'catBI')
        self.db = self.conn.Connect_()
        self.cursor = self.db.cursor()

    def Upload(self, csv_file):

        print("- Upload Transactions: " + csv_file)

        # Read file
        data = pd.read_csv(csv_file, sep = "\t", dtype={
            "record": "string"
            ,"duration": float
            ,"status": "string"
            ,"called_at": "string"
            ,"user": "string"
            ,"campaign_id": "string"
            ,"list_id": int
        })

        # Truncate transactions_pre
        self.cursor.execute("TRUNCATE TABLE transactions_pre")
        self.db.commit()

        # Iterate over CSV transactions
        array = []
        for index, row in data.iterrows():

            # Values
            record = t.FormatString(str(row['record']))
            duration = t.FormatInt(str(row['duration']))
            status = t.FormatString(str(row['status']))
            called_at = t.FormatString(str(row['called_at']))
            user = t.FormatString(str(row['user']))
            campaign_id = t.FormatString(str(row['campaign_id']))
            list_id = t.FormatInt(str(row['list_id']))

            if record is not None and not pd.isnull(record) and record != "":
                insert_row = [record, duration, status, called_at, user, campaign_id, list_id]
                array.append(tuple(insert_row))
            
        print("- Total transactions to upload: " + str(len(array)))

        # Upload transactions to DB
        try:
            query = """
                INSERT INTO transactions_pre (record, duration, status, called_at, user, campaign_id, list_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            self.cursor.executemany(query, array)
            self.db.commit()

        except mysql.connector.Error as e:
            print("- Error to Upload Transactions: " + e.msg)

    def Add(self):

        print("- Add transactions")

        try:

            # Add new records to Referidos DB
            records = r.Records()
            records.AddTo("INSERT INTO records_pre (record) SELECT DISTINCT record FROM transactions_pre", 3)

            # Update statuses prefix
            """self.cursor.execute("UPDATE transactions_pre SET status = CONCAT('BP_', status)")
            self.db.commit()"""

            # Add transactions
            self.cursor.execute("""
                INSERT INTO transactions (id_record, duration, extra, called_at, id_status, id_nir)
                SELECT
                    r.id
                    ,pre.duration
                    ,CONCAT('{"user":"', pre.user,'","campaign_id":"', pre.campaign_id,'","list_id":"', pre.list_id,'"}')
                    ,pre.called_at
                    ,s.id
                    ,n.id
                FROM transactions_pre pre
                JOIN records r ON r.record = pre.record
                JOIN statuses s ON s.status = pre.status
                JOIN nirs n ON n.nir = SUBSTRING(pre.record, 1, 3)
            """)
            self.db.commit()

        except mysql.connector.Error as e:
            print("- Error to Add Transactions: " + e.msg)
