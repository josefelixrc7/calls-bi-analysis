
import pandas as pd
import mysql.connector

from functions.connection import Connection
import functions.tools as t
import functions.records as r

class Transactions:

    def __init__(self):
        self.conn = Connection('localhost', 'root', '0UHC72zNvywZ', 'catBI')
        self.db = self.conn.Connect_()
        self.cursor = self.db.cursor()

    def Upload(self, csv_file, truncate = True):

        print("- Upload Transactions: " + csv_file)

        try:
                
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

            if truncate:
                # Truncate transactions_pre
                self.cursor.execute("TRUNCATE TABLE transactions_pre")
                self.db.commit()

            # Iterate over CSV transactions
            array = []
            cont = 0
            for index, row in data.iterrows():

                # Values
                record = t.FormatString(str(row['record']))
                duration = t.FormatFloat(str(row['duration']))
                status = t.FormatString(str(row['status']))
                called_at = t.FormatString(str(row['called_at']))
                user = t.FormatString(str(row['user']))
                campaign_id = t.FormatString(str(row['campaign_id']))
                list_id = t.FormatInt(str(row['list_id']))

                if record is not None and not pd.isnull(record) and record != "":
                    insert_row = [record, duration, status, called_at, user, campaign_id, list_id]
                    array.append(tuple(insert_row))

                # Verify a 500k batch
                cont = cont + 1
                if cont >= 500000:

                    print("- Total transactions to upload (batch): " + str(len(array)))

                    # Upload transactions to DB
                    query = """
                        INSERT INTO transactions_pre (record, duration, status, called_at, user, campaign_id, list_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """
                    self.cursor.executemany(query, array)
                    self.db.commit()
                    array = []
                    cont = 0
            
            # Upload dangling records
            print("- Total transactions to upload (last batch): " + str(len(array)))

            query = """
                INSERT INTO transactions_pre (record, duration, status, called_at, user, campaign_id, list_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            self.cursor.executemany(query, array)
            self.db.commit()

        except mysql.connector.Error as e:
            print("- Error to Upload Transactions: " + e.msg)


    def Add(self, prefix = ''):

        print("- Add transactions")

        try:

            # Add new records to Referidos DB
            records = r.Records()
            records.AddTo("INSERT INTO records_pre (record) SELECT DISTINCT record FROM transactions_pre", 3)

            # Update statuses prefix
            if prefix != '':
                self.cursor.execute("UPDATE transactions_pre SET status = CONCAT('" + prefix + "_', status)")
                self.db.commit()

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

            # More functions
            self.UpdateLast()
            self.AddRecordsToSales()

        except mysql.connector.Error as e:
            print("- Error to Add Transactions: " + e.msg)

    def UpdateLast(self):
        print("- UpdateLast")

        try:

            # Add new records to last transactions
            self.cursor.execute("""
                INSERT INTO transactions_last (id_record)
                SELECT DISTINCT r.id
                FROM records r
                LEFT JOIN transactions_last tl ON tl.id_record = r.id
                WHERE tl.id_record IS NULL
            """)
            self.db.commit()

            # Update called_at
            self.cursor.execute("""
                UPDATE transactions_last tl
                JOIN records r ON r.id = tl.id_record
                JOIN transactions_pre tp ON tp.record = r.record
                SET
                    tl.called_at = tp.called_at
                    ,tl.updated_at = NOW()
                WHERE 
                    tp.called_at > tl.called_at OR tl.called_at IS NULL
            """)
            self.db.commit()

            # Update durations
            self.cursor.execute("""
                UPDATE transactions_last tl
                JOIN records r ON r.id = tl.id_record
                JOIN transactions_pre tp ON tp.record = r.record
                SET
                    tl.duration = tp.duration
                    ,tl.updated_at = NOW()
                WHERE 
                    tp.duration > tl.duration OR tl.duration IS NULL
            """)
            self.db.commit()

        except mysql.connector.Error as e:
            print("- Error to UpdateLast: " + e.msg)

    def AddRecordsToSales(self):
        print("- AddRecordsToSales")

        try:

            self.cursor.execute("""
                INSERT INTO sales (id_record, sales_date)
                SELECT r.id, tp.called_at
                FROM transactions_pre tp
                JOIN records r ON r.record = tp.record
                JOIN statuses s ON s.status = tp.status
                WHERE 
                    s.sale = 1
            """)
            self.db.commit()

        except mysql.connector.Error as e:
            print("- Error to AddRecordsToSales: " + e.msg)
