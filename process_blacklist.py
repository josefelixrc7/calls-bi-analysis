
import pandas as pd
import mysql.connector

from connection import Connection
import tools as t

def UploadBacklist(csv_file):

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

def ProcessBacklist():

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

def ExcludeBacklist():

    print("- Exclude Blacklist")

    # DB connection
    conn = Connection('localhost', 'root', '0UHC72zNvywZ', 'catBI')
    db = conn.Connect_()
    cursor = db.cursor()

    # Upload records to DB
    try:
        # Create segment
        cursor.execute("INSERT INTO segments (name) VALUES ('blacklist')")
        db.commit()
        id_segment = cursor._last_insert_id

        # JOIN and exclude records
        cursor.execute(
        """
            INSERT INTO segments_records(id_record, id_segment)
            SELECT rb.id_record, """ + str(id_segment) + """
            FROM records_blacklist rb
            LEFT JOIN segments_records sr ON sr.id_record = rb.id_record
            WHERE
                sr.id_record IS NULL
        """)
        db.commit()

    except mysql.connector.Error as e:
        print("- Error to exclude blacklist: " + e.msg)
