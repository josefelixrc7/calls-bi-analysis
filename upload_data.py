
import pandas as pd
import mysql.connector

def FormatString(string):
    valid_characteres = "abcdefghijklmnopqrstuvwxyzABCDEF GHIJKLMNOPQRSTUVWXYZ,¡:?@*_-!1234567890"
    new_string = ''.join(c for c in string if c in valid_characteres)

    if new_string == "nan":
        return ""
    else:
        return new_string

def FormatInt(string):
    valid_characteres = "-1234567890"
    new_int = ''.join(c for c in string if c in valid_characteres)

    if new_int == "nan" or new_int == "":
        return 0
    else:
        return int(new_int)

def UploadData(csv_file):

    print("- Upload Data")

    # DB connection
    db_host = 'localhost'
    db_user = 'root'
    db_password = '0UHC72zNvywZ'
    db_name = 'catBI'
    db = mysql.connector.connect(host=db_host, user=db_user, passwd=db_password, db=db_name)
    cursor = db.cursor()

    # Read file
    print("- Reading file")
    data = pd.read_csv(csv_file, sep = ",")

    # Truncate transactions_pre
    cursor.execute("TRUNCATE TABLE transactions_pre")
    db.commit()

    # Iterate over CSV records
    array = []
    for index, row in data.iterrows():

        # Values
        record = FormatString(str(row['record']))
        duration = FormatInt(str(row['duration']))
        user = FormatString(str(row['user']))
        status = FormatString(str(row['status']))
        called_at = FormatString(str(row['called_at']))

        if record is not None and not pd.isnull(record) and record != "":
            insert_row = [record, duration, user, status, called_at]
            array.append(tuple(insert_row))
           
    print("- Total records to upload: " + str(len(array)))

    # Upload records to DB
    try:
        query = """
            INSERT INTO transactions_pre (record, duration, user, status, called_at)
            VALUES (%s, %s, %s, %s, %s)
        """
        cursor.executemany(query, array)
        db.commit()

    except mysql.connector.Error as e:
        print("- Error to upload records: " + e.msg)
