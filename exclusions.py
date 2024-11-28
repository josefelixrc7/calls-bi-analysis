
import mysql.connector

from connection import Connection

def ExcludeBacklist():

    print("- Exclude Blacklist")

    # DB connection
    conn = Connection('localhost', 'root', '0UHC72zNvywZ', 'catBI')
    db = conn.Connect_()
    cursor = db.cursor()

    # Upload records to DB
    try:
        # Create segment
        cursor.execute("INSERT INTO segments (name, type) VALUES ('blacklist', 'exclusion')")
        db.commit()
        id_segment = cursor._last_insert_id

        # JOIN and exclude records
        cursor.execute(
        """
            INSERT INTO segments_records(id_record, id_segment)
            SELECT rb.id_record, """ + str(id_segment) + """
            FROM records_blacklist rb
            LEFT JOIN segments_records sr ON sr.id_record = rb.id_record
            JOIN databases_records dr ON dr.id_record = rb.id_record
            JOIN segments_databases sd ON sd.id_database = dr.id_database
            WHERE
                sr.id_record IS NULL
        """)
        db.commit()

    except mysql.connector.Error as e:
        print("- Error to exclude blacklist: " + e.msg)

def ExcludeNoDuration():

    print("- Exclude no duration records")

    # DB connection
    conn = Connection('localhost', 'root', '0UHC72zNvywZ', 'catBI')
    db = conn.Connect_()
    cursor = db.cursor()

    try:

        # Create segment
        cursor.execute("INSERT INTO segments (name, type) VALUES ('noduration', 'exclusion')")
        db.commit()
        id_segment = cursor._last_insert_id

        # JOIN and exclude records
        cursor.execute(
        """
            INSERT INTO segments_records(id_record, id_segment)
            SELECT t.id_record, """ + str(id_segment) + """
            FROM transactions t
            LEFT JOIN segments_records sr ON sr.id_record = t.id_record
            JOIN databases_records dr ON dr.id_record = t.id_record
            JOIN segments_databases sd ON sd.id_database = dr.id_database
            WHERE
                sr.id_record IS NULL
                AND t.called_at >= NOW() - INTERVAL 1 MONTH
            GROUP BY t.id_record
            HAVING MAX(t.duration) = 0
        """)
        db.commit()

    except mysql.connector.Error as e:
        print("- Error to Exclude no duration records: " + e.msg)

def ExcludeNoreusable():

    print("- Exclude no reusable records")

    # DB connection
    conn = Connection('localhost', 'root', '0UHC72zNvywZ', 'catBI')
    db = conn.Connect_()
    cursor = db.cursor()

    try:

        # Create segment
        cursor.execute("INSERT INTO segments (name, type) VALUES ('noreusable', 'exclusion')")
        db.commit()
        id_segment = cursor._last_insert_id

        # JOIN and exclude records
        cursor.execute(
        """
            INSERT INTO segments_records(id_record, id_segment)
            SELECT t.id_record, """ + str(id_segment) + """
            FROM transactions t
            LEFT JOIN segments_records sr ON sr.id_record = t.id_record
            JOIN statuses s ON s.id = t.id_status
            JOIN databases_records dr ON dr.id_record = t.id_record
            JOIN segments_databases sd ON sd.id_database = dr.id_database
            WHERE
                sr.id_record IS NULL
                AND s.reusable = 0
                AND t.called_at >= NOW() - INTERVAL 7 DAY
            GROUP BY t.id_record
        """)
        db.commit()

    except mysql.connector.Error as e:
        print("- Error to Exclude no reusable records: " + e.msg)

def ExcludeOverused():

    print("- Exclude overused records")

    # DB connection
    conn = Connection('localhost', 'root', '0UHC72zNvywZ', 'catBI')
    db = conn.Connect_()
    cursor = db.cursor()

    try:

        # Create segment
        cursor.execute("INSERT INTO segments (name, type) VALUES ('overused', 'exclusion')")
        db.commit()
        id_segment = cursor._last_insert_id

        # JOIN and exclude records
        cursor.execute(
        """
            INSERT INTO segments_records(id_record, id_segment)
            SELECT t.id_record, """ + str(id_segment) + """
            FROM transactions t
            LEFT JOIN segments_records sr ON sr.id_record = t.id_record
            JOIN databases_records dr ON dr.id_record = t.id_record
            JOIN segments_databases sd ON sd.id_database = dr.id_database
            WHERE
                sr.id_record IS NULL
                AND t.called_at >= NOW() - INTERVAL 1 MONTH
                AND t.duration >= 20
            GROUP BY t.id_record
            HAVING COUNT(t.id) >= 6
        """)
        db.commit()

    except mysql.connector.Error as e:
        print("- Error to Exclude overused records: " + e.msg)

def ExcludeSales():

    print("- Exclude sales records")

    # DB connection
    conn = Connection('localhost', 'root', '0UHC72zNvywZ', 'catBI')
    db = conn.Connect_()
    cursor = db.cursor()

    try:

        # Create segment
        cursor.execute("INSERT INTO segments (name, type) VALUES ('sales', 'exclusion')")
        db.commit()
        id_segment = cursor._last_insert_id

        # JOIN and exclude records
        cursor.execute(
        """
            INSERT INTO segments_records(id_record, id_segment)
            SELECT t.id_record, """ + str(id_segment) + """
            FROM transactions t
            LEFT JOIN segments_records sr ON sr.id_record = t.id_record
            JOIN statuses s ON s.id = t.id_status
            JOIN databases_records dr ON dr.id_record = t.id_record
            JOIN segments_databases sd ON sd.id_database = dr.id_database
            WHERE
                sr.id_record IS NULL
                AND s.status IN ('11', '13', '600', '602', '603', 'BP_11')
                AND t.called_at >= NOW() - INTERVAL 1 MONTH
            GROUP BY t.id_record
        """)
        db.commit()

    except mysql.connector.Error as e:
        print("- Error to Exclude sales records: " + e.msg)
