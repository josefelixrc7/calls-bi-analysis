
import mysql.connector

from connection import Connection

def CleanSegments():

    print("- Clean segments")

    # DB connection
    conn = Connection('localhost', 'root', '0UHC72zNvywZ', 'catBI')
    db = conn.Connect_()
    cursor = db.cursor()

    try:

        # Truncate segments_records
        cursor.execute("TRUNCATE TABLE segments_records")
        db.commit()

        # Delete segments
        cursor.execute("DELETE FROM segments")
        db.commit()

    except mysql.connector.Error as e:
        print("- Error to clean segments: " + e.msg)

def DeleteSegment(segment):

    print("- Delete segment: " + segment)

    # DB connection
    conn = Connection('localhost', 'root', '0UHC72zNvywZ', 'catBI')
    db = conn.Connect_()
    cursor = db.cursor()

    try:

        # Create segment
        cursor.execute("DELETE FROM segments WHERE name = '" + segment + "'")
        db.commit()

    except mysql.connector.Error as e:
        print("- Error to delete segment: " + e.msg)

def ShowSegments():

    print("- Show segments")

    # DB connection
    conn = Connection('localhost', 'root', '0UHC72zNvywZ', 'catBI')
    db = conn.Connect_()
    cursor = db.cursor()

    try:

        # Create segment
        cursor.execute("""
            SELECT s.id, s.type, s.name, COUNT(sr.id)
            FROM segments s
            LEFT JOIN segments_records sr ON sr.id_segment = s.id
            GROUP BY s.id
        """)
        results = cursor.fetchall()
        for row in results:
            print("- ID: " + str(row[0]) + ", Segment: " + str(row[2]) + "(" + str(row[1]) + "), Records: " + str(row[3]))

    except mysql.connector.Error as e:
        print("- Error to show segments: " + e.msg)

def CleanDatabasesUsed():

    print("- CleanDatabasesUsed")

    # DB connection
    conn = Connection('localhost', 'root', '0UHC72zNvywZ', 'catBI')
    db = conn.Connect_()
    cursor = db.cursor()

    try:

        # Truncate
        cursor.execute("TRUNCATE TABLE segments_databases")
        db.commit()

    except mysql.connector.Error as e:
        print("- Error to CleanDatabasesUsed: " + e.msg)

def UseDatabase(database):

    print("- UseDatabase")

    # DB connection
    conn = Connection('localhost', 'root', '0UHC72zNvywZ', 'catBI')
    db = conn.Connect_()
    cursor = db.cursor()

    try:

        # Add databases to use
        cursor.execute("""
            INSERT INTO segments_databases (id_database)
            SELECT id
            FROM `databases`
            WHERE id = """ + str(database) + """
        """)
        db.commit()

    except mysql.connector.Error as e:
        print("- Error to UseDatabase: " + e.msg)

def UseDatabaseType(database):

    print("- UseDatabaseType")

    # DB connection
    conn = Connection('localhost', 'root', '0UHC72zNvywZ', 'catBI')
    db = conn.Connect_()
    cursor = db.cursor()

    try:

        # Add databases to use
        cursor.execute("""
            INSERT INTO segments_databases (id_database)
            SELECT db.id
            FROM `databases` db
            JOIN databases_types dt ON dt.id = db.id_database_type
            WHERE dt.name = '""" + str(database) + """'
        """)
        db.commit()

    except mysql.connector.Error as e:
        print("- Error to UseDatabaseType: " + e.msg)

def ShowDatabasesUsed():

    print("- ShowDatabasesUsed")

    # DB connection
    conn = Connection('localhost', 'root', '0UHC72zNvywZ', 'catBI')
    db = conn.Connect_()
    cursor = db.cursor()

    try:

        # Add databases to use
        cursor.execute("""
            SELECT db.id AS 'ID', db.name AS 'DB'
            FROM `databases` db
            JOIN segments_databases sd ON sd.id_database = db.id
        """)
        results = cursor.fetchall()
        print(results)

    except mysql.connector.Error as e:
        print("- Error to ShowDatabasesUsed: " + e.msg)

def CreateSegment(cantity, segment_name):

    print("- Create Segment: " + segment_name + " (" + str(cantity) + ")")

    # DB connection
    conn = Connection('localhost', 'root', '0UHC72zNvywZ', 'catBI')
    db = conn.Connect_()
    cursor = db.cursor()

    try:

        # Celing nirs parts
        cursor.execute("TRUNCATE TABLE segments_nirs_distribution")
        db.commit()

        if cantity <= 1:
            cursor.execute("""
                INSERT INTO segments_nirs_distribution (id_nir, parts)
                SELECT 
                    ni.id
                    ,CEILING(COUNT(1) * """ + str(cantity) + """)
                FROM records_selected rs
                JOIN nirs ni ON ni.id = rs.id_nir
                GROUP BY ni.nir
            """)
            db.commit()
        else:
            cursor.execute("""
                INSERT INTO segments_nirs_distribution (id_nir, parts)
                SELECT 
                    ni.id
                    ,CEILING(COUNT(1) * (SELECT """ + str(cantity) + """ / NULLIF(COUNT(1), 0) FROM records_selected))
                FROM records_selected rs
                JOIN nirs ni ON ni.id = rs.id_nir
                GROUP BY ni.nir
            """)
            db.commit()

        # Row enumeration
        cursor.execute("TRUNCATE TABLE segments_row_enumeration")
        db.commit()

        cursor.execute("""
            INSERT INTO segments_row_enumeration (id_record, id_nir, parts, row)
            SELECT
                rs.id_record
                ,q.id_nir
                ,q.parts 
                ,ROW_NUMBER() OVER (PARTITION BY q.id_nir ORDER BY q.id_nir ASC) 
            FROM records_selected rs
            JOIN segments_nirs_distribution q ON q.id_nir = rs.id_nir
        """)
        db.commit()

        # Create the segment
        cursor.execute("INSERT INTO segments (name, type) VALUES ('" + segment_name + "', 'analysis')")
        db.commit()
        id_segment = cursor._last_insert_id

        # Segment records
        cursor.execute(
        """
            INSERT INTO segments_records (id_segment, id_record)
            SELECT
                """ + str(id_segment) + """
                ,rs.id_record 
            FROM records_selected rs
            JOIN segments_row_enumeration sre ON sre.id_record = rs.id_record
            WHERE
                sre.parts >= sre.row
            ORDER BY sre.id_nir, sre.row
        """)
        db.commit()

        # Delete segmented records from records_selected
        cursor.execute(
        """
            DELETE rs FROM records_selected rs 
            JOIN segments_records sr ON sr.id_record = rs.id_record 
            WHERE sr.id_segment = """ + str(id_segment) + """
        """)
        db.commit()


    except mysql.connector.Error as e:
        print("- Error to Create Segment: " + e.msg)

def SegmentLeft():

    print("- Segment Left")

    # DB connection
    conn = Connection('localhost', 'root', '0UHC72zNvywZ', 'catBI')
    db = conn.Connect_()
    cursor = db.cursor()

    try:

        # Truncate records_preselected
        cursor.execute("TRUNCATE TABLE records_selected")
        db.commit()

        # Insert records
        cursor.execute(
        """
            INSERT INTO records_selected(id_record, id_nir)
            SELECT r.id, n.id
            FROM databases_records dr
            JOIN records r ON r.id = dr.id_record
            JOIN segments_databases sd ON sd.id_database = dr.id_database
            LEFT JOIN segments_records sr ON sr.id_record = r.id
            JOIN nirs n ON n.nir = SUBSTRING(r.record, 1, 3)
            WHERE
                sr.id_record IS NULL
            GROUP BY r.id
        """)
        db.commit()

    except mysql.connector.Error as e:
        print("- Error to Segment Left: " + e.msg)
