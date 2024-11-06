
import mysql.connector

from connection import Connection

def CleanSegments():

    print("- Clean segments")

    # DB connection
    conn = Connection('localhost', 'root', '0UHC72zNvywZ', 'catBI')
    db = conn.Connect_()
    cursor = db.cursor()

    try:

        # Create segment
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
            SELECT s.id AS 'ID', s.name AS 'Segment', COUNT(sr.id) AS 'Records'
            FROM segments s
            LEFT JOIN segments_records sr ON sr.id_segment = s.id
            GROUP BY s.id
        """)
        results = cursor.fetchall()
        for row in results:
            print("- ID: " + str(row[0]) + ", Segment: " + str(row[1]) + ", Records: " + str(row[2]))

    except mysql.connector.Error as e:
        print("- Error to show segments: " + e.msg)
