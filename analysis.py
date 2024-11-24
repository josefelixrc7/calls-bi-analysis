
import mysql.connector

from connection import Connection

def AnalysisNoreste():

    print("- AnalysisNoreste")

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
            FROM records r
            LEFT JOIN segments_records sr ON sr.id_record = r.id
            JOIN nirs n ON n.nir = SUBSTRING(r.record, 1, 3)
            WHERE
                sr.id_record IS NULL
                AND n.region = 'Noreste'
        """)
        db.commit()

    except mysql.connector.Error as e:
        print("- Error to make AnalysisNoreste: " + e.msg)
