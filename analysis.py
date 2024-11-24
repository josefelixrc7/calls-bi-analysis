
import mysql.connector

from connection import Connection

# DB connection
conn = Connection('localhost', 'root', '0UHC72zNvywZ', 'catBI')
db = conn.Connect_()
cursor = db.cursor()

def AnalysisCentroForaneo():

    print("- AnalysisCentroForaneo")

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
            JOIN segments_databases sd ON sd.id_database = r.id_database
            LEFT JOIN segments_records sr ON sr.id_record = r.id
            JOIN nirs n ON n.nir = SUBSTRING(r.record, 1, 3)
            WHERE
                sr.id_record IS NULL
                AND n.region = 'Centro Foráneo'
        """)
        db.commit()

    except mysql.connector.Error as e:
        print("- Error to make AnalysisCentroForaneo: " + e.msg)

def AnalysisCentroMetropolitano():

    print("- AnalysisCentroMetropolitano")

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
            JOIN segments_databases sd ON sd.id_database = r.id_database
            LEFT JOIN segments_records sr ON sr.id_record = r.id
            JOIN nirs n ON n.nir = SUBSTRING(r.record, 1, 3)
            WHERE
                sr.id_record IS NULL
                AND n.region = 'Centro Metropolitano'
        """)
        db.commit()

    except mysql.connector.Error as e:
        print("- Error to make AnalysisCentroMetropolitano: " + e.msg)

def AnalysisNoreste():

    print("- AnalysisNoreste")

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
            JOIN segments_databases sd ON sd.id_database = r.id_database
            LEFT JOIN segments_records sr ON sr.id_record = r.id
            JOIN nirs n ON n.nir = SUBSTRING(r.record, 1, 3)
            WHERE
                sr.id_record IS NULL
                AND n.region = 'Noreste'
        """)
        db.commit()

    except mysql.connector.Error as e:
        print("- Error to make AnalysisNoreste: " + e.msg)

def AnalysisNoroeste():

    print("- AnalysisNoroeste")

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
            JOIN segments_databases sd ON sd.id_database = r.id_database
            LEFT JOIN segments_records sr ON sr.id_record = r.id
            JOIN nirs n ON n.nir = SUBSTRING(r.record, 1, 3)
            WHERE
                sr.id_record IS NULL
                AND n.region = 'Noroeste'
        """)
        db.commit()

    except mysql.connector.Error as e:
        print("- Error to make AnalysisNoroeste: " + e.msg)

def AnalysisOccidente():

    print("- AnalysisOccidente")

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
            JOIN segments_databases sd ON sd.id_database = r.id_database
            LEFT JOIN segments_records sr ON sr.id_record = r.id
            JOIN nirs n ON n.nir = SUBSTRING(r.record, 1, 3)
            WHERE
                sr.id_record IS NULL
                AND n.region = 'Occidente'
        """)
        db.commit()

    except mysql.connector.Error as e:
        print("- Error to make AnalysisOccidente: " + e.msg)

def AnalysisSur():

    print("- AnalysisSur")

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
            JOIN segments_databases sd ON sd.id_database = r.id_database
            LEFT JOIN segments_records sr ON sr.id_record = r.id
            JOIN nirs n ON n.nir = SUBSTRING(r.record, 1, 3)
            WHERE
                sr.id_record IS NULL
                AND n.region = 'Sur'
        """)
        db.commit()

    except mysql.connector.Error as e:
        print("- Error to make AnalysisSur: " + e.msg)

def AnalysisSureste():

    print("- AnalysisSureste")

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
            JOIN segments_databases sd ON sd.id_database = r.id_database
            LEFT JOIN segments_records sr ON sr.id_record = r.id
            JOIN nirs n ON n.nir = SUBSTRING(r.record, 1, 3)
            WHERE
                sr.id_record IS NULL
                AND n.region = 'Sureste'
        """)
        db.commit()

    except mysql.connector.Error as e:
        print("- Error to make AnalysisSureste: " + e.msg)
