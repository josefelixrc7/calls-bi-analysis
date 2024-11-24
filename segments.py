
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
            SELECT s.id AS 'ID', s.type AS 'Type', s.name AS 'Segment', COUNT(sr.id) AS 'Records'
            FROM segments s
            LEFT JOIN segments_records sr ON sr.id_segment = s.id
            GROUP BY s.id
        """)
        results = cursor.fetchall()
        for row in results:
            print("- ID: " + str(row[0]) + ", Segment: " + str(row[1]) + ", Records: " + str(row[2]))

    except mysql.connector.Error as e:
        print("- Error to show segments: " + e.msg)

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

        if cantity < 1:
            cursor.execute("""
                INSERT INTO segments_nirs_distribution (id_nir, parts)
                SELECT 
                    ni.id
                    ,CEILING(COUNT(1) * """ + cantity + """)
                FROM records_selected rs
                JOIN records r ON r.id = rs.id_record
                JOIN nirs ni ON ni.id = SUBSTRING(r.record, 1, 3)
                GROUP BY ni.nir
            """)
            db.commit()
        else:
            cursor.execute("""
                INSERT INTO segments_nirs_distribution (id_nir, parts)
                SELECT 
                    ,ni.id
                    ,CEILING(COUNT(1) * (SELECT """ + cantity + """ / NULLIF(COUNT(1), 0) FROM records_selected))
                FROM records_selected rs
                JOIN records r ON r.id = rs.id_record
                JOIN nirs ni ON ni.id = SUBSTRING(r.record, 1, 3)
                GROUP BY ni.nir
            """)
            db.commit()

        # Row enumeration
        cursor.execute("TRUNCATE TABLE segments_rows_enumeration")
        db.commit()

        cursor.execute("""
            INSERT INTO segments_rows_enumeration (id_record, id_nir, parts, row)
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
        cursor.execute("INSERT INTO segments (name, type) VALUES (%s, 'analysis')", (segment_name))
        db.commit()
        id_segment = cursor._last_insert_id

        # JOIN and exclude records
        cursor.execute(
        """
            INSERT INTO segments_records (id_segment, id_record)
            SELECT
                %d
                ,rs.id_record 
            FROM records_selected rs
            JOIN segments_rows_enumeration sre ON sre.id_record = rs.id_record
            WHERE
                sre.parts >= sre.row
            ORDER BY sre.id_nir, sre.row
        """, (id_segment))
        db.commit()


    except mysql.connector.Error as e:
        print("- Error to Create Segment: " + e.msg)

    """
		
	INSERT INTO administrador_registros.segmentos 
	SELECT
		NULL
		,'analisis'
		,var_nombre_segmento
		,NOW()
	;

	SET var_id_segmento = LAST_INSERT_ID(); 

	;



	DELETE rs FROM administrador_registros.registros_seleccionados rs 
	JOIN administrador_registros.segmentos_registros sr ON sr.id_registro = rs.id_registro 
	WHERE
		sr.id_segmento = var_id_segmento
	;



	DROP TABLE IF EXISTS administrador_registros.tmp_distribucion_nirs;
	DROP TABLE IF EXISTS administrador_registros.tmp_enumeracion_filas;
	
    """