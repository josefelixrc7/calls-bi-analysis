
import mysql.connector
import pandas as pd

from connection import Connection

def ExportAnalysis():

    print("- Export Analysis")

    # DB connection
    conn = Connection('localhost', 'root', '0UHC72zNvywZ', 'catBI')
    db = conn.Connect_()
    cursor = db.cursor()

    try:
        # Segments
        cursor.execute("SELECT id, name FROM segments WHERE type = 'analysis'")
        segments = cursor.fetchall()

        # Iterate over segments
        for it in segments:
                
            print("-- Export: " + it[1])

            # Get analysis
            cursor.execute("""
                SELECT
                    '52' AS phone_code
                    ,r.record AS phone_number
                    ,n.region AS region
                    ,n.state AS estado
                    ,n.city AS plaza
                    ,IFNULL(JSON_EXTRACT(ri.info, '$.client'), "") AS cliente
                    ,IFNULL(JSON_EXTRACT(ri.info, '$.curp'), "") AS curp
                FROM segments s
                JOIN segments_records sr ON sr.id_segment = s.id 
                JOIN records r ON r.id = sr.id_record 
                LEFT JOIN records_info ri ON ri.id_record = r.id 
                JOIN nirs n ON n.nir = SUBSTRING(r.record, 1, 3)
                WHERE s.id = """ + str(it[0]) + """
                ORDER BY RAND()
            """)
            analysis = cursor.fetchall()

            # Save to CSV
            filename = "~/" + str(it[1]) + ".csv"
            columns = ['phone_code','phone_number','region','estado','plaza','cliente','curp']
            df = pd.DataFrame(analysis, columns=columns, dtype='string')
            df.to_csv(filename, index=False, sep="\t", header=columns)

    except mysql.connector.Error as e:
        print("- Error to Export Analysis: " + e.msg)

    cursor.close()
