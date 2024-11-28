
import pandas as pd
import mysql.connector

from connection import Connection
import tools as t

class Analysis:

    def __init__(self):
        self.conn = Connection('localhost', 'root', '0UHC72zNvywZ', 'catBI')
        self.db = self.conn.Connect_()
        self.cursor = self.db.cursor()

    def Export(self):

        print("- Export Analysis")

        try:
            # Segments
            self.cursor.execute("SELECT id, name FROM segments WHERE type = 'analysis'")
            segments = self.cursor.fetchall()

            # Iterate over segments
            for it in segments:
                    
                print("-- Export: " + it[1])

                # Get analysis
                self.cursor.execute("""
                    SELECT
                        r.record AS phone_number
                        ,n.region AS region
                        ,n.state AS estado
                        ,n.city AS plaza
                        ,IFNULL(JSON_EXTRACT(ri.info, '$.client'), "") AS cliente
                        ,IFNULL(JSON_EXTRACT(ri.info, '$.curp'), "") AS curp
                    FROM segments s
                    JOIN segments_records sr ON sr.id_segment = s.id 
                    JOIN records r ON r.id = sr.id_record 
                    JOIN records_info ri ON ri.id_record = r.id 
                    JOIN nirs n ON n.nir = SUBSTRING(r.record, 1, 3)
                    WHERE s.id = """ + str(it[0]) + """
                    ORDER BY RAND()
                """)
                analysis = self.cursor.fetchall()

                # Save to CSV
                filename = "~/" + str(it[1]) + ".csv"
                columns = ['phone_number','region','estado','plaza','cliente','curp']
                df = pd.DataFrame(analysis, columns=columns, dtype='string')
                df.to_csv(filename, index=False, sep="\t", header=columns)

        except mysql.connector.Error as e:
            print("- Error to Export Analysis: " + e.msg)

        self.cursor.close()

    def AnalysisCentroForaneo(self):

        print("- AnalysisCentroForaneo")

        try:
            # Truncate records_preselected
            self.cursor.execute("TRUNCATE TABLE records_selected")
            self.db.commit()

            # Insert records
            self.cursor.execute(
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
                    AND n.region = 'Centro Foráneo'
                GROUP BY r.id
            """)
            self.db.commit()

        except mysql.connector.Error as e:
            print("- Error to make AnalysisCentroForaneo: " + e.msg)

    def AnalysisCentroMetropolitano(self):

        print("- AnalysisCentroMetropolitano")

        try:
            # Truncate records_preselected
            self.cursor.execute("TRUNCATE TABLE records_selected")
            self.db.commit()

            # Insert records
            self.cursor.execute(
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
                    AND n.region = 'Centro Metropolitano'
                GROUP BY r.id
            """)
            self.db.commit()

        except mysql.connector.Error as e:
            print("- Error to make AnalysisCentroMetropolitano: " + e.msg)

    def AnalysisNoreste(self):

        print("- AnalysisNoreste")

        try:
            # Truncate records_preselected
            self.cursor.execute("TRUNCATE TABLE records_selected")
            self.db.commit()

            # Insert records
            self.cursor.execute(
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
                    AND n.region = 'Noreste'
                GROUP BY r.id
            """)
            self.db.commit()

        except mysql.connector.Error as e:
            print("- Error to make AnalysisNoreste: " + e.msg)

    def AnalysisNoroeste(self):

        print("- AnalysisNoroeste")

        try:
            # Truncate records_preselected
            self.cursor.execute("TRUNCATE TABLE records_selected")
            self.db.commit()

            # Insert records
            self.cursor.execute(
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
                    AND n.region = 'Noroeste'
                GROUP BY r.id
            """)
            self.db.commit()

        except mysql.connector.Error as e:
            print("- Error to make AnalysisNoroeste: " + e.msg)

    def AnalysisOccidente(self):

        print("- AnalysisOccidente")

        try:
            # Truncate records_preselected
            self.cursor.execute("TRUNCATE TABLE records_selected")
            self.db.commit()

            # Insert records
            self.cursor.execute(
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
                    AND n.region = 'Occidente'
                GROUP BY r.id
            """)
            self.db.commit()

        except mysql.connector.Error as e:
            print("- Error to make AnalysisOccidente: " + e.msg)

    def AnalysisSur(self):

        print("- AnalysisSur")

        try:
            # Truncate records_selected
            self.cursor.execute("TRUNCATE TABLE records_selected")
            self.db.commit()

            # Insert records
            self.cursor.execute(
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
                    AND n.region = 'Sur'
                GROUP BY r.id
            """)
            self.db.commit()

        except mysql.connector.Error as e:
            print("- Error to make AnalysisSur: " + e.msg)

    def AnalysisSureste(self):

        print("- AnalysisSureste")

        try:
            # Truncate records_preselected
            self.cursor.execute("TRUNCATE TABLE records_selected")
            self.db.commit()

            # Insert records
            self.cursor.execute(
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
                    AND n.region = 'Sureste'
                GROUP BY r.id
            """)
            self.db.commit()

        except mysql.connector.Error as e:
            print("- Error to make AnalysisSureste: " + e.msg)
