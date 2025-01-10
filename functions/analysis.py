
import pandas as pd
import mysql.connector

from connection import Connection
import tools as t

class Analysis:

    def __init__(self):
        self.conn = Connection('localhost', 'root', '0UHC72zNvywZ', 'catBI')
        self.db = self.conn.Connect_()
        self.cursor = self.db.cursor()

    def ViewSelectedRecords(self):
        # Truncate records_preselected
        self.cursor.execute("SELECT COUNT(1) FROM records_selected")
        total = self.cursor.fetchall()
        print('- Selected records: ' + str(total[0][0]))

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
                analysis = self.cursor.fetchall()

                # Save to CSV
                filename = "~/" + str(it[1]) + ".csv"
                columns = ['phone_code', 'phone_number','region','estado','plaza','cliente','curp']
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
                FROM records_preselected rp
                JOIN records r ON r.id = rp.id_record
                LEFT JOIN segments_records sr ON sr.id_record = r.id
                JOIN nirs n ON n.nir = SUBSTRING(r.record, 1, 3)
                WHERE
                    sr.id_record IS NULL
                    AND n.region = 'Centro Foráneo'
                GROUP BY r.id
            """)
            self.db.commit()
            self.ViewSelectedRecords()

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
                FROM records_preselected rp
                JOIN records r ON r.id = rp.id_record
                LEFT JOIN segments_records sr ON sr.id_record = r.id
                JOIN nirs n ON n.nir = SUBSTRING(r.record, 1, 3)
                WHERE
                    sr.id_record IS NULL
                    AND n.region = 'Centro Metropolitano'
                GROUP BY r.id
            """)
            self.db.commit()
            self.ViewSelectedRecords()

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
                FROM records_preselected rp
                JOIN records r ON r.id = rp.id_record
                LEFT JOIN segments_records sr ON sr.id_record = r.id
                JOIN nirs n ON n.nir = SUBSTRING(r.record, 1, 3)
                WHERE
                    sr.id_record IS NULL
                    AND n.region = 'Noreste'
                GROUP BY r.id
            """)
            self.db.commit()
            self.ViewSelectedRecords()

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
                FROM records_preselected rp
                JOIN records r ON r.id = rp.id_record
                LEFT JOIN segments_records sr ON sr.id_record = r.id
                JOIN nirs n ON n.nir = SUBSTRING(r.record, 1, 3)
                WHERE
                    sr.id_record IS NULL
                    AND n.region = 'Noroeste'
                GROUP BY r.id
            """)
            self.db.commit()
            self.ViewSelectedRecords()

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
                FROM records_preselected rp
                JOIN records r ON r.id = rp.id_record
                LEFT JOIN segments_records sr ON sr.id_record = r.id
                JOIN nirs n ON n.nir = SUBSTRING(r.record, 1, 3)
                WHERE
                    sr.id_record IS NULL
                    AND n.region = 'Occidente'
                GROUP BY r.id
            """)
            self.db.commit()
            self.ViewSelectedRecords()

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
                FROM records_preselected rp
                JOIN records r ON r.id = rp.id_record
                LEFT JOIN segments_records sr ON sr.id_record = r.id
                JOIN nirs n ON n.nir = SUBSTRING(r.record, 1, 3)
                WHERE
                    sr.id_record IS NULL
                    AND n.region = 'Sur'
                GROUP BY r.id
            """)
            self.db.commit()
            self.ViewSelectedRecords()

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
                FROM records_preselected rp
                JOIN records r ON r.id = rp.id_record
                LEFT JOIN segments_records sr ON sr.id_record = r.id
                JOIN nirs n ON n.nir = SUBSTRING(r.record, 1, 3)
                WHERE
                    sr.id_record IS NULL
                    AND n.region = 'Sureste'
                GROUP BY r.id
            """)
            self.db.commit()
            self.ViewSelectedRecords()

        except mysql.connector.Error as e:
            print("- Error to make AnalysisSureste: " + e.msg)

    def AnalysisMinutos1Plus(self):

        print("- AnalysisMinutos1Plus")

        try:
            # Truncate records_preselected
            self.cursor.execute("TRUNCATE TABLE records_selected")
            self.db.commit()

            # Insert records
            self.cursor.execute(
            """
                INSERT INTO records_selected(id_record, id_nir)
                SELECT r.id, n.id
                FROM records_preselected rp
                JOIN records r ON r.id = rp.id_record
                JOIN transactions_last tl ON tl.id_record = r.id
                LEFT JOIN segments_records sr ON sr.id_record = r.id
                JOIN nirs n ON n.nir = SUBSTRING(r.record, 1, 3)
                WHERE
                    sr.id_record IS NULL
                    AND tl.duration >= 60
                GROUP BY r.id
            """)
            self.db.commit()
            self.ViewSelectedRecords()

        except mysql.connector.Error as e:
            print("- Error to make AnalysisMinutos1Plus: " + e.msg)

    def AnalysisEFE20(self):

        print("- AnalysisEFE20")

        try:
            # Truncate records_preselected
            self.cursor.execute("TRUNCATE TABLE records_selected")
            self.db.commit()

            # Insert records
            self.cursor.execute(
            """
                INSERT INTO records_selected(id_record, id_nir)
                SELECT r.id, n.id
                FROM records_preselected rp
                JOIN records r ON r.id = rp.id_record
                LEFT JOIN segments_records sr ON sr.id_record = r.id
                JOIN nirs n ON n.nir = SUBSTRING(r.record, 1, 3)
                WHERE
                    sr.id_record IS NULL
                    AND n.nir IN ('999','663','712','642','744','618','818','554'
                        ,'664','322','662','644','246','331','272','646','686','562','443','312'
                    )
                GROUP BY r.id
            """)
            self.db.commit()
            self.ViewSelectedRecords()

        except mysql.connector.Error as e:
            print("- Error to make AnalysisEFE20: " + e.msg)

    def AnalysisMN10(self):

        print("- AnalysisMN10")

        try:
            # Truncate best nirs
            self.cursor.execute("TRUNCATE TABLE best_nirs")
            self.db.commit()

            # Update best nirs
            self.cursor.execute(
            """
                INSERT INTO best_nirs(id_nir)
                SELECT s.id_nir
                FROM (
                    SELECT
                        t.id_nir AS id_nir
                        ,COUNT(1) AS transfered
                        ,SUM(IF(st.sale = 1, 1, 0)) AS sales
                    FROM transactions t
                    JOIN statuses st ON st.id = t.id_status
                    WHERE
                        t.called_at >= NOW() - INTERVAL 1 MONTH
                        AND st.contact = 1
                    GROUP BY t.id_nir
                ) s
                WHERE
                    s.transfered >= 1000
                    AND s.sales > 0
                ORDER BY (s.transfered / s.sales) ASC
                LIMIT 10
            """)
            self.db.commit()

            # Truncate records_preselected
            self.cursor.execute("TRUNCATE TABLE records_selected")
            self.db.commit()

            # Insert records
            self.cursor.execute(
            """
                INSERT INTO records_selected(id_record, id_nir)
                SELECT r.id, n.id
                FROM records_preselected rp
                JOIN records r ON r.id = rp.id_record
                LEFT JOIN segments_records sr ON sr.id_record = r.id
                JOIN nirs n ON n.nir = SUBSTRING(r.record, 1, 3)
                JOIN best_nirs bn ON bn.id_nir = n.id
                WHERE
                    sr.id_record IS NULL
                GROUP BY r.id
            """)
            self.db.commit()
            self.ViewSelectedRecords()

        except mysql.connector.Error as e:
            print("- Error to make AnalysisMN10: " + e.msg)

    def AnalysisReferidos(self):

        print("- AnalysisReferidos")

        try:
            # Truncate records_preselected
            self.cursor.execute("TRUNCATE TABLE records_selected")
            self.db.commit()

            # Insert records
            self.cursor.execute(
            """
                INSERT INTO records_selected(id_record, id_nir)
                SELECT r.id, n.id
                FROM records_preselected rp
                JOIN records r ON r.id = rp.id_record
                LEFT JOIN segments_records sr ON sr.id_record = r.id
                JOIN nirs n ON n.nir = SUBSTRING(r.record, 1, 3)
                JOIN (
                    SELECT DISTINCT t.id_record AS id_record
                    FROM transactions t
                    JOIN statuses s ON s.id = t.id_status
                    WHERE s.referred = 1
                ) j ON j.id_record = r.id
                WHERE
                    sr.id_record IS NULL
                GROUP BY r.id
            """)
            self.db.commit()
            self.ViewSelectedRecords()

        except mysql.connector.Error as e:
            print("- Error to make AnalysisReferidos: " + e.msg)

    def AnalysisBajaCalifornia(self):

        print("- AnalysisBajaCalifornia")

        try:
            # Truncate records_preselected
            self.cursor.execute("TRUNCATE TABLE records_selected")
            self.db.commit()

            # Insert records
            self.cursor.execute(
            """
                INSERT INTO records_selected(id_record, id_nir)
                SELECT r.id, n.id
                FROM records_preselected rp
                JOIN records r ON r.id = rp.id_record
                LEFT JOIN segments_records sr ON sr.id_record = r.id
                JOIN nirs n ON n.nir = SUBSTRING(r.record, 1, 3)
                WHERE
                    sr.id_record IS NULL
                    AND n.state = 'BAJA CALIFORNIA'
                GROUP BY r.id
            """)
            self.db.commit()
            self.ViewSelectedRecords()

        except mysql.connector.Error as e:
            print("- Error to make AnalysisBajaCalifornia: " + e.msg)

    def AnalysisJalisco(self):

        print("- AnalysisJalisco")

        try:
            # Truncate records_preselected
            self.cursor.execute("TRUNCATE TABLE records_selected")
            self.db.commit()

            # Insert records
            self.cursor.execute(
            """
                INSERT INTO records_selected(id_record, id_nir)
                SELECT r.id, n.id
                FROM records_preselected rp
                JOIN records r ON r.id = rp.id_record
                LEFT JOIN segments_records sr ON sr.id_record = r.id
                JOIN nirs n ON n.nir = SUBSTRING(r.record, 1, 3)
                WHERE
                    sr.id_record IS NULL
                    AND n.state = 'JALISCO'
                GROUP BY r.id
            """)
            self.db.commit()
            self.ViewSelectedRecords()

        except mysql.connector.Error as e:
            print("- Error to make AnalysisJalisco: " + e.msg)
