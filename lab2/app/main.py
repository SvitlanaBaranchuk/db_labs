import psycopg2, time, csv, sys, os, logging

files = ["Odata2019File.csv", "Odata2020File.csv"]
tables = ["zno2019", "zno2020"]
transaction_size = 500


def set_tables():
    conn = psycopg2.connect(dbname="test", user="postgres", password="postgres", host="db")
    cursor = conn.cursor()
    try:
        for table in tables:
            cursor.execute(f"""
                DROP TABLE IF EXISTS {table};
                CREATE TABLE {table} (
                    out_id VARCHAR PRIMARY KEY,
                     birth VARCHAR,
                     sex VARCHAR,
                     region VARCHAR,
                     area VARCHAR,
                     tername VARCHAR,
                     reg_type VARCHAR,
                     ter_type VARCHAR,
                     class_profile VARCHAR,
                     class_lang VARCHAR,
                     EOName VARCHAR,
                     EOType VARCHAR,
                     EOReg VARCHAR,
                     EOArea VARCHAR,
                     EOTer VARCHAR,
                     EOParent VARCHAR,
                     ukr_test VARCHAR,
                     ukr_test_stat VARCHAR,
                     ukr_100 FLOAT,
                     ukr_12 FLOAT,
                     ukr_ball FLOAT,
                     ukr_adapt FLOAT,
                     ukrPTName VARCHAR,
                     ukrPTReg VARCHAR,
                     ukrPTArea VARCHAR,
                     ukrPTTer VARCHAR,
                     hist_test VARCHAR,
                     hist_lang VARCHAR,
                     hist_test_stat VARCHAR,
                     hist_100 FLOAT,
                     hist_12 FLOAT,
                     hist_ball FLOAT,
                     histPTName VARCHAR,
                     histPTReg VARCHAR,
                     histPTArea VARCHAR,
                     histPTTer VARCHAR,
                     math_test VARCHAR,
                     math_lang VARCHAR,
                     math_test_stat VARCHAR,
                     math_100 FLOAT,
                     math_12 FLOAT,
                     math_ball FLOAT,
                     mathPTName VARCHAR,
                     mathPTReg VARCHAR,
                     mathPTArea VARCHAR,
                     mathPTTer VARCHAR,
                     phys_test VARCHAR,
                     phys_lang VARCHAR,
                     phys_test_stat VARCHAR,
                     phys_100 FLOAT,
                     phys_12 FLOAT,
                     phys_ball FLOAT,
                     physPTName VARCHAR,
                     physPTReg VARCHAR,
                     physPTArea VARCHAR,
                     physPTTer VARCHAR,
                     chem_test VARCHAR,
                     chem_lang VARCHAR,
                     chem_test_stat VARCHAR,
                     chem_100 FLOAT,
                     chem_12 FLOAT,
                     chem_ball FLOAT,
                     chemPTName VARCHAR,
                     chemPTReg VARCHAR,
                     chemPTArea VARCHAR,
                     chemPTTer VARCHAR,
                     bio_test VARCHAR,
                     bio_lang VARCHAR,
                     bio_test_stat VARCHAR,
                     bio_100 FLOAT,
                     bio_12 FLOAT,
                     bio_ball FLOAT,
                     bioPTName VARCHAR,
                     bioPTReg VARCHAR,
                     bioPTArea VARCHAR,
                     bioPTTer VARCHAR,
                     geo_test VARCHAR,
                     geo_lang VARCHAR,
                     geo_test_stat VARCHAR,
                     geo_100 FLOAT,
                     geo_12 FLOAT,
                     geo_ball FLOAT,
                     geoPTName VARCHAR,
                     geoPTReg VARCHAR,
                     geoPTArea VARCHAR,
                     geoPTTer VARCHAR,
                     eng_test VARCHAR,
                     eng_test_stat VARCHAR,
                     eng_100 FLOAT,
                     eng_12 FLOAT,
                     eng_dpa VARCHAR,
                     eng_ball FLOAT,
                     engPTName VARCHAR,
                     engPTReg VARCHAR,
                     engPTArea VARCHAR,
                     engPTTer VARCHAR,
                     fra_test VARCHAR,
                     fra_test_stat VARCHAR,
                     fra_100 FLOAT,
                     fra_12 FLOAT,
                     fra_dpa VARCHAR,
                     fra_ball FLOAT,
                     fraPTName VARCHAR,
                     fraPTReg VARCHAR,
                     fraPTArea VARCHAR,
                     fraPTTer VARCHAR,
                     deu_test VARCHAR,
                     deu_test_stat VARCHAR,
                     deu_100 FLOAT,
                     deu_12 FLOAT,
                     deu_dpa VARCHAR,
                     deu_ball FLOAT,
                     deuPTName VARCHAR,
                     deuPTReg VARCHAR,
                     deuPTArea VARCHAR,
                     deuPTTer VARCHAR,
                     spa_test VARCHAR,
                     spa_test_stat VARCHAR,
                     spa_100 FLOAT,
                     spa_12 FLOAT,
                     spa_dpa VARCHAR,
                     spa_ball FLOAT,
                     spaPTName VARCHAR,
                     spaPTReg VARCHAR,
                     spaPTArea VARCHAR,
                     spaPTTer VARCHAR,
                     year VARCHAR);""")
        conn.commit()
    except psycopg2.Error:
        print("Retrying in 5 seconds...")
        time.sleep(5)
        set_tables()
    finally:
        cursor.close()
        conn.close()
    print('done set tables')



def fill_year_column():
    conn = psycopg2.connect(dbname="test", user="postgres", password="postgres", host="db")
    cursor = conn.cursor()
    try:
        cursor.execute("""UPDATE zno2019 SET year = '2019';""")
        cursor.execute("""UPDATE zno2020 SET year = '2020';""")
        conn.commit()
    except psycopg2.Error:
        print("Retrying in 5 seconds...")
        time.sleep(5)
        fill_year_column()
    finally:
        cursor.close()
        conn.close()
    print('done fill_year_column')


def db_data():
    conn = psycopg2.connect(dbname="test", user="postgres", password="postgres", host="db")
    set_tables()

    for i in range(2):
        with open(files[i], "r", encoding="cp1251") as file:
            cursor = conn.cursor()
            conn.autocommit = False
            start_line = 0
            for num_line, line in enumerate(file):
                if num_line == 0:
                    continue
                try:
                    line = line.replace('"', "").replace(',', '.').replace("null", "-1").strip()
                    values = line.split(';')

                    datatypes = ",".join(["%s"] * len(values))
                    cursor.execute(f"INSERT INTO {tables[i]} VALUES ({datatypes})", values)

                    if (num_line - start_line) % transaction_size == 0:
                        sys.stdout.write(f"{num_line} {files[i]}\n")
                        sys.stdout.flush()
                        conn.commit()
                        start_line = num_line

                except psycopg2.OperationalError:
                    while True:
                        try:
                            conn = psycopg2.connect(dbname="test", user="postgres", password="postgres", host="db")
                            cursor = conn.cursor()
                            logging.info("Connection to the database is restored.")
                            break
                        except psycopg2.OperationalError:
                            logging.error("Could not connect to the database")
                            print("Could not reconnect to the database. Retrying in 5 seconds...")
                            time.sleep(5)
                        file.seek(0)
                        for _ in range(start_line):
                            next(csv.reader(file))

                except Exception as e:
                    conn.rollback()
                    print(e)
            conn.commit()
        cursor.close()
    conn.close()

    print('done db data')


def combine_data():
    conn = psycopg2.connect(dbname="test", user="postgres", password="postgres", host="db")
    cursor = conn.cursor()
    try:
        cursor.execute("""
            DROP TABLE IF EXISTS zno;
            CREATE TABLE zno AS
            SELECT *
            FROM zno2019
            UNION ALL
            SELECT *
            FROM zno2020;
        """)
        conn.commit()
    except psycopg2.Error:
        print("Retrying in 5 seconds...")
        time.sleep(5)
        combine_data()
    finally:
        cursor.close()
        conn.close()
    print('done combine data')


# func for testing
def print_data():
    conn = psycopg2.connect(dbname="test", user="postgres", password="postgres", host="db")
    cursor = conn.cursor()
    cursor.execute("""SELECT * FROM zno2020 LIMIT 5""")
    rows = cursor.fetchall()
    for row in rows:
        print(row)
    cursor.close()
    conn.close()
    print('done print data')


def get_zno_data_by_year():
    combine_data()
    print("ZNO table created")

    conn = psycopg2.connect(dbname="test", user="postgres", password="postgres", host="db")
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT region, 
                   AVG(CASE WHEN year = '2019' AND math_test_stat = 'Зараховано' 
                     AND math_100 > 0 THEN math_100 END) AS avg_math_2019,
                   AVG(CASE WHEN year = '2020' AND math_test_stat = 'Зараховано' 
                     AND math_100 > 0 THEN math_100 END) AS avg_math_2020
            FROM zno
            GROUP BY region""")
        results = cursor.fetchall()
        print(results)

        dict_math = {}
        for row in results:
            region = row[0]
            avg_math_2019 = float(row[1])
            avg_math_2020 = float(row[2])
            dict_math[region] = [avg_math_2019, avg_math_2020]
        write_in_file(dict_math)
        print("Results are written to results.csv")

    except psycopg2.Error:
        print("Retrying in 5 seconds...")
        time.sleep(5)
        combine_data()
    finally:
        cursor.close()
        conn.close()
    print('done get zno by year')


def write_in_file(dict_math):
    try:
        filename = os.path.join(os.getcwd(), "results.csv")
        with open(filename, mode="w", newline="") as csv_file:
            writer = csv.writer(csv_file, delimiter=",")
            writer.writerow(["Regions", "2019", "2020"])
            for region, avg_math in dict_math.items():
                print([region, avg_math[0], avg_math[1]])
                writer.writerow([region, avg_math[0], avg_math[1]])
        print(f"Results are written to {filename}")
    except Exception:
        print("Error writing to file")
    print('done write in file')


def main():

    try:
        # connect to database
        conn = psycopg2.connect(dbname="test", user="postgres", password="postgres", host="db")

        cur = conn.cursor()
        # checks
        cur.execute("""
                        SELECT RegName, examyear, AVG(BALL100)  
                        FROM public.test
                        LEFT JOIN public.student
                        USING (student_id)
                        LEFT JOIN public.place
                        USING (place_id)
                        WHERE Subject_ID=4 AND teststatus='Зараховано'
                        GROUP BY RegName, examyear
                        ORDER BY RegName, examyear; 
                        """)

        # fetch all the rows from the resultset
        rows = cur.fetchall()

        # open the output file in write mode
        with open("/app/logs/phys_2019_2020.csv", "w", newline='', encoding='utf-8') as csvfile:
            # create a csv writer object
            writer = csv.writer(csvfile)

            # write the header row
            writer.writerow([col.name for col in cur.description])

            # write the rows to the csv file
            for row in rows:
                writer.writerow(row)
        conn.commit()

        print("Stats created successfully.")

    except psycopg2.Error as e:
        print(f"Error: {e}")
        print("Retrying in 10 seconds...")
        time.sleep(10)
        create_tables()

    finally:
        cur.close()
        conn.close()


# db_data()
# fill_year_column()
# combine_data()
main()
