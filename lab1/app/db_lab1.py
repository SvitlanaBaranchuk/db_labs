# Імпорт необхідних бібліотек.
import psycopg2
import pandas as pd
import time
import csv

username = 'Baranchuk'
password = '20032003'
database = 'Baranchuk'
host = 'data_base'
port = '5432'

# імпортуємо дані за 2020 рік
data2020 = pd.read_csv('Odata2020File.csv',
                        encoding='cp1251', sep=';', low_memory=False, decimal=',')

# імпортуємо дані за 2021 рік
data2021 = pd.read_csv('Odata2021File.csv',
                       encoding='utf-8', sep=';', low_memory=False, decimal=',')

# зведемо найменування стовпчиків до одного типу найменування
data2020.columns = data2020.columns.str.lower()
data2021.columns = data2021.columns.str.lower()

data2020['zno_year'] = 2020
data2021['zno_year'] = 2021

# об'єднаємо наші дані в один датафрейм
data = pd.concat([data2020, data2021], sort=False, axis=0)


### переходимо до PostgreSQL
conn = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)

query = """
    CREATE TABLE IF NOT EXISTS zno_results(
        outid  VARCHAR NOT NULL,
        birth  NUMERIC NOT NULL,
        sextypename  VARCHAR NOT NULL,
        regname  VARCHAR NOT NULL,
        areaname  VARCHAR NOT NULL,
        tername  VARCHAR NOT NULL,
        regtypename  VARCHAR NOT NULL,
        tertypename  VARCHAR NOT NULL,
        classprofilename  VARCHAR NOT NULL,
        classlangname  VARCHAR NOT NULL,
        eoname  VARCHAR NOT NULL,
        eotypename  VARCHAR NOT NULL,
        eoregname  VARCHAR NOT NULL,
        eoareaname  VARCHAR NOT NULL,
        eotername  VARCHAR NOT NULL,
        eoparent  VARCHAR NOT NULL,
        ukrtest  VARCHAR,
        ukrteststatus  VARCHAR,
        ukrball100  DECIMAL,
        ukrball12  NUMERIC,
        ukrball  NUMERIC,
        ukradaptscale  NUMERIC,
        ukrptname  VARCHAR,
        ukrptregname  VARCHAR,
        ukrptareaname  VARCHAR,
        ukrpttername  VARCHAR,
        histtest  VARCHAR,
        histlang  VARCHAR,
        histteststatus  VARCHAR,
        histball100  DECIMAL,
        histball12  NUMERIC,
        histball  NUMERIC,
        histptname  VARCHAR,
        histptregname  VARCHAR,
        histptareaname  VARCHAR,
        histpttername  VARCHAR,
        mathtest  VARCHAR,
        mathlang  VARCHAR,
        mathteststatus  VARCHAR,
        mathball100  DECIMAL,
        mathball12  NUMERIC,
        mathball  NUMERIC,
        mathptname  VARCHAR,
        mathptregname  VARCHAR,
        mathptareaname  VARCHAR,
        mathpttername  VARCHAR,
        phystest  VARCHAR,
        physlang  VARCHAR,
        physteststatus  VARCHAR,
        physball100  DECIMAL,
        physball12  NUMERIC,
        physball  NUMERIC,
        physptname  VARCHAR,
        physptregname  VARCHAR,
        physptareaname  VARCHAR,
        physpttername  VARCHAR,
        chemtest  VARCHAR,
        chemlang  VARCHAR,
        chemteststatus  VARCHAR,
        chemball100  DECIMAL,
        chemball12  NUMERIC,
        chemball  NUMERIC,
        chemptname  VARCHAR,
        chemptregname  VARCHAR,
        chemptareaname  VARCHAR,
        chempttername VARCHAR,
        biotest  VARCHAR,
        biolang  VARCHAR,
        bioteststatus  VARCHAR,
        bioball100  DECIMAL,
        bioball12  NUMERIC,
        bioball  NUMERIC,
        bioptname  VARCHAR,
        bioptregname  VARCHAR,
        bioptareaname  VARCHAR,
        biopttername  VARCHAR,
        geotest  VARCHAR,
        geolang  VARCHAR,
        geoteststatus  VARCHAR,
        geoball100  DECIMAL,
        geoball12  NUMERIC,
        geoball  NUMERIC,
        geoptname  VARCHAR,
        geoptregname  VARCHAR,
        geoptareaname  VARCHAR,
        geopttername  VARCHAR,
        engtest  VARCHAR,
        engteststatus  VARCHAR,
        engball100  DECIMAL,
        engball12  VARCHAR,
        engdpalevel  VARCHAR,
        engball  NUMERIC,
        engptname  VARCHAR,
        engptregname  VARCHAR,
        engptareaname  VARCHAR,
        engpttername  VARCHAR,
        fratest  VARCHAR,
        frateststatus  VARCHAR,
        fraball100  DECIMAL,
        fraball12  NUMERIC,
        fradpalevel  VARCHAR,
        fraball  NUMERIC,
        fraptname  VARCHAR,
        fraptregname  VARCHAR,
        fraptareaname  VARCHAR,
        frapttername  VARCHAR,
        deutest  VARCHAR,
        deuteststatus  VARCHAR,
        deuball100  DECIMAL,
        deuball12  NUMERIC,
        deudpalevel  VARCHAR,
        deuball  NUMERIC,
        deuptname  VARCHAR,
        deuptregname  VARCHAR,
        deuptareaname VARCHAR,
        deupttername  VARCHAR,
        spatest  VARCHAR,
        spateststatus  VARCHAR,
        spaball100  DECIMAL,
        spaball12  NUMERIC,
        spadpalevel  VARCHAR,
        spaball  NUMERIC,
        spaptname  VARCHAR,
        spaptregname  VARCHAR,
        spaptareaname  VARCHAR,
        spapttername  VARCHAR,
        zno_year  NUMERIC NOT NULL,
        umltest  VARCHAR,
        umlteststatus  VARCHAR,
        umlball100  DECIMAL,
        umlball12  NUMERIC,
        umlball  NUMERIC,
        umladaptscale  NUMERIC,
        umlptname  VARCHAR,
        umlptregname  VARCHAR,
        umlptareaname  VARCHAR,
        umlpttername  VARCHAR,
        ukrsubtest  VARCHAR,
        mathdpalevel  VARCHAR,
        mathsttest  VARCHAR,
        mathstlang  VARCHAR,
        mathstteststatus  VARCHAR,
        mathstball12  NUMERIC,
        mathstball  NUMERIC,
        mathstptname  VARCHAR,
        mathstptregname  VARCHAR,
        mathstptareaname  VARCHAR,
        mathstpttername  VARCHAR
    );
    """

# Початковий час виконання запиту
start_time = time.time()
cursor = conn.cursor()
cursor.execute(query)
conn.commit()

columns = ','.join(data.columns.tolist())
placeholders = ','.join(['%s' for i in range(len(data.columns))])
batch_size = 200
offset = 0


# Функція для підключення до бази даних
def connect_to_db():
    try:
        conn = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)
        return conn
    except psycopg2.Error as e:
        # Створюємо виключення, якщо не вдається підключитися до бази даних
        raise Exception(f"Failed to connect to database: {e}")


print('Start data load.')
# Головний код програми
while offset <= len(data):
    rows = data.iloc[offset:offset + batch_size]
    print('Begin row:', rows)
    values = [tuple(row) for _, row in rows.iterrows()]
    insert_query = f"INSERT INTO zno_results ({columns}) VALUES ({placeholders})"
    try:
        conn = connect_to_db()
        cursor = conn.cursor()
        # Виконуємо запит до бази даних
        print('Load:', offset)
        cursor.executemany(insert_query, values)
        conn.commit()
        cursor.close()

    except Exception as e:
        print(e)
        # Очікуємо 5 секунд перед спробою відновлення з'єднання
        time.sleep(5)
        continue

    offset += batch_size
    print(offset)

print('Data load is done.')

# Кінцевий час виконання запиту
end_time = time.time()

# Збереження часу виконання у файл
with open('/app/time.csv', 'a') as f:
    f.write("Start_time, End_time\n")
    f.write(f"{start_time}, {end_time}\n")


query_res = """SELECT regname, zno_year, MIN(engball100) AS ball 
            FROM zno_results 
            WHERE engteststatus = 'Зараховано' 
            GROUP BY regname, zno_year;"""

cursor = conn.cursor()
cursor.execute(query_res)
results = cursor.fetchall()

with open('/app/output.csv', 'a', newline='') as f:
    writer = csv.writer(f)
    # Write the header row (if necessary)
    writer.writerow(['regname', 'zno_year', 'ball'])
    # Write each row of results
    for row in results:
        writer.writerow(row)


# Закриваємо з'єднання
conn.close()
