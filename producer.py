from kafka import KafkapProducer
import json
from time import sleep
import psycopg2

prod = KafkapProducer(bootstrap_servers=['list boostrap servers here'])


try:
    connection = psycopg2.connect(user="sysadmin",
                                  password="pynative@#29",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="postgres_db")
    cursor = connection.cursor()
    postgreSQL_select_Query = "select * from capture_state"

    cursor.execute(postgreSQL_select_Query)
    print("Selecting rows from capture_state table using cursor.fetchall")
    capture_state_records = cursor.fetchall()

    print("Print each row and it's columns values")
    for row in capture_state_records:
        data = row
        prod.send('awskafkatopic',json.dumps(data).encode('utf-8'))
        sleep(2)
        print("Id = ", row[0], )
        print("Model = ", row[1])
        print("Price  = ", row[2], "\n")

except (Exception, psycopg2.Error) as error:
    print("Error while fetching data from PostgreSQL", error)

finally:
    # closing database connection.
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")