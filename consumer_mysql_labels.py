# Updated consumer_labels.py file to read from MySQL table. Act as a sink/source connector
import time
import mysql.connector

MYSQL_HOST = ""
MYSQL_USER = ""
MYSQL_PASS = ""
MYSQL_DB   = ""

conn = mysql.connector.connect(
    host=MYSQL_HOST,
    user=MYSQL_USER,
    password=MYSQL_PASS,
    database=MYSQL_DB
)
cur = conn.cursor(dictionary=True)

print("Consumer (MySQL) running...")

while True:
    cur.execute("""
        SELECT ID, time, profileName, temperature, humidity, pressure
        FROM LabelsReadings
        WHERE processed = false
        ORDER BY ID ASC
        LIMIT 20
    """)
    rows = cur.fetchall()

    if not rows:
        time.sleep(1)
        continue

    for r in rows:
        print("\nReceived record from MySQL:")
        print("ID:", r["ID"])
        print("time:", r["time"])
        print("profileName:", r["profileName"])
        print("temperature:", r["temperature"])
        print("humidity:", r["humidity"])
        print("pressure:", r["pressure"])

        cur.execute("UPDATE LabelsReadings SET processed = true WHERE ID = %s", (r["ID"],))
        conn.commit()
