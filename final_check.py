import psycopg2

conn = psycopg2.connect(
    host="eia-db.crk68cw88mup.ap-south-1.rds.amazonaws.com",
    database="postgres",
    user="postgres",
    password="eia12345",
    port=5432,
    sslmode="require",
)

cur = conn.cursor()

cur.execute(
    """
SELECT * FROM weather_daily;
"""
)

tables = cur.fetchall()

for table in tables:
    print(table[0])

conn.close()
