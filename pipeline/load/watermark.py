import psycopg2

from config.settings import POSTGRES_CONFIG


def get_last_energy_timestamp():
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = conn.cursor()

    cursor.execute("SELECT MAX(timestamp) FROM energy_consumption;")
    result = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    return result


def get_last_weather_timestamp():
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = conn.cursor()

    cursor.execute("SELECT MAX(date) FROM weather_daily;")
    result = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    return result
