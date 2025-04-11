from datetime import datetime, timedelta

from config.config_loader import environment
import pyodbc


def get_db_connection():
    connection = pyodbc.connect(
        f"DRIVER={{SQL Server}};"
        f"SERVER={environment['db_host']};"
        f"DATABASE={environment['db_name']};"
        f"UID={environment['db_user']};"
        f"PWD={environment['db_password']}"
    )
    return connection


def fetch_data_from_database(query):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    # Convert rows to a list of dictionaries
    columns = [column[0] for column in cursor.description]
    data = [dict(zip(columns, row)) for row in rows]
    conn.close()
    return data


# def organize_count_database_response(database_data):
#     organized_data = {}
#     for row in database_data:
#         cause_name = row['causeName']
#         count = row['total_count']
#         cumulative_percentage = row['cumulative_percentage']
#
#         if cause_name not in organized_data:
#             organized_data[cause_name] = {}
#         organized_data[cause_name] = {
#             'total_count': count,
#             'cumulative_percentage': cumulative_percentage
#         }
#     return organized_data
#
#
# def organize_duration_database_response(database_data):
#     organized_data = {}
#     for row in database_data:
#         cause_name = row['causeName']
#         cumulative_percentage = round(Decimal(row['cumulative_percentage']), 2)
#         duration = row['duration']
#
#         if cause_name not in organized_data:
#             organized_data[cause_name] = {}
#         organized_data[cause_name] = {
#             'cumulative_percentage': cumulative_percentage,
#             'duration': duration
#         }
#     return organized_data


def get_breaks_configurations():
    """
    Fetches the breaks table data from the database.

    Returns:
        list of dict: Breaks table data with relevant columns.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    query = """
    SELECT Break_Start_Time, Break_End_Time
    FROM Breaks
    WHERE iStatus = 1
    """
    cursor.execute(query)

    breaks_table = [
        (int(row.Break_Start_Time), int(row.Break_End_Time))
        for row in cursor.fetchall()
    ]

    conn.close()
    return breaks_table


def get_shift_configurations():
    """
    Fetch shift timings dynamically from the database.
    Returns:
        List of tuples [(Shift_Start_Time, Shift_End_Time)]
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    query = "SELECT Shift_Start_Time, Shift_End_Time FROM ShiftsMaster WHERE iStatus = 1 AND set_fk = 1"
    cursor.execute(query)
    shifts = [
        (millis_to_time(row.Shift_Start_Time), millis_to_time(row.Shift_End_Time))
        for row in cursor.fetchall()
    ]

    conn.close()
    return shifts


# Convert milliseconds into time
def millis_to_time(ms):
    """
    Convert milliseconds to datetime.time.
    """
    return (datetime.min + timedelta(milliseconds=ms)).time()

