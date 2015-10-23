from bs4 import BeautifulSoup
import pymysql.cursors
from pymysql.cursors import DictCursor
import pymssql
import collections
import sys
import datetime

DBHOST = "127.0.0.1"
USER = "erickardus"
PASSWORD = "icarus123"
DB= "reports"
CHARSET = "utf8"
TABLENAME = "lync_report"

report_file = "./LyncServicesReport.htm"
now = datetime.datetime.now().strftime("%Y-%m-%d")


def connect_mysql():

    try:
        conn = pymysql.connect(host=DBHOST, user=USER, password=PASSWORD, db=DB, charset=CHARSET, cursorclass=DictCursor)
        return conn
    except pymysql.err.OperationalError as err:
        print("There was a problem connection to the DB. " + str(err))
        sys.exit(15)
    except:
        print(sys.exc_info())
        print("Unknown issue connection to the DB.")
    finally:
        print("Shutting down...")


def connect_mssql():

    try:
        conn = pymssql.connect(DBHOST, USER, PASSWORD, DB)
        return conn
    except:
        print(sys.exc_info())
        print("Unknown issue connection to the DB.")
    finally:
        print("Shutting down...")


def create_table(table):

    headers = [x.string for x in table.find_all('tr')[1] if x != '\n']
    mystr = "("
    for head in headers:
        mystr += head.lower().replace(" ", "_") + " VARCHAR(90), "
    mystr += "submission_date DATE, "
    sql_query = "CREATE TABLE IF NOT EXISTS " + TABLENAME + " " + mystr + "PRIMARY KEY (" + headers[0].lower().replace(" ", "_") + ", " + headers[1].lower().replace(" ", "_") + "))"
    print(sql_query)
    return sql_query


def create_row_query(row):
    mystr = "("
    for value in row.values():
        mystr += "'" + value + "'" + ","
    mystr += " '" + now + "')"
    sql_query = "INSERT IGNORE INTO " + TABLENAME + " VALUES " + mystr
    print(sql_query)

    return sql_query


def parse_html_table(table):
    """
    :param table: Expects a table object structure from BeautifulSoup
    :return: Returns a list of dictionaries, that contain a mapping of the relationship header<->value
    e.g [ {'server_name': 'host1.domain', 'IP': '10.10.11.11', 'ping': 'ok'},
        {'server_name': 'host3.domain', 'IP': '192.168.1.20', 'ping': 'ok'} ]
    """

    # Creates a list with the table header values, please note we consider the headers to be in row[0]
    headers = [x.string for x in table.find_all('tr')[1] if x != '\n']

    # Remove the first rows of the table, so we only see values
    rows = table.find_all('tr')[2:]

    # For every row, combine the 'headers' list with the row into a dictionary and append the result in a list of dict.
    result = []
    for row in rows:
        values = [x.string for x in row.find_all('td')]
        dictionary = collections.OrderedDict(zip(headers, values))
        result.append(dictionary)

    return result


def execute_query(sql_query, cursor):
    """
    Create and execute MySQL query based on the row dictionary.
    :param row: table row formatted as a dictionary.
    :param cursor: the MySQL connector cursor, initialized previously.
    :return: doesn't return anything, it only executes the query and print result.
    """

    # Uses the provider connector cursor to execute query and prints out result.
    try:
        cursor.execute(sql_query)
        print(sql_query)
        print("Executed successfully")
    except pymysql.err.ProgrammingError as err:
        print(err)
    except:
        print(sys.exc_info())
        print("didn't work")

    return

# ######### MAIN ########## #

# Parse the HTML report file into a BeautifulSoup object
try:
    with open(report_file) as fh:
        soup = BeautifulSoup(fh.read().replace('\x00', ''), 'html.parser')
except FileNotFoundError as err:
    print("Error parsing HTML report. " + str(err))
    sys.exit(10)

# Parse every table into dictionaries. Make sure to only use tables with desired content.
list_of_tables = soup.find_all('table')
chosen_tables = list_of_tables[1:]   # Only interested in table num 1 and above

# Connect to database and fill the DB with our data. CHOOSE: MySQL or MSSQL
connection = connect_mysql()
#connection = connect_mssql()

with connection.cursor() as cursor:

    for table in chosen_tables:

        # Obtain headers from table and create a MySQL table or make sure it exists.
        sql_query = create_table(table)
        execute_query(sql_query, cursor)

        # Parse data and insert into previously created table.
        data = parse_html_table(table)
        for row in data:
            sql_query = create_row_query(row)
            execute_query(sql_query, cursor)

    connection.commit()