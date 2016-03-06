import getopt
import json
import sys
import psycopg2
import requests
from psycopg2 import connect
from psycopg2.extensions import AsIs
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL



def begin_nass_harvest(database_host, database_name, database_user, database_password,
                       port, start_date, end_date):
#    print("\nThis is a starter script for the Gro Hackathon's NASS harvest. It meets the API " \
#          "requirements defined for the hackathon\n\n")

    print("Run 'python harvest.py -h' for help\n\n")
#    print("Feel free to edit the entirety of this start script\n")

    print("Supplied Args (some default): ")
    print("Database Host: {}".format(database_host))
    print("Database Name: {}".format(database_name))
    print("Database Username: {}".format(database_user))
    print("Database Password: {}".format(database_password))
    print("Database Port (hard-coded): {}".format(port))
    print("Harvest Start Date: {}".format(start_date))
    print("Harvest End Date: {}\n".format(end_date))

    """config.start_date= start_date
    config.end_date = end_date
    config.database_name = database_name
    config.database_user = database_user
    config.database_password = database_password"""

    create_db(database_user, database_host, database_name, database_password, port)
    data = make_request()
    store_data(data, database_user, database_host, database_name, database_password, port)


###Create the Database and Tables
def create_db(database_user, database_host, database_name, database_password, port):
    conn = connect(user=database_user, host=database_host, password=database_password)

    cur = conn.cursor()

    cur.execute("""SELECT * FROM pg_catalog.pg_database WHERE datname= %s """, (database_name,))

    if cur.rowcount == 1:
        print("DB already Exists")
        conn.close()
    else:
        print("DB doesnt exist, Creating Database")
        url = URL(drivername='postgresql', username=database_user, password=database_password, host=database_host,
                  database='template1')
        eng = create_engine(url)
        conn = eng.connect()
        conn.connection.connection.set_isolation_level(0)
        conn.execute('create database %s', (AsIs(database_name),))
        conn.connection.connection.set_isolation_level(1)
        print("DB Created Successfully")
        conn.close()

    try:
        conn = psycopg2.connect(database=database_name, user=database_user, password=database_password,
                                host=database_host,
                                port=port)
        cur = conn.cursor()
    except psycopg2.OperationalError as e:
        print("Error\n{0}").format(e)
    else:
        print("DB connect Successfull")

    try:
        cur.execute(
            "CREATE TABLE IF NOT EXISTS public.fact_data(id SERIAL NOT NULL PRIMARY KEY,domain_desc text, commodity_desc text,statisticcat_desc text,agg_level_desc text,country_name text,state_name text,county_name text,unit_desc text,value text,year integer)")
        conn.commit()
    except psycopg2.OperationalError as e:
        print("Error\n{0}").format(e)
    else:
        print("Table Ok")
    conn.close()


# cur.execute("select exists(select * from information_schema.tables where table_name=%s)", ('fact_data',))
# if cur.fetchone()[0] == True:
#    print("Table Doesnt Exist")
# else:
#    print("Table Exists")

####Request the data and store in a local file
def make_request():
    r = requests.get(
            'http://quickstats.nass.usda.gov/api/api_GET/?key=AD726B5F-3047-34A2-9104-30AB0AB714EC&commodity_desc=CORN&year__GE=2012&state_alpha=VA&format=JSON')
    with open('data.txt', 'w') as outfile:
        json.dump(r.json(), outfile)

    maindict = json.loads(open('data.json').read())

    data = maindict["data"]
    return data


##Store Data in DB
def store_data(data, database_user, database_host, database_name, database_password, port):
    conn = psycopg2.connect(database=database_name, user=database_user, password=database_password,
                            host=database_host,
                            port=port)
    cur = conn.cursor()
    print("Preparing Table for Entry")
    cur.execute("truncate fact_data;")
    print("Inserting Records")
    for value in data:
        domain_desc = value["domain_desc"]
        commodity_desc = value["commodity_desc"]
        statisticcat_desc = value["statisticcat_desc"]
        agg_level_desc = value["agg_level_desc"]
        country_name = value["country_name"]
        state_name = value["state_name"]
        county_name = value["county_name"]
        unit_desc = value["unit_desc"]
        value1 = value["Value"]
        year_val = value["year"]
        # cur.execute("INSERT INTO fact_data (domain_desc,commodity_desc,statisticcat_desc,agg_level_desc,country_name,state_name,county_name,unit_desc,value,year) VALUES (domain_desc,commodity_desc,statisticcat_desc,agg_level_desc,country_name,state_name,county_name,unit_desc,value1,year_val)")
        # cur.execute("INSERT INTO fact_data (domain_desc,commodity_desc,statisticcat_desc,agg_level_desc,country_name,state_name,county_name,unit_desc,value,year) VALUES (" + ', '.join(map(str, (domain_desc,commodity_desc,statisticcat_desc,agg_level_desc,country_name,state_name,county_name,unit_desc,value1,year_val))) + ")")
        cur.execute(
            """INSERT INTO fact_data (domain_desc,commodity_desc,statisticcat_desc,agg_level_desc,country_name,state_name,county_name,unit_desc,value,year)
                VALUES (%s, %s, %s, %s, %s, %s,%s, %s, %s, %s);""",
            (domain_desc, commodity_desc, statisticcat_desc, agg_level_desc, country_name, state_name, county_name,
             unit_desc, value1, year_val))
        print(".", end=" ")

    print("\n")
    conn.commit()
    conn.close()
    print("Success!")


# #################################################
# PUT YOUR CODE ABOVE THIS LINE
# #################################################
def main(argv):
    try:
        opts, args = getopt.getopt(argv, "h", ["database_host=", "database_name=", "start_date=",
                                               "database_user=", "database_password=", "end_date="])
    except getopt.GetoptError:
        print
        'Flag error. Probably a mis-typed flag. Make sure they start with "--". Run python ' \
        'harvest.py -h'
        sys.exit(2)

    # define defaults
    database_host = 'localhost'
    database_name = 'groapp'
    port = 5432
    database_user = 'postgres'
    database_password = 'spiderpig'
    start_date = '2005-1-1'
    end_date = '2015-12-31'

    for opt, arg in opts:
        if opt == '-h':
            print("\nThis is my harvest script for the Gro Hackathon NASS harvest")
            print('\nExample:\npython harvest.py --database_host localhost --database_name gro2\n')
            print('\nFlags (all optional, see defaults below):\n ' \
                  '--database_host [default is "{}"]\n ' \
                  '--database_name [default is "{}"]\n ' \
                  '--database_user [default is "{}"]\n ' \
                  '--database_password [default is "{}"]\n ' \
                  '--start_date [default is "{}"]\n ' \
                  '--end_date [default is "{}"]\n'.format(database_host, database_name, database_user,
                                                          database_password, start_date, end_date))
            sys.exit()
        elif opt in ("--database_host"):
            database_host = arg
        elif opt in ("--database_name"):
            database_name = arg
        elif opt in ("--database_user"):
            database_user = arg
        elif opt in ("--database_password"):
            database_password = arg
        elif opt in ("--start_date"):
            start_date = arg
        elif opt in ("--end_date"):
            end_date = arg

    begin_nass_harvest(database_host, database_name, database_user, database_password,
                       port, start_date, end_date)


if __name__ == "__main__":
    main(sys.argv[1:])
