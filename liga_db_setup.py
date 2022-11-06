import psycopg2 as ppg

# Connection parameters
database = 'postgres'
user = 'postgres'
password = ' '
host = 'localhost'

# Connect to postgres DB
conn = ppg.connect("dbname=\'{}\' user=\'{}\' password=\'{}\' host=\'{}\'"
                   .format(database, user, password, host))
# Set autocommit to create a db
conn.autocommit = True
cur = conn.cursor()

# Create database
cur.execute("create database liga;")

cur.close()
conn.close()

# Reconnect to newly created database
database = 'liga'
conn = ppg.connect("dbname=\'{}\' user=\'{}\' password=\'{}\' host=\'{}\'"
                   .format(database, user, password, host))
cur = conn.cursor()

# Create schema
cur.execute("begin;"
            "create schema stats;"
            "commit;")

# Create club table
cur.execute("begin;"
            "create table stats.clubs ("
            "id serial primary key,"
            "club varchar unique,"
            "mv_euros numeric,"
            "num_foreigners int);"
            "commit;")

# Create player table
cur.execute("begin;"
            "create table stats.players ("
            "id serial primary key,"
            "player_name varchar unique,"
            "status varchar,"
            "primary_position varchar,"
            "secondary_position varchar,"
            "age int,"
            "country varchar,"
            "club varchar,"
            "mv_euros int);"
            "commit;")

# Close connection to DB
cur.close()
conn.close()
