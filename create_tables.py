import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries, copy_table_queries, insert_table_queries 



#Function to drop any existing tables
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

#Function to create new tables
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

        

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print("Using these parameters to connect to database: ")
    print("----->" +  *config['CLUSTER'].values())
    print("\n")

    print("Make a connection to redshift db")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("   Connection made to redshift")


    print("Dropping any existing tables in database")
    drop_tables(cur, conn)
    print("   All tables dropped in database!")
    print("\n")


    print("Recreating all new tables in database")
    create_tables(cur, conn)
    print("   All tables created in redshift database!")
    print("\n")


    conn.close()

    print("   Run the ETL process to load data into tables!")
 

if __name__ == "__main__":
    main()