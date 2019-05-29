import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


#Load files into staging tables
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

#Insert records into fact and dimension tables        
def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
        
        
        
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print("Using these parameters to connect to database: {}".format(*config['CLUSTER'].values()))
    print("\n")

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print("Loading the 2 files from the AWS S3 bucket")
    load_staging_tables(cur, conn)
    print("   Loaded log_data and song_data into the staging tables!")
    print("\n")


    
    print("Inserting data from S3 into Facts & Dimensional tables in Redshift cluster")
    insert_tables(cur, conn)
    print("   Inserted all data into the respective facts & dimensional tables!")

    conn.close()

    print("   ETL process completed!")



if __name__ == "__main__":
    main()