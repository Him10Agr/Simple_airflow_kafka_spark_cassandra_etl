import logging
from datetime import datetime

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *


#creating keyspace (schema) for cassandra db
def create_keyspace(session):
    session.execute(
        '''
            CREATE KEYSPACE IF NOT EXISTS spark_streams
            WITH replication: {'class': 'SimpleStrategy', 'replication_factor' = '1'};
        '''
    )
    

def create_table(session):
    session.execute(
        '''
            CREATE TABLE IF NOT EXISTS spark_streams.created_users (
                id UUID PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                gender TEXT,
                address TEXT,
                post_code TEXT,
                email TEXT,
                username TEXT,
                registered_date TEXT,
                phone TEXT,
                picture TEXT
            );
        '''
    )
    logging.info('Table created successfully!')
    

def insert_data(session, **kwargs):
    logging.info("Inserting data.....")
    id = kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    post_code = kwargs.get('post_code')
    email = kwargs.get('email')
    username = kwargs.get('username')
    dob = kwargs.get('dob')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')
    
    try:
        session.execute("""
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
                post_code, email, username, dob, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """, (id, first_name, last_name, gender, address,
              post_code, email, username, dob, registered_date, phone, picture)
        )    
        logging.info(f'Data Inserted Successfully for {first_name} {last_name}')
        
    except Exception as e:
        logging.error(f'Couldn\'t create the spark session due to {e}')
        
        
def create_spark_connection():
    
    spark = None
    try:
        spark = SparkSession.builder.appName('SparkDataStreaming')\
                .config('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,'
                        'org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0')\
                .config('spark.cassandra.connection.host', 'localhost')\
                    .getOrCreate()
                    
        spark.sparkContext.setLogLevel('ERROR')
        logging.info('Spark connection created successfully!')
                    
    except Exception as e:
        logging.error(f'Couldn\'t create the spark session due to {e}' )
        
    return spark


def create_cassandra_connection():
    
    session = None
    try:
        #connecting to the cassandra cluster
        cluster = Cluster(['localhost'])
        session = cluster.connect()
        
    except Exception as e:
        logging.error(f'Couldn\'t create the cassandra session due to {e}')

    return session

def connect_to_kafka(spark):
    spark_df = None
    try:
        spark_df = spark.readStream\
                    .format('kafka')\
                    .option('kafka.bootstrap.servers', 'localhost:9092')\
                    .option('subscribe','users_created')\
                    .option('startingOffsets', 'earliest')\
                    .load()
        logging.info('kafka dataframe created successfully')
    except Exception as e:
        logging.error(f'kafka dataframe creation unsuccessful : {e}')
    
    return spark_df

def df_format_cassandra(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField('dob', StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)])
    
    sel = spark_df.selectExpr("CAST(value as STRING)")\
        .select(F.from_json(F.col('value'), schema).alias('data')).select('data.*')
        
    print(sel)
    return sel

if __name__ == '__main__':
    
    spark = create_spark_connection()
    if spark:
        df = connect_to_kafka(spark= spark)
        sel_df = df_format_cassandra(spark_df= df)
        cassandra = create_cassandra_connection()
        if cassandra:
            create_keyspace(session= cassandra)
            create_table(session= cassandra)
            #insert_data(session= cassandra)
            streaming_query = (sel_df.writeStream.format('org.apache.spark.sql.cassandra')\
                                .option('checkpointLocation', '/home/himanshu/Projects/DE_Proj2/Checkpoint/')\
                                .option('keyspace', 'spark_streams')\
                                .option('table', 'created_users')\
                                .start())
            streaming_query.awaitTermination()
            
    