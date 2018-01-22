from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank
import datetime


TOP_NUMBER = '5'

def read_data_from_csv():    
    
    ts1 = datetime.datetime.now().timestamp()
    df_calls = spark.read.load("sources/calls.csv", format="csv", sep=",", inferSchema="true", header="true")
    ts2 = datetime.datetime.now().timestamp()
    
    print ('It takes ' + str(round(ts2 - ts1, 3)) + ' seconds to read data from csv' )
    return df_calls

def read_data_from_parquet():    
    
    ts1 = datetime.datetime.now().timestamp()
    df_calls = spark.read.load("sources/calls.parquet")
    ts2 = datetime.datetime.now().timestamp()
    
    print ('It takes ' + str(round(ts2 - ts1, 3)) + ' seconds to read data from parquet' )
    return df_calls

def calculate_top_numbers(): 

    df_calls = read_data_from_csv()
    df_calls_p = read_data_from_parquet()

    region_id = input('Give me an region id from 1 to 10: ')
    
    df_calls.registerTempTable('calls')

    spark.sql('select \
            number_from \
          , duration \
          , rank \
    from \
            (select \
                  number_from \
                , duration \
                , dense_rank() over(order by duration desc) as rank \
            from \
                    (select \
                            number_from \
                          , sum(duration) as duration \
                     from calls \
                     where region_id = ' + region_id + ' \
                     group by number_from \
                    )a \
            )b \
    where rank <= ' + TOP_NUMBER + ' \
    order by duration desc')\
    .show()



spark = SparkSession \
    .builder \
    .appName("Python Spark calls example") \
    .getOrCreate()

calculate_top_numbers()

spark.stop()
