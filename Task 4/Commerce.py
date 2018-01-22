from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank


def load_data ():    
    global df_p
    global df_oi
    df_p = spark.read.load("sources/product.csv",format="csv", sep=";", inferSchema="true", header="true")
    df_oi = spark.read.load("sources/order_item.csv",format="csv", sep=",", inferSchema="true", header="true")


    
def calculate_top_products_by_sql():    

    df_p.registerTempTable('products')
    df_oi.registerTempTable('order_items')


    spark.sql('select \
      category \
    , name \
    , cost \
    from \
            (select \
                      name \
                    , category \
                    , cost \
                    , dense_rank() over (partition by category order by cost desc ) as rank \
            from \
                    (select \
                              p.name \
                            , p.category \
                            , sum(i.cost) as cost \
                       from products p \
                       join order_items i on p.product_id = i.product_id \
                       group by  p.name, p.category \
                     ) a \
            ) b \
    where rank <= 3 \
    order by category, cost desc')\
    .show()

def calculate_top_products_by_data_frame():
    df_s = df_oi.join(df_p, df_oi.product_id == df_p.product_id, 'inner')\
        .groupby( df_p.category, df_p.name)\
        .sum ('cost')\
        .withColumnRenamed('sum(cost)', 'cost')

    w =  Window.partitionBy(df_s.category ).orderBy(df_s.cost.desc())

    df_s.select( "category", "name", dense_rank().over(w).alias("rank"), "cost")\
        .filter("rank <= 3").select("category", "name","cost")\
        .sort(["category","cost"], ascending=[True,False])\
        .show()
    
    
spark = SparkSession \
    .builder \
    .appName("Python Spark example") \
    .getOrCreate()

load_data()
calculate_top_products_by_sql()
calculate_top_products_by_data_frame()
    
spark.stop() 

    


   
