import pyspark
from pyspark.sql import session,SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DecimalType,FloatType
from pyspark.sql.functions   import col
def file_load(path):
    df=spark.read.option("delimiter", ",").\
        option("quote", "'"). \
        csv(path,inferSchema=True,header=True)
    return df
def data_cleaning(df):
    #drop logo  column
    df=(df.drop('LOGO','LOGO_MIME_TYPE'))
    #replace null values
    #df=df.na.fill("retail",["LOGO_MIME_TYPE"])
    #LOGO_MIME_TYPE put this ecom for store_name online else retail
    df=df.withColumn("LOGO_MIME_TYPE",F.when(df.STORE_NAME=='Online','ecom').otherwise('retail'))
    #replace certain unwanted values
    df=df.replace("null","0.00",["LATITUDE","LONGITUDE"]).replace("null","NA",["PHYSICAL_ADDRESS"])
    return df

def data_transform(df):
    df=df.withColumn("LONGITUDE",col('LONGITUDE').cast(FloatType()))\
    .withColumn("LATITUDE",col('LATITUDE').cast(FloatType()))
    df=df.withColumn("WEB_ADDRESS",F.when(col("WEB_ADDRESS")=='null',"").otherwise(col('WEB_ADDRESS')))
    return df
def data_loading(df):
    #dta loading techniques to be reviewd
    df.write \
    .format("jdbc") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("url", "jdbc:mysql://localhost:3306/Work") \
    .option("dbtable", "stores") \
    .option("user", "Aashish") \
    .option("password", "19May2000") \
    .option("mode","overwrite")\
    .save()

if __name__=='__main__':
    spark=SparkSession.builder\
            .appName("csv_db")\
           .config("spark.jars","/usr/share/java/mysql-connector-java-9.0.0.jar")\
           .getOrCreate()
    csv_file=input('Enter File path')
    data=file_load(csv_file)
    #print(data_cleaning(data).show())
    cleaned_data=data_cleaning(data)
    final_data=data_transform(cleaned_data)
    data_loading(final_data)
    print(final_data.show())
    print(final_data.printSchema())
    # extract the name column using list comprehension
    #name_list = [row.name for row in file_load(csv_file).select('physical_address').collect()]