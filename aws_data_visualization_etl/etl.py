
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.job import Job


from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql import functions as F

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])


#build spark session
sc = SparkSession \
    .builder \
    .appName("how to read csv file") \
    .getOrCreate()
    
def tranform_city(df, city, date_range):
    """
    Fumction to transform df given date range
    return : dataframe
    param : 
        df : dataframe
        city: name of city
        date_range: dae range
    """
    df = sc.read.csv(df,header=True)
    df = df.select(['date_time','maxtempC','mintempC','tempC'
                                    ,'DewPointC','FeelsLikeC','humidity','precipMM','pressure'])
    df = df.withColumn('cityName', lit(city))
    df = df.withColumn("date_time", to_timestamp(col("date_time"),"yyyy-MM-dd HH:mm:ss"))
    df = df.where(col("date_time") <= date_range)
    
    cols = ['maxtempC', 'mintempC', 'tempC', 'DewPointC', 'FeelsLikeC', 'humidity', 'precipMM', 'pressure']
    for column in cols:
        df = df.withColumn(column, col(column).cast('double'))
    print("total record for {} is {}".format(city, df.count()))
    print(df.printSchema())
    return df
    

def feature_aggregations(df,city):
    """
    Function to aggregate and join data frames.
    return : dataframe
    param:
        city: city name
        df: dataframe
    """
    func = [F.min,F.max,F.avg]
    groupby = ["date_time"]
    agg_cv = ["pressure","tempC","humidity"]
    expr_cv = [f(F.col(c)) for f in func for c in agg_cv]
    df = df.withColumn('date_time', split(col("date_time")," ")[0].cast("date"))
    df_final = df.groupby(*groupby).agg(*expr_cv)
    df_final = df_final.withColumn('cityName', lit(city))
    return df_final
    
    
df_bengaluru = tranform_city('s3://citiesweather/bengaluru.csv','bengaluru', "2009-01-31 00:00:00")
df_bengaluru_final = feature_aggregations(df_bengaluru,'bengaluru')

df_pune = tranform_city('s3://citiesweather/pune.csv','pune', "2009-01-31 00:00:00")
df_pune_final = feature_aggregations(df_pune,'pune')

df_delhi = tranform_city('s3://citiesweather/delhi.csv','delhi', "2009-01-31 00:00:00")
df_delhi_final = feature_aggregations(df_delhi,'delhi')

final_df = df_bengaluru_final.union(df_pune_final).union(df_delhi_final)

final_df.coalesce(1).write.format('csv').option('header','true').mode("overwrite").save("s3://citiesweather/output")
