from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, when, first, desc
from pyspark.sql.types import IntegerType
from config import PATHS, SPARK_CONFIG, SPARK_PACKAGES


def read_mongo(collection):
    
    return spark.read.format("mongodb") \
        .option("connection.uri", f"mongodb://127.0.0.1:27017/formattedzone.{collection}") \
        .option("database", "formattedzone") \
        .option("collection", collection) \
        .load()


def saveDelta_df(df, transformed_path: str):
    """
    Saves the formatted dataframe in a Delta File 
    """
    print("saving in ", transformed_path)
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(transformed_path)
    


# def get_year_range(df, df_name):
#     """
#     Prints the year range (min and max) for a given DataFrame.
#     """
#     try:
#         year_range = df.selectExpr("min(year) as min_year", "max(year) as max_year").collect()
#         print(f"{df_name} Year Range: {year_range[0]['min_year']} - {year_range[0]['max_year']}")
#     except Exception as e:
#         print(f"Error getting year range for {df_name}: {e}")


def transform_data(spark: SparkSession, formatted_path: str, transformed_path: str):
    """
    Transforms formatted data to create features for predictive analytics
    """
    
    #read from mongodb
    try:
        df_income = read_mongo("income")
        df_density = read_mongo("density")
        df_immigrants = read_mongo("immigrants")
        df_unemployment = read_mongo("unemployment")
    except Exception as e:
        print(f"Error: Could not connect/read from MongoDB. {e}")
        return          
    

    
    #applying transformations for the desired analysis:

    #For immigrants: group by year, neighborhood and getting the total immigrants (we treat all immigrants as equal for simplicity)
    df_immigrants = df_immigrants.fillna({"value":0})
    df_agg_immigrants = df_immigrants.groupby("year", "neighborhood_code").agg(
                        _sum("value").alias("total_immigrants"))
    
    #For unemployment: select "Atur registrat", and group by year and neighborhood, and fill "count" for an exceptional case
    df_unemployment = df_unemployment.fillna({"count":0}).filter(
        (col("job_demand") == 'Atur Registrat') | (col("job_demand") == 'Atur registrat')).groupby(
                        "year", "neighborhood_code").agg(
                        _sum("count").alias("total_unemployed"))


    # # Example usage for all DataFrames
    # get_year_range(df_income, "Income")
    # get_year_range(df_density, "Density")
    # get_year_range(df_agg_immigrants, "Immigrants")
    # get_year_range(df_unemployment, "Unemployment")
    # Density and Income is not necessary as we already have it on a year and neighborhood level.

    #Joining dataframes:
    df_base = df_income.filter(col("RDF_index").isNotNull())

    # Join Density
    df_step1 = df_base.join(
        df_density, 
        ["year", "neighborhood_code"], 
        "left"
    )

    # Join Immigrant 
    df_step2 = df_step1.join(
        df_agg_immigrants,
        ["year", "neighborhood_code"], 
        "left"
    )

    # Join Unemployment
    df_step3 = df_step2.join(
        df_unemployment,
        ["year", "neighborhood_code"],
        "left"
    )

    #drop years where we don't have data:
    df_temp = df_step3.filter(col("net_density").isNotNull() & col("total_immigrants").isNotNull() 
                              & col("total_unemployed").isNotNull()
                ).select(
        col("year"),
        col("neighborhood_code"),
        
        # Resolving 'district_code' duplicate
        df_base["district_code"], 
        df_base["district_name"],
        
        # Resoloving 'population' duplicate
        df_base["population"],    
        
        col("RDF_index"),
        col("density"),
        col("net_density"),
        col("total_immigrants"),
        col("total_unemployed")
    )


    # CHECKING ALL OKAY
    print("temp table:", df_temp.head())

    #Feature Engineering:
    df_final = df_temp\
        .withColumn("ratio_immigrants", col("total_immigrants")/col("population"))\
        .withColumn("ratio_unemployed", col("total_unemployed")/col("population"))

    saveDelta_df(df_final, transformed_path)

    





if __name__ == "__main__":

    appName = "app"
    master = "local[*]" 

    if not 'spark' in globals(): 
        conf = SparkConf().setAppName(appName).setMaster(master)
        spark = SparkSession.builder \
                .config(conf=conf) \
                .config("spark.jars.packages", ",".join(SPARK_PACKAGES)) \
                .config("spark.sql.extensions", SPARK_CONFIG["spark.sql.extensions"]) \
                .config("spark.sql.catalog.spark_catalog", SPARK_CONFIG["spark.sql.catalog.spark_catalog"]) \
                .getOrCreate()

    formatted_zone = PATHS["formatted_zone"]
    exploitation_zone = PATHS["exploitation_zone"]
    transform_data(spark, formatted_zone, exploitation_zone)
    spark.stop()