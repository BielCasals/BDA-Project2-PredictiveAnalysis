"""
Data formatter: take from landing zone and format it to ingest in mongoDB 
"""

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, when, explode
from pyspark.sql.types import IntegerType, FloatType
import os
from config import PATHS, SPARK_CONFIG, SPARK_PACKAGES

def format_and_load(landigzone_path, formatted_path, spark):
    """
    Formats and loads data from landing zone to mongoDB
    """
    format_load_lookup_tables(f"{landigzone_path}/lookup_tables", f"{formatted_path}/lookup_tables", spark)
    format_load_income(f"{landigzone_path}/income", f"{formatted_path}/income", spark)
    format_load_density(f"{landigzone_path}/density", f"{formatted_path}/density", spark)
    format_load_immigrants(f"{landigzone_path}/immigrants", f"{formatted_path}/immigrants", spark)
    format_load_unemployment(f"{landigzone_path}/unemployment", f"{formatted_path}/unemployment", spark)





def save_df(df, collection_name, formatted_zone):
    """
    Saves dataframe to mongoDB and local file system
    """

    #to mongoDB. CHANGE OVERWRITE LATER
            # .mode("append")

    df.write.format("mongodb") \
        .mode("overwrite") \
        .option("uri", "mongodb://localhost:27017/formattedzone") \
        .option("database", "formattedzone") \
        .option("collection", collection_name) \
        .save()

    # Save to local file system
    df.write.mode("overwrite").json(f"{formatted_zone}/{collection_name}")

    print(f"written in local {collection_name} on {formatted_zone}/{collection_name}")


def format_load_unemployment(landigzone_path, formatted_path, spark):
    """
    Formats unemployment data
    """
    print("formatting unemployment data...")
    raw_df = None
    
    #read all files from landing zone
    for folder in os.listdir(landigzone_path):
        if folder != "_processed_log.json":
            for file in os.listdir(f"{landigzone_path}/{folder}"):
                if file != "_SUCCESS":
                    read_df = spark.read.json(f"{landigzone_path}/{folder}/{file}")
                    if "error" not in read_df.columns:
                        raw_df = (raw_df.union(read_df)
                                ) if raw_df else read_df

# "records":[{"Any":"2014","Codi_Barri":"1","Codi_Districte":"1","Demanda_ocupació":"Atur registrat","Mes":"1","Nom_Barri":"el Raval",
# "Nom_Districte":"Ciutat Vella","Nombre":"2864","Sexe":"Homes","_id":1},
    formatted_df = raw_df.select(explode(col("result.records")).alias("record")).select(
        col("record.Codi_Districte").alias("district_code").cast(IntegerType()),
        col("record.Nom_Districte").alias("district_name"),
        col("record.Any").alias("year").cast(IntegerType()),
        col("record.Codi_Barri").alias("neighborhood_code").cast(IntegerType()),
        col("record.Nom_Barri").alias("neighborhood_name"),
        col("record.Nombre").alias("count").cast(IntegerType()),
        col("record.Sexe").alias("gender"),
        col("record.Demanda_ocupació").alias("job_demand"),
        col("record.Mes").alias("month").cast(IntegerType())
    ).withColumn("district_name", regexp_replace(col("district_name"), "-", " ")) \
     .withColumn("neighborhood_name", regexp_replace(col("neighborhood_name"), "-", " ")) \
     .withColumn("neighborhood_name", when(col("neighborhood_name") == "No consta", None).otherwise(col("neighborhood_name"))) \
     .withColumn("district_name", when(col("district_name") == "No consta", None).otherwise(col("district_name"))) \


    save_df(formatted_df, "unemployment", formatted_path)

    

def format_load_income(landigzone_path, formatted_path, spark):
    """
    Formats income data
    """
    print("formatting income data...")
    raw_df = None
    
    #read all files from landing zone
    for folder in os.listdir(landigzone_path):
        if folder != "_processed_log.json":
            for file in os.listdir(f"{landigzone_path}/{folder}"):
                if file != "_SUCCESS":
                    raw_df = (raw_df.union(spark.read.csv(f"{landigzone_path}/{folder}/{file}", header=True, inferSchema=True))
                            ) if raw_df else spark.read.csv(f"{landigzone_path}/{folder}/{file}", header=True, inferSchema=True) 
    

    #formatations:
    formatted_df = raw_df.select(
        col("Codi_Districte").alias("district_code").cast(IntegerType()),
        col("Nom_Districte").alias("district_name"),
        col("Any").alias("year").cast(IntegerType()),
        col("Índex RFD Barcelona = 100").alias("RDF_index").cast(FloatType()),
        col("Codi_Barri").alias("neighborhood_code").cast(IntegerType()),
        col("Nom_Barri").alias("neighborhood_name"),
        col("Població").alias("population").cast(IntegerType()),
    ).withColumn("RDF_index", when(col("RDF_index") == "-", None).otherwise(col("RDF_index").cast(FloatType()))) \
     .withColumn("population", when(col("population") == "-", None).otherwise(col("population").cast(IntegerType()))) \
     .withColumn("district_name", regexp_replace(col("district_name"), "-", " ")) \
     .withColumn("neighborhood_name", regexp_replace(col("neighborhood_name"), "-", " ")) \
     .withColumn("neighborhood_name", when(col("neighborhood_name") == "No consta", None).otherwise(col("neighborhood_name"))) \
     .withColumn("district_name", when(col("district_name") == "No consta", None).otherwise(col("district_name"))) \


    #save
    save_df(formatted_df, "income", formatted_path)
    
    


def format_load_density(landigzone_path, formatted_path, spark):
    """
    formatts density data
    We only keep the flat structure .json files as they give more variables
    """
    print("formatting density data...")
    raw_df = None

    #read all files from landing zone: but we check in json if nested or flat
    for folder in os.listdir(landigzone_path):
        if folder != "_processed_log.json":
            for file in os.listdir(f"{landigzone_path}/{folder}"):
                if file != "_SUCCESS":
                    df_temp = spark.read.json(f"{landigzone_path}/{folder}/{file}")
                    if 'info' in df_temp.columns:
                        #nested 
                        # flat_df = process_nested_density(df_temp)
                        pass
                    else:
                        flat_df = df_temp
                        raw_df = (raw_df.union(flat_df)) if raw_df else flat_df
        
    # formattations: we check formats as we have JSON with nested structures and others flat
    formatted_df = raw_df.select(
        col("Any").alias("year").cast(IntegerType()),
        col("Codi_Barri").alias("neighborhood_code").cast(IntegerType()),
        col("Codi_Districte").alias("district_code").cast(IntegerType()),
        col("Densitat (hab/ha)").alias("density").cast(FloatType()),
        col("Densitat neta (hab/ha)").alias("net_density").cast(FloatType()),
        col("Nom_Barri").alias("neighborhood_name"),
        col("Nom_Districte").alias("district_name"),
        col("Població").alias("population").cast(IntegerType()),
        col("Superfície (ha)").alias("area").cast(FloatType()),
        col("Superfície Residencial (ha)").alias("residential_area").cast(FloatType())
    ).withColumn("density", when(col("density") == "-", None).otherwise(col("density").cast(FloatType()))) \
     .withColumn("net_density", when(col("net_density") == "-", None).otherwise(col("net_density").cast(FloatType()))) \
     .withColumn("population", when(col("population") == "-", None).otherwise(col("population").cast(IntegerType()))) \
     .withColumn("area", when(col("area") == "-", None).otherwise(col("area").cast(FloatType()))) \
     .withColumn("residential_area", when(col("residential_area") == "-", None).otherwise(col("residential_area").cast(FloatType()))) \
     .withColumn("district_name", regexp_replace(col("district_name"), "-", " ")) \
     .withColumn("neighborhood_name", regexp_replace(col("neighborhood_name"), "-", " ")) \
     .withColumn("neighborhood_name", when(col("neighborhood_name") == "No consta", None).otherwise(col("neighborhood_name"))) \
     .withColumn("district_name", when(col("district_name") == "No consta", None).otherwise(col("district_name"))) \

    # Save
    save_df(formatted_df, "density", formatted_path)


def format_load_immigrants(landigzone_path, formatted_path, spark):
    """
    formatts immigrants data
    """
    print("formatting immigrants data...")
    raw_df = None

    #read all files from landing zone
    for folder in os.listdir(landigzone_path):
        if folder != "_processed_log.json":
            for file in os.listdir(f"{landigzone_path}/{folder}"):
                if file != "_SUCCESS":
                    raw_df = (raw_df.union(spark.read.json(f"{landigzone_path}/{folder}/{file}"))
                            ) if raw_df else spark.read.json(f"{landigzone_path}/{folder}/{file}")
    
    #formattations:
    formatted_df = raw_df.select(
        col("Any").alias("year").cast(IntegerType()),
        col("Codi_Barri").alias("neighborhood_code").cast(IntegerType()),
        col("Codi_Districte").alias("district_code").cast(IntegerType()),
        col("EDAT_Q").alias("age_group").cast(IntegerType()),
        col("NACIONALITAT_G").alias("nationality_group").cast(IntegerType()),
        col("Nom_Barri").alias("neighborhood_name"),
        col("Nom_Districte").alias("district_name"),
        col("SEXE").alias("gender").cast(IntegerType()),
        col("Valor").alias("value").cast(FloatType())
    ).withColumn("district_name", regexp_replace(col("district_name"), "-", " ")) \
     .withColumn("neighborhood_name", regexp_replace(col("neighborhood_name"), "-", " ")) \
     .withColumn("neighborhood_name", when(col("neighborhood_name") == "No consta", None).otherwise(col("neighborhood_name"))) \
     .withColumn("value", when(col("value") == "..", None).otherwise(col("value"))) 
    
    #save
    save_df(formatted_df, "immigrants", formatted_path)

def format_load_lookup_tables (landigzone_path, formatted_path, spark):
    """
    formatts lookup tables data
    """
    print("formatting lookup tables data...")
    
    immigrants_lookup_df = None
    names_lookup_df = None
    
    for batch in os.listdir(landigzone_path):
        if batch != "_processed_log.json":
            for file_imm in os.listdir(f"{landigzone_path}/{batch}/immigrants_extended.csv"):
                if file_imm != "_SUCCESS":
                    imm_df = spark.read.csv(f"{landigzone_path}/{batch}/immigrants_extended.csv/{file_imm}", header=True, inferSchema=True)
                    immigrants_lookup_df = (immigrants_lookup_df.union(imm_df)
                                            ) if immigrants_lookup_df else imm_df
            for file_name in os.listdir(f"{landigzone_path}/{batch}/income_opendatabcn_extended.csv"):
                if file_name != "_SUCCESS":
                    name_df = spark.read.csv(f"{landigzone_path}/{batch}/income_opendatabcn_extended.csv/{file_name}", header=True, inferSchema=True)
                    names_lookup_df = (names_lookup_df.union(name_df)
                                            ) if names_lookup_df else name_df

    formatted_immigrants_lookup_df = immigrants_lookup_df.select(
        col("Codi_Dimensio").alias("dimension_code").cast(IntegerType()),
        col("Desc_Dimensio").alias("dimension_description"),
        col("Codi_Valor").alias("value_code").cast(IntegerType()),
        col("Desc_Valor_EN").alias("value_description")
    )

    formatted_names_lookup_df = names_lookup_df.select(
        col("district").alias("district_name"),
        col("neighborhood").alias("neighborhood_name"),
        col("district_n_reconciled").alias("district_name_reconciled"),
        col("district_n").alias("district_name_normalized"),
        col("district_id").alias("district_id"),
        col("neighborhood_n_reconciled").alias("neighborhood_name_reconciled"),
        col("neighborhood_n").alias("neighborhood_name_normalized"),
        col("neighborhood_id").alias("neighborhood_id")
    )

    # Save the formatted lookup table    
    save_df(formatted_immigrants_lookup_df, "immigrants_lookup", formatted_path)
    save_df(formatted_names_lookup_df, "names_lookup", formatted_path)


if __name__ == "__main__":
    appName = "app"
    master = "local[*]" 
    if not 'spark' in globals(): 
        conf = SparkConf().setAppName(appName).setMaster(master)
        spark = SparkSession.builder \
                .config(conf=conf) \
                .config("spark.jars.packages", ",".join(SPARK_PACKAGES)) \
                .getOrCreate()
    landing_zone = PATHS["landing_zone"]
    formatted_zone = PATHS["formatted_zone"]
    format_and_load(landing_zone, formatted_zone, spark)
    spark.stop()