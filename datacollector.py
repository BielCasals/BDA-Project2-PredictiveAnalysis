"""
Should collect from datasets folder(simulating real world) and transport to "landingzone" folder
and be able to do it periodically without duplicating files already collected.
"""
import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from datetime import datetime
import json
from config import PATHS, SPARK_CONFIG      


     

def collect_data(source_path: str, landingzone_path: str, spark: SparkSession):
    print("executing data collection")
    categories = ["income", "density", "immigrants", "unemployment", "lookup_tables"]

    for category in categories:
        source_folder = os.path.join(source_path, category)
        landing_folder = os.path.join(landingzone_path, category)

        if not os.path.exists(landing_folder):
            os.makedirs(landing_folder)

        tracker_file = os.path.join(landing_folder, "_processed_log.json")
        processed_files = set()

        if os.path.exists(tracker_file):
            try:
                with open(tracker_file, 'r') as f:
                    processed_files = set(json.load(f))
            except:
                    print(f"[Warning] Could not read tracker file for {category}. Starting fresh.")


        # Get existing FOLDERS in landing zone to prevent duplicates
        all_files = [f for f in os.listdir(source_folder) if not f.startswith('.')]
        new_files = [f for f in all_files if f not in processed_files]

        if new_files:
            #getting batch_id based on the actual date, to be able to locate changes made each day
            batch_id = datetime.now()
            batch_folder = os.path.join(landing_folder, f"batch_{batch_id}")
            print(f"Found {len(new_files)} new files for category {category} \n Collecting ...")
            
            for file_name in new_files:
                source_file_path = os.path.join(source_folder, file_name)
                dst_file_path = os.path.join(batch_folder, file_name)
                try:
                    #checking type of file
                    if file_name.endswith(".csv"):
                        print(f"Reading CSV: {file_name}")
                        df = spark.read.csv(source_file_path, header=True, inferSchema=True)
                        df.write.csv(dst_file_path, header=True, mode="overwrite")
                    elif file_name.endswith(".json"):
                        df = spark.read.json(source_file_path, multiLine=True)
                        df.write.json(dst_file_path, mode="overwrite")
                    
                    processed_files.add(file_name)
                except:
                    print(f"Successfully ingested to: {dst_file_path}")
            
            #updating tracker file
            with open(tracker_file, "w") as f:
                json.dump(list(processed_files), f, indent=4)
        else:
            print(f"No new files found for category: {category}")
            

  



if __name__ == "__main__":
    source_path = PATHS["datasets"]
    landingzone_path = PATHS["landing_zone"]
    appName = "app"
    master = "local[*]" #(*) for all cores
    if not 'spark' in globals(): 
        conf = SparkConf().setAppName(appName).setMaster(master)
        spark = SparkSession.builder \
            .config(conf=conf) \
            .config("spark.hadoop.fs.file.impl", SPARK_CONFIG["spark.hadoop.fs.file.impl"]) \
            .getOrCreate()
    collect_data(source_path, landingzone_path, spark)
    spark.stop()

      

