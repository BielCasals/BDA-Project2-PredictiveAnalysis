"""
Resolves relative paths problems between the spark scripts
"""
import os

# root of the project relative to this file
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

PATHS = {
    "root": PROJECT_ROOT,
    "datasets": os.path.join(PROJECT_ROOT, "Datasets/datasets"),
    "landing_zone": os.path.join(PROJECT_ROOT, "landingzone"),
    "formatted_zone": os.path.join(PROJECT_ROOT, "formattedzone"),
    "exploitation_zone": os.path.join(PROJECT_ROOT, "exploitationzone")
}


# version control of all Spark Packages
SPARK_PACKAGES = [
    "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0",
    "io.delta:delta-core_2.12:2.4.0"
]

SPARK_CONFIG = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.hadoop.fs.file.impl": "org.apache.hadoop.fs.RawLocalFileSystem"
}

# database credentials
MONGODB_CONFIG = {
    "host": "127.0.0.1",
    "port": 27017,
    "db_name": "formattedzone",
    "uri": "mongodb://127.0.0.1:27017/formattedzone"
}