"""
Consume clean data from the exploitation zone to train 
predictive models and track the experiments using MLflow

Implementing B.1 and B.2 in the same script
"""
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, max, min

import mlflow
import mlflow.spark

from config import PATHS, SPARK_CONFIG, SPARK_PACKAGES

def calculate_nrmse(predictions, target_col, prediction_col):
    """
    Calculate NRMSE for the given predictions DataFrame.
    """
    # calculating RMSE
    evaluator = RegressionEvaluator(labelCol=target_col, predictionCol=prediction_col, metricName="rmse")
    rmse = evaluator.evaluate(predictions)

    #range of target values
    range_values = predictions.select(
        max(col(target_col)).alias("max_target"),
        min(col(target_col)).alias("min_target")
    ).collect()
    target_range = range_values[0]["max_target"] - range_values[0]["min_target"]

    #normalizing
    nrmse = rmse / target_range if target_range != 0 else float("inf")
    return nrmse


def log_grid_results(cvModel: CrossValidator, run_name: str):
    """Logs the details of each Model"""
    params_map = cvModel.getEstimatorParamMaps()
    metrics = cvModel.avgMetrics

    for params, metric in zip(params_map, metrics):
        with mlflow.start_run(run_name="Trial", nested=True):
            for param, value in params.items():
                mlflow.log_param(param.name, value)
            mlflow.log_metric("avg_cv_rmse", metric)
            mlflow.end_run()



def run_ml_pipeline(exploitation_path: str, spark: SparkSession):
    """
    Orchestrate the ML Training and Management.
    Reads from Delta Table -> Trains Models -> Logs to MLflow.
    """
    try:
        data = spark.read.format("delta").load(exploitation_path)
        print(f"Data loaded succesfully")
    except Exception as e:
        print(f"Error loading data: {e}")
        return
    
    #just in case dropping NA values
    df_clean = data.na.drop(subset=["RDF_index", "net_density", "ratio_immigrants", "population", "ratio_unemployed"])

    mlflow.set_experiment("Income_Prediction")

    # Assembling vector: To predict RDF_index in terms of net_density, ratio_immigrants and population  (add more if necessary)
    vector_assembler = VectorAssembler(
        inputCols=["net_density", "ratio_immigrants", "population", "ratio_unemployed"],
        outputCol="features"
    )

    #splitting data:
    train_data, test_data = df_clean.randomSplit([0.8, 0.2], seed=42)
    
    #defining evaluators for the models:
    rmse_evaluator = RegressionEvaluator(labelCol="RDF_index", predictionCol="prediction",
                              metricName="rmse")
    r2_evaluator = RegressionEvaluator(labelCol="RDF_index", predictionCol="prediction",
                              metricName="r2")
    
    # mlflow.set_tracking_uri(f"{PATHS['root']}/mlruns")
    # print("Tracking in: ", f"{PATHS['root']}/mlruns" )
    
    with mlflow.start_run(run_name="Linear_Regression_Group"):
        print("Tuning Linear Regression...")
        
        #model lr:
        lr = LinearRegression(featuresCol="features", labelCol="RDF_index")
        
        #Spark ML pipeline
        pipeline_lr = Pipeline(stages=[vector_assembler, lr])

        #defining hyperparamters:
        paramGrid_lr = ParamGridBuilder() \
            .addGrid(lr.regParam, [0.01, 0.1, 0.5]) \
            .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0]) \
            .build()

        cv_lr = CrossValidator(estimator=pipeline_lr,
                               estimatorParamMaps=paramGrid_lr,
                               evaluator=rmse_evaluator,
                               numFolds=3)

        #training 3*3*3=27 models:
        cvModel_lr = cv_lr.fit(train_data)

        #log Trials
        log_grid_results(cvModel_lr, "Linear_Regression")

        #getting best model
        best_lr = cvModel_lr.bestModel

        #evaluatting best model on test data
        predictions = best_lr.transform(test_data)
        final_rmse = rmse_evaluator.evaluate(predictions)
        final_r2 = r2_evaluator.evaluate(predictions)
        final_nrmse = calculate_nrmse(predictions, target_col="RDF_index", prediction_col="prediction")
             
        #log winner stats
        mlflow.log_metrics({"test_RMSE": final_rmse})
        mlflow.log_metrics({"test_R2": final_r2})
        mlflow.log_metrics({"test_rnmse": final_nrmse})

        mlflow.spark.log_model(best_lr, "best_model_lr")
    
    # with mlflow.start_run(run_name="Random_Forest_Group"):
    #     print("Training Random Forest...")
    #     rf = RandomForestRegressor(featuresCol="features", labelCol="RDF_index")
    #     pipeline_rf = Pipeline(stages=[vector_assembler, rf])

    #     #defining possible hyperparams
    #     paramGrid_rf = ParamGridBuilder() \
    #         .addGrid(rf.numTrees, [10, 50, 100]) \
    #         .addGrid(rf.maxDepth, [5, 10]) \
    #         .build()
        
    #     #crossvalidating for each possible combination with 3 folds 
    #     cv_rf = CrossValidator(estimator=pipeline_rf,
    #                            estimatorParamMaps=paramGrid_rf,
    #                            evaluator=rmse_evaluator,
    #                            numFolds=3)

    #     #fitting all combination of models
    #     cvModel_rf = cv_rf.fit(train_data)
        
    #     # logging all trials 
    #     log_grid_results(cvModel_rf, "Random_Forest")

    #     # getting best model 
    #     best_rf = cvModel_rf.bestModel
        
    #     predictions = best_rf.transform(test_data)
    #     final_rmse = rmse_evaluator.evaluate(predictions)
    #     final_r2 = r2_evaluator.evaluate(predictions)
    #     final_nrmse = calculate_nrmse(predictions, target_col="RDF_index", prediction_col="prediction")

    #     print(f"    Winner RMSE: {final_rmse}")

    #     mlflow.log_metric("test_rmse", final_rmse)
    #     mlflow.log_metric("test_r2", final_r2)
    #     mlflow.log_metrics({"test_rnmse": final_nrmse})

    #     mlflow.spark.log_model(best_rf, "best_model_rf")

    # with mlflow.start_run(run_name="GBT_Regressor_Group"):
    #     print("\n--> Tuning GBT Regressor...")
        
    #     gbt = GBTRegressor(featuresCol="features", labelCol="RDF_index")
    #     pipeline_gbt = Pipeline(stages=[vector_assembler, gbt])

    #     # Grid Search
    #     paramGrid_gbt = ParamGridBuilder() \
    #         .addGrid(gbt.maxIter, [20, 50]) \
    #         .addGrid(gbt.maxDepth, [3, 5, 7]) \
    #         .build()

    #     cv_gbt = CrossValidator(estimator=pipeline_gbt,
    #                             estimatorParamMaps=paramGrid_gbt,
    #                             evaluator=rmse_evaluator,
    #                             numFolds=3)

    #     cvModel_gbt = cv_gbt.fit(train_data)
    #     log_grid_results(cvModel_gbt, "GBT Regressor")

    #     best_gbt = cvModel_gbt.bestModel
    #     predictions = best_gbt.transform(test_data)
        
    #     final_rmse = rmse_evaluator.evaluate(predictions)
    #     final_r2 = r2_evaluator.evaluate(predictions)
    #     final_nrmse = calculate_nrmse(predictions, "RDF_index", "prediction")
        
    #     print(f"    Winner RMSE: {final_rmse:.4f}")
    #     print(f"    Winner R2: {final_r2:.4f}")

    #     mlflow.log_metric("test_rmse", final_rmse)
    #     mlflow.log_metric("test_r2", final_r2)
    #     mlflow.log_metric("test_nrmse", final_nrmse)
        
    #     mlflow.spark.log_model(best_gbt, "best_gbt_model")

    
    print("Pipeline Complete. All models trained and evaluated.")


if __name__ == "__main__":
    
    appName = "app"
    master = "local[6]" 

    if not 'spark' in globals():
        packages = ["io.delta:delta-core_2.12:2.4.0"]
        spark = SparkSession.builder \
            .appName("BarcelonaIncomePrediction") \
            .master("local[*]") \
            .config("spark.jars.packages", ",".join(SPARK_PACKAGES)) \
            .config("spark.sql.extensions", SPARK_CONFIG["spark.sql.extensions"]) \
            .config("spark.sql.catalog.spark_catalog", SPARK_CONFIG["spark.sql.catalog.spark_catalog"]) \
            .getOrCreate()

    exploitation_zone = PATHS["exploitation_zone"]
    run_ml_pipeline(exploitation_zone, spark)
    spark.stop()
    





