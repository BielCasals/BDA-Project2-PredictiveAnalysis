"""
Consume clean data from the exploitation zone to train 
predictive models and track the experiments using MLflow
Upgraded with Optuna to automatically train and select best hyperparameter tuning efficiently
Implementing B.1 and B.2 in the same script
"""
import os
import mlflow
import mlflow.spark
import optuna
from optuna.integration.mlflow import MLflowCallback

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, max, min


from config import PATHS, SPARK_CONFIG, SPARK_PACKAGES

def calculate_nrmse(predictions, target_col, prediction_col):
    """
    Calculate NRMSE for the given predictions DataFrame.
    NRMSE = RMSE / (Max - Min)
    """
    evaluator = RegressionEvaluator(labelCol=target_col, predictionCol=prediction_col, metricName="rmse")
    rmse = evaluator.evaluate(predictions)

    range_values = predictions.select(
        max(col(target_col)).alias("max_target"),
        min(col(target_col)).alias("min_target")
    ).collect()
    
    if not range_values:
        return float("inf")
        
    target_range = range_values[0]["max_target"] - range_values[0]["min_target"]

    nrmse = rmse / target_range if target_range != 0 else float("inf")
    return nrmse

def get_model_from_trial(trial_or_params):
    """
    Helper to initialize the correct model based on parameters.
    Works for both the 'trial' object (during optimization)
    and the 'best_params' dict (during retraining).
    """
    # check if in trial or in the retraining
    is_trial = hasattr(trial_or_params, "suggest_categorical")
    
    # 1. get model type
    if is_trial:
        model_type = trial_or_params.suggest_categorical("model_type", ["lr", "rf", "gbt"])
    else:
        model_type = trial_or_params["model_type"]

    # 2. Initialize model based on the type
    if model_type == "lr":
        if is_trial:
            reg_param = trial_or_params.suggest_float("lr_regParam", 0.001, 10.0, log=True)
            elastic_net = trial_or_params.suggest_float("lr_elasticNetParam", 0.0, 1.0)
        else:
            reg_param = trial_or_params["lr_regParam"]
            elastic_net = trial_or_params["lr_elasticNetParam"]
            
        return LinearRegression(featuresCol="features", labelCol="RDF_index", 
                                regParam=reg_param, elasticNetParam=elastic_net)

    elif model_type == "rf":
        if is_trial:
            num_trees = trial_or_params.suggest_int("rf_numTrees", 10, 100)
            max_depth = trial_or_params.suggest_int("rf_maxDepth", 5, 20)
        else:
            num_trees = trial_or_params["rf_numTrees"]
            max_depth = trial_or_params["rf_maxDepth"]
            
        return RandomForestRegressor(featuresCol="features", labelCol="RDF_index",
                                     numTrees=num_trees, maxDepth=max_depth, seed=42)

    elif model_type == "gbt":
        if is_trial:
            max_iter = trial_or_params.suggest_int("gbt_maxIter", 10, 100)
            max_depth = trial_or_params.suggest_int("gbt_maxDepth", 3, 10)
        else:
            max_iter = trial_or_params["gbt_maxIter"]
            max_depth = trial_or_params["gbt_maxDepth"]
            
        return GBTRegressor(featuresCol="features", labelCol="RDF_index",
                            maxIter=max_iter, maxDepth=max_depth, seed=42)


def objective(trial, vector_assembler, scaler, train_data, rmse_evaluator):
    """
    Objective function on the training to compare model performance 
    Returns the evaluation of the model on a validation set of the train_data
    """

    #get model using the function
    model = get_model_from_trial(trial)

    # defining pipeline
    pipeline = Pipeline(stages=[vector_assembler, scaler, model])

    #splitting the training on train and validation
    train_sub, val_sub = train_data.randomSplit([0.7, 0.3], seed=trial.number)
    
    model_fit = pipeline.fit(train_sub)
    preds = model_fit.transform(val_sub)
    
    return rmse_evaluator.evaluate(preds)


def run_ml_pipeline(exploitation_path: str, spark: SparkSession):
    """
    Orchestrate Training.
    """
    #loading data
    try:
        data = spark.read.format("delta").load(exploitation_path)
        print("Data loaded successfully")
    except Exception as e:
        print(f"Error loading data: {e}")
        return
    
    local_dir = os.path.dirname(os.path.abspath(__file__))

    tracking_uri = os.path.join(local_dir, "mlruns")
    mlflow.set_tracking_uri(tracking_uri)
    print("saving in", tracking_uri)

    #cleaning data, dropping NA just in case
    df_clean = data.na.drop(subset=["RDF_index", "net_density", "ratio_immigrants", "population", "ratio_unemployed"])

    #assembling vector 
    vector_assembler = VectorAssembler(
        inputCols=["net_density", "ratio_immigrants", "population", "ratio_unemployed"],
        outputCol="unscaled_features"
    )
    
    # defining the scaler
    scaler = StandardScaler(inputCol="unscaled_features", outputCol="features", withStd=True, withMean=False)

    #splitting data 
    train_data, test_data = df_clean.randomSplit([0.8, 0.2], seed=42)
    
    #evaluators
    rmse_evaluator = RegressionEvaluator(labelCol="RDF_index", predictionCol="prediction", metricName="rmse")
    r2_evaluator = RegressionEvaluator(labelCol="RDF_index", predictionCol="prediction", metricName="r2")
    
    mlflow.set_experiment("Income_Prediction")


    # starting run
    with mlflow.start_run(run_name="Optuna_Optimizer"):
        print("Starting Optuna Optimization (LR vs RF vs GBT)...")

        # MLflow integration
        mlflow_callback = MLflowCallback(
            metric_name="rmse",
            create_experiment=False,
            mlflow_kwargs={"nested": True}
        )

        #creating study
        study = optuna.create_study(direction="minimize")
        #starting the optimizers and defining the number of trials
        study.optimize(
            lambda trial: objective(trial, vector_assembler, scaler, train_data, rmse_evaluator),
            n_trials=25, 
            callbacks=[mlflow_callback])

        print(f"Winner Model & Params: {study.best_params}")
        print(f"Best Internal RMSE: {study.best_value}")

        
        print("Retraining the winning model on full training data...")
        
        # recreate the winner using the helper function
        final_model_obj = get_model_from_trial(study.best_params)
        
        final_pipeline = Pipeline(stages=[vector_assembler, scaler, final_model_obj])
        #it fits the model and gets the scaler right for the test part
        final_model_fit = final_pipeline.fit(train_data)

        # final evaluation
        predictions = final_model_fit.transform(test_data)
        
        predictions.toPandas().to_csv("predictions.csv", index=False)

        final_rmse = rmse_evaluator.evaluate(predictions)
        final_r2 = r2_evaluator.evaluate(predictions)
        final_nrmse = calculate_nrmse(predictions, target_col="RDF_index", prediction_col="prediction")

        print(f"Final Test RMSE: {final_rmse}")
        print(f"Final Test R2: {final_r2}")

        # logging all the models studied
        mlflow.log_params(study.best_params)
        mlflow.log_metrics({
            "test_RMSE": final_rmse,
            "test_R2": final_r2,
            "test_nrmse": final_nrmse
        })
        
        # saving the best model 
        mlflow.spark.log_model(final_model_fit, "best_model_smart")
        print("Best model saved to MLflow.")

if __name__ == "__main__":
    
    if not 'spark' in globals():
        spark = SparkSession.builder \
            .appName("BarcelonaIncomePrediction") \
            .master("local[*]") \
            .config("spark.jars.packages", ",".join(SPARK_PACKAGES)) \
            .config("spark.sql.extensions", SPARK_CONFIG["spark.sql.extensions"]) \
            .config("spark.sql.catalog.spark_catalog", SPARK_CONFIG["spark.sql.catalog.spark_catalog"]) \
            .getOrCreate()

    exploitation_zone = PATHS["exploitation_zone"]

    run_ml_pipeline(exploitation_zone, spark)
    
    
