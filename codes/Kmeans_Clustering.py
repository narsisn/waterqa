""" 
Time Series Clustering Using Kmeans Algorithm.    
"""

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.types import *
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.sql.functions import lit ,row_number,col, monotonically_increasing_id, when
from sklearn.ensemble import IsolationForest
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

sc = SparkContext()
sqlc = SQLContext(sc)

import numpy as np
import pandas as pd
import datetime
import Preprocessing 



# Loading Raw data 
def load_data(dataPath):
    dataPathSens30 = dataPath + "Sensor_data_for_30_cm.csv"
    rawDataSens30= sqlc.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(dataPathSens30)
    return rawDataSens30

# preparing training data 
def train_data_preparing(downsampleData):
    downsampleData = downsampleData.reset_index()
    downsampleData = sqlc.createDataFrame(downsampleData)
    downsampleData = downsampleData.withColumn("id", monotonically_increasing_id())
    train = downsampleData.select('id','Temperature','pH','Turbidity')
    # Vector Assembling
    vecAssembler = VectorAssembler(inputCols=train.columns[1:], outputCol="features")
    trainDF = vecAssembler.transform(train).select('id', 'features')
    return trainDF , downsampleData

# Model Training and Evaluation 
def train(trainDF):
    kmeans = KMeans().setK(3).setSeed(5)
    model = kmeans.fit(trainDF)
    # Model Evaluation
    predictionsEvaluate = model.transform(trainDF)
    evaluator = ClusteringEvaluator()
    silhouette = evaluator.evaluate(predictionsEvaluate)
    centers = model.clusterCenters()
    return model, silhouette , centers 

# Clustering 

def data_clustering(trainDF,downsampleData ):
    predictions = model.transform(trainDF).select('id', 'prediction')
    resultRows = predictions.collect()
    predictionDF = sqlc.createDataFrame(resultRows)
    predictionDF = downsampleData.join(predictionDF,'id').drop('id').sort('Date_Time')
    return predictionDF

if __name__ == '__main__':

    # Output Path: Writing Data to HDFS, please change the file directory 
    outPath = 'hdfs://node1.sepahtan:8020/data/results/'
    pre = Preprocessing.PreprocessingClass()
    # Reading data from HDFS, please change the file directory 
    dataPath = 'hdfs://node1.sepahtan:8020/data/'
    rawDataSens30 = load_data(dataPath)
    # Preprocessing
    rawDataSens30 = pre.preprocessing_steps(rawDataSens30)
    # downsampleing data 
    downsampleData = pre.downsample(rawDataSens30,60)
    # Train Data
    trainDF , downsampleData = train_data_preparing(downsampleData)
    model, sed, centers = train(trainDF)
    print("Silhouette with squared euclidean distance = " + str(sed))
    print("Cluster Centers = " + str(centers))
    # Clustering 
    predictionDF = data_clustering(trainDF,downsampleData)
    # Save clustering result at hdfs csv file 
    predictionDF.coalesce(1).write.mode('overwrite').option('header','true').csv(outPath+'Clustering_Result.csv',sep=',')






