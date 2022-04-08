""" 
Multivariate Time Series Anomaly Detection Using IsolationForest Algorithm.    
"""

# Spark Context Initialization 
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

import os
import numpy as np
import pandas as pd
from scipy import stats
from scipy import signal
from statsmodels.tsa.seasonal import seasonal_decompose
import datetime
import Preprocessing 

# Spark Session Ini
sc = SparkContext()
sqlc = SQLContext(sc)

# Loading Raw data 
def load_data(dataPath):
    dataPathSens30 = dataPath + "Sensor_data_for_30_cm.csv"
    rawDataSens30= sqlc.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(dataPathSens30)
    return rawDataSens30


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
    # Split Training Data
    x_train = downsampleData[['Temperature','pH','Turbidity']]
    x_train = x_train.values.tolist()
    # Initiating the Model
    classifier = IsolationForest(n_estimators=100, max_samples='auto', contamination=0.3, random_state=200, n_jobs=-1)
    # Model Training 
    clf = classifier.fit(x_train)
    # prediction  Normal = 1 and Anomaly = 0
    prediction =clf.predict(x_train)
    downsampleData=downsampleData
    downsampleData['Prediction'] = prediction.tolist()
    print("The Prediction Results is : ", downsampleData)
    # Save the results 
    downsampleData = downsampleData.reset_index()
    print(downsampleData)
    sparkDF=sqlc.createDataFrame(downsampleData)
    sparkDF.coalesce(1).write.mode('overwrite').option("header", "true").csv(outPath + "Anomaly_Results.csv",sep=',')



    

