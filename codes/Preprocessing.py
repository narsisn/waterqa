
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import lit ,row_number,col, monotonically_increasing_id, when

import numpy as np
import pandas as pd
from scipy import stats
from scipy import signal
from statsmodels.tsa.seasonal import seasonal_decompose
import datetime


# preprocessing Class 

class PreprocessingClass():
    def __init__(self):
        # Define Constants 
        self.night = [(117,810), (1446,2169), (2856,3633), (4411,5155), (5886,6553), (7190,7832), (8477,9077)]
        self.rainy = [(6149,7461)] 
        # proportionality constant and taken as; 1.8615
        self.proConstant = 1.8615
        self.standardTemp = 23.17
        self.standardPH = 7.0
        self.standardTurb = 198
        self.headers = ['Temperature','pH','Turbidity','Quality']
        self.variables = ['Humidity','Night']
    
        # Calculating water quality sum(wi*value), wi=I/Si
    def cal_water_quality_index(self,rawData):
        rawData = rawData.withColumn("sTemp", lit(self.standardTemp))
        rawData = rawData.withColumn("sPH", lit(self.standardPH))
        rawData = rawData.withColumn("sTurb", lit(self.standardTurb))
        qualityUdf =F.udf(self.cal_quality, FloatType())
        rawData=rawData.withColumn('Quality', qualityUdf(rawData.Temperature,rawData.pH,rawData.Turbidity,rawData.sTemp,rawData.sPH,rawData.sTurb))

        rawData = rawData.drop('index','sTemp','sPH','sTurb')
        return rawData
    
        # Row based processing using UDF function at pyspark  
    def cal_quality(self,Temp,PH,Trub,sTemp,sPH,sTurb):
        quality = Temp*(self.proConstant/sTemp) + PH*(self.proConstant/sPH) + Trub*(self.proConstant/sTurb)
        return quality
    
        # Adding Humidity:Dry=0/Rainy=1, Time: Day=0/Night=1, Quality Columns to raw data
    def add_new_columns(self,rawData):
        rawData = rawData.withColumn("Humidity", lit(0))
        rawData = rawData.withColumn("Night", lit(0))
        rawData = rawData.withColumn("index", monotonically_increasing_id()+2)
        for row in self.night:
            rawData = rawData.withColumn("Night", when(rawData.index.between(int(row[0]),int(row[1])),lit(1))\
                                        .otherwise(rawData.Night))
        for row in self.rainy:
            rawData = rawData.withColumn("Humidity", when(rawData.index.between(int(row[0]),int(row[1])),lit(1))\
                                        .otherwise(rawData.Humidity))
        return rawData
    
        # Data Downsampleing per minutes
    def downsample(self,rawData,minutes):
        resampledData = rawData.select(rawData.columns[:7]).toPandas()
        resampledData = resampledData.set_index('Date_Time')
        resampledData = resampledData.resample(str(minutes)+'T').mean()
        resampledData['Humidity'] = resampledData['Humidity'].fillna(0).round().astype(int)
        resampledData['Night'] = resampledData['Night'].fillna(0).round().astype(int)
        return resampledData
    
    def preprocessing_steps (self,rawData):
        # rename colname Temperature (°C) to Temperature 
        rawData = rawData.withColumnRenamed(rawData.columns[0], 'Date_Time')
        rawData = rawData.withColumnRenamed(rawData.columns[1], 'Temperature')
        rawData = rawData.withColumnRenamed(rawData.columns[3], 'Turbidity')
        # Adding Humidity:Dry=0/Rainy=1, Time: Day=0/Night=1, Quality Columns to raw data
        rawData = self.add_new_columns(rawData)
        rawData.count()
        rawData = self.cal_water_quality_index(rawData)
        rawData.count()
        return rawData
    
        # Data Downsampleing per minutes
    def downsample(self,rawData,minutes):
        resampledData = rawData.select(rawData.columns[:7]).toPandas()
        resampledData = resampledData.set_index('Date_Time')
        resampledData = resampledData.resample(str(minutes)+'T').mean()
        resampledData['Humidity'] = resampledData['Humidity'].fillna(0).round().astype(int)
        resampledData['Night'] = resampledData['Night'].fillna(0).round().astype(int)
        return resampledData
    

