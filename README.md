# waterqa: Water quality analyses using spark

This repositroy contains s the results of descriptive and predictive analyses on the KU-MWQ dataset. These analyses have been implemented in two separate Container-based and VM-based clustered environments.

# Implementation Infrastructure
**1.Three Nodes Container-Based Cluster Using Docker Compose** 

In this section, a three-node cluster has been set up using Docker Compose, whose main purpose is to execute Jupyter notebooks containing the result visualization.
Actually, I have created these docker images (2 Spark nodes and 1 Jupyter node) so that .ipynb files can be executed by you on spark and you can see the results.

- Install docker and docker-compose packages:
Sudo apt install docker.io docker-compose

- Go to the docker_running directory and run the script
Sudo chmod +x run_water_quality_analysis.sh
Sudo ./run_water_quality_analysis.sh start

- Simply open the output URL in a web browser to start Jupyter
Run the notebooks

- To stop the containers just run the following command:
Sudo ./run_water_quality_analysis.sh start stop

**2.Use PySpark in Jupyter**
Load a regular Jupyter Notebook and load PySpark using the FindSpark package

- Install FindSpark Package: Pip3 install findspark

- Import findSpark and use findSpark.init() or findSpark.find(), I have already added this statement to my code, so there is no need to add it again. But just change the Spark HOME path

- Open the Exploratory_Data_Analysis.ipynb file and run it. Please note that you should change the path of the dataset directory from HDFS to your path.
# Machine Learning-Based Analyses (Diagnostic&Predictive) 

**Multivariate Anomaly Detection Using Isolation Forest Algorithm**

![image](https://user-images.githubusercontent.com/41056415/162408549-a08e02ea-5b93-4cfd-8943-7999741fb4f8.png)

**Water Quality Clustering Using MLlib KMeans Algorithms**

![image](https://user-images.githubusercontent.com/41056415/162408756-79acfcc8-0730-403c-9c98-df1b047cfad8.png)

**Multivariate Time Series Prediction Using Vanilla Transformer**

![image](https://user-images.githubusercontent.com/41056415/162408843-e4a10be4-86eb-4774-8a3c-cfea145c2db7.png)


