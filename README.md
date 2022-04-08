# waterqa: Water quality analyses using spark

This repositroy contains s the results of descriptive and predictive analyses on the KU-MWQ
dataset. These analyses have been implemented in two separate Container-based and VM-
based clustered environments.

# Implementation Infrastructure
**Three Nodes Container-Based Cluster Using Docker Compose** 

In this section, a three-node cluster has been set up using Docker Compose, whose main
purpose is to execute Jupyter notebooks containing the result visualization.
Actually, I have created these docker images (2 Spark nodes and 1 Jupyter node) so that .ipynb
files can be executed by you on spark and you can see the results.

- Install docker and docker-compose packages:
Sudo apt install docker.io docker-compose

- Go to the docker_running directory and run the script
Sudo chmod +x run_water_quality_analysis.sh
Sudo ./run_water_quality_analysis.sh start

- Simply open the output URL in a web browser to start Jupyter
Run the notebooks

- To stop the containers just run the following command:
Sudo ./run_water_quality_analysis.sh start stop
