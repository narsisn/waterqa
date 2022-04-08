import pandas as pd
from torch.utils.data import Dataset, DataLoader
from sklearn.preprocessing import MinMaxScaler
import os
import torch
import numpy as np
import random
import matplotlib.pyplot as plt
from joblib import dump
from icecream import ic

class SensorDataset(Dataset):


    def __init__(self, csv_name, root_dir, training_length, forecast_window):
        """
        Args:
            csv_file (string): Path to the csv file.
            root_dir (string): Directory
        """
        print(csv_name)
        # load raw data file
        csv_file = os.path.join(root_dir, csv_name)
        self.df = pd.read_csv(csv_file)
        self.root_dir = root_dir
        self.transform = MinMaxScaler()
        self.T = training_length
        self.S = forecast_window
        self.number = 1

    def __len__(self):
        # return number of sensors
        return len(self.df.groupby(by=["reindexed_id"]))

    # Will pull an index between 0 and __len__. 
    def __getitem__(self, idx):
        
        # Sensors are indexed from 1
        idx = idx+1
  
        start = self.df[self.df['reindexed_id']==idx].index.values.astype(int)[0] +2
        sensor_number =idx
        index_in = torch.tensor([i for i in range(start, start+self.T)])
        index_tar = torch.tensor([i for i in range(start + self.T, start + self.T + self.S)])
        _input = torch.tensor(self.df[self.df["reindexed_id"]==idx][["Temperature", "pH", "Turbidity","Humidity","Night","Quality"]][:24].values)
        
        target = torch.tensor(self.df[self.df["reindexed_id"]==idx][["Temperature", "pH", "Turbidity","Humidity","Night","Quality"]][12:].values)
        scaler = self.transform
        scaler.fit(_input[:,0].unsqueeze(-1))
        _input[:,0] = torch.tensor(scaler.transform(_input[:,0].unsqueeze(-1)).squeeze(-1))
        target[:,0] = torch.tensor(scaler.transform(target[:,0].unsqueeze(-1)).squeeze(-1))

        # save the scalar to be used later when inverse translating the data for plotting.
        dump(scaler, 'scalar_item.joblib')

        return index_in, index_tar, _input, target, sensor_number