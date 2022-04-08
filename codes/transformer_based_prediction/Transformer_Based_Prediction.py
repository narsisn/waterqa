import argparse
# from train_teacher_forcing import *
from train_with_sampling import *
from DataLoader import *
from torch.utils.data import DataLoader
import torch.nn as nn
import torch
from helpers import *


if __name__ == "__main__":

    # Initializing  
    epoch: int = 1000
    k: int = 1
    batch_size: int = 1
    frequency: int = 100
    training_length =24
    forecast_window = 12
    train_csv = "Sensor_data_for_30_cm_per.csv"
    path_to_save_model = "save_model/"
    path_to_save_loss = "save_loss/"
    path_to_save_predictions = "save_predictions/"
    device = "cpu"
    # Create and clean directories 
    clean_directory()
    # run the code
    train_dataset = SensorDataset(csv_name = train_csv, root_dir = "Data/", training_length = training_length, forecast_window = forecast_window)
    train_dataloader = DataLoader(train_dataset, batch_size=1, shuffle=True)
    best_model = transformer(train_dataloader, epoch, k, frequency, path_to_save_model, path_to_save_loss, path_to_save_predictions, device)
    

