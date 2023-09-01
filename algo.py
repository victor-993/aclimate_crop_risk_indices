import os
import pandas as pd

if __name__ == "__main__":
    file_config = os.path.join(os.getcwd(), "parameters.csv")
    df = pd.read_csv(file_config)

    path_outputs_stations = os.path.join(os.getcwd(), 'data',"outputs","prediccionClimatica", "resampling")
    path_inputs_crop = os.path.join(os.getcwd(), 'data',"inputs", "cultivos", 'perenne')

    for folder_name in os.listdir(path_outputs_stations):
        new_folder=f"{folder_name}_60a16e2826e98d13b8dbb878_6334a6d230243c12cc1fa8c3_3"
        os.makedirs(os.path.join(path_inputs_crop, new_folder))
        df.to_csv(os.path.join(path_inputs_crop, new_folder, 'crop_conf.csv'))