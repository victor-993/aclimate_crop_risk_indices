import pandas as pd
import glob
import os
import multiprocessing
from aclimate_crop_risk_indices.codigo_calculos_aclimate import main
from tqdm import tqdm

class CropsRisk():

    def __init__(self, path, cores, crop, country):
        self.crop = crop
        self.path = path
        self.cores = cores
        self.country = country
        self.configurations = []
        self.loaded_scenarios = {}
        self.path_inputs_crop = os.path.join(path, country,"inputs", "cultivos", crop)
        self.path_outputs_stations = os.path.join(path, country,"outputs","prediccionClimatica", "resampling")
        self.path_outputs_crop = os.path.join(path, country,"outputs", "cultivos", crop)
        self.path_inputs_crop = self.verify_path_exists(self.path_inputs_crop)
        self.path_outputs_stations = self.verify_path_exists(self.path_outputs_stations)
        self.path_outputs_crop = self.create_path_if_not_exists(self.path_outputs_crop)

    def verify_path_exists(self, path):
        if not os.path.exists(path):
            raise ValueError(f"The path '{path}' does not exist.")
        return path

    def create_path_if_not_exists(self, path):
        if not os.path.exists(path):
            os.makedirs(path)
        return path
    
    def read_configurations(self):

        for folder_name in tqdm(os.listdir(self.path_inputs_crop), desc=f"Processing data config:"):
            folder_path = os.path.join(self.path_inputs_crop, folder_name)
            
            if os.path.isdir(folder_path):
                
                partes = folder_name.split("_")
                
                ws = partes[0]
                cultivar = partes[1]
                soil = partes[2]
                frequency = partes[3]
                
                file_config = os.path.join(folder_path, "crop_conf.csv")
                df = pd.read_csv(file_config, index_col=False)

                self.configurations.append({
                    "id_estacion": ws,
                    "id_cultivar": cultivar,
                    "id_soil": soil,
                    "frecuencia": frequency,
                    "df_configuracion": df,
                    "file_name": folder_name,
                })

    def load_scenario(self, ws):
        
        if ws not in self.loaded_scenarios:
            path_station =  os.path.join(self.path_outputs_stations, ws)
            archivos_csv = glob.glob(os.path.join(path_station, '*.csv'))
            scenarios = {}
            for archivo in archivos_csv:
                nombre_archivo = os.path.basename(archivo)
                scenarios[nombre_archivo] = pd.read_csv(archivo)
            self.loaded_scenarios[ws] = scenarios

    def procesar(self, dato):

        self.load_scenario(dato["id_estacion"])

        result = main(self.loaded_scenarios[dato["id_estacion"]], dato["df_configuracion"], dato["id_estacion"], dato["id_cultivar"], dato["id_soil"])
        result.to_csv(os.path.join(self.path_outputs_crop, f"{dato['file_name']}.csv"), na_rep=-1, index=False)

    def run(self):       
        self.read_configurations()

        with multiprocessing.Pool(self.cores) as pool:
            pool.map(self.procesar, self.configurations)