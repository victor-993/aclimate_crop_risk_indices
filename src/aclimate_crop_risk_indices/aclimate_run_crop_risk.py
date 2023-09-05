import os
import argparse
from aclimate_crop_risk_indices.crops_risk import CropsRisk
import time

def main():
    # Params
    # 0: Crop
    # 1: Path root
    # 2: Number of stations

    parser = argparse.ArgumentParser(description="Script for calculating crop risk indices")

    parser.add_argument("-c", "--crop", help="Crop name", required=True)
    parser.add_argument("-C", "--country", help="Country name", required=True)
    parser.add_argument("-p", "--path", help="Path to data directory", default=os.getcwd())
    parser.add_argument("-s", "--stations", type=int, help="Number of stations to be executed in parallel", default=2)

    args = parser.parse_args()

    print("Reading inputs")

    crop = args.crop
    path = args.path
    country = args.country
    stations = args.stations

    start_time = time.time()

    #llamado a la funcion que paralelizara todo
    para = CropsRisk(path, stations, crop, country)
    para.run()

    end_time = time.time()

    # Calcular el tiempo transcurrido
    elapsed_time = end_time - start_time

    # Imprimir el tiempo transcurrido en horas, minutos y segundos
    hours = int(elapsed_time // 3600)
    minutes = int((elapsed_time % 3600) // 60)
    seconds = int(elapsed_time % 60)
    print(f"Tiempo transcurrido: {hours} horas, {minutes} minutos, {seconds} segundos")

if __name__ == "__main__":
    main()