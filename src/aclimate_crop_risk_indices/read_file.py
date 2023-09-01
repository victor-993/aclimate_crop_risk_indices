import pandas as pd
import os
import multiprocessing

# Función para leer un lote de archivos CSV en paralelo
def read_csv_batch(filenames):
    data_frames = []
    for filename in filenames:
        data_frames.append(pd.read_csv(filename))
    return data_frames

if __name__ == '__main__':
    filenames = [archivo for archivo in os.listdir(carpeta) if archivo.endswith('.csv')]
    batch_size = 10  # Tamaño del lote
    
    # Dividir la lista de archivos en lotes
    batches = [filenames[i:i + batch_size] for i in range(0, len(filenames), batch_size)]
    
    # Crear un grupo de procesos
    pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())  # Usa todos los núcleos disponibles
    
    # Leer los lotes en paralelo y combinar los resultados en una lista única
    all_data_frames = []
    for batch in batches:
        result = pool.apply_async(read_csv_batch, (batch,))
        all_data_frames.extend(result.get())
    
    # Cierra el grupo de procesos
    pool.close()
    pool.join()