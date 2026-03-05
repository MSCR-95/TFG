import os
import sys 

# Agrega el directorio raíz del proyecto al sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from pathlib import Path
from codigo.Framework import Runner, JSONLResultSink, build_algorithms
from codigo.Fuerza_bruta import Fuerza_bruta  # necesario para registrar el algoritmo
from codigo.Generador import Generador


def main():
    generador = Generador() # Crear instancia del generador
    generador.eliminar_archivos_problemas() # Eliminar archivos anteriores
    generador.generar_ficheros(20) 

    carpeta = Path("problemas") # Carpeta donde se encuentran los archivos de problemas
    algorithms = build_algorithms(["fuerza_bruta"]) # Construir el diccionario de algoritmos disponibles, nos permite agregar más algoritmos sin cambiar esta parte del código, solo importarlos y registrarlos en su módulo correspondiente
    runner = Runner(algorithms, n_jobs=1) # Crear el runner con los algoritmos disponibles y n_jobs=1 para ejecución secuencial
    results = runner.run_directory(carpeta, pattern="*.txt") # Ejecutar los algoritmos en todos los archivos de la carpeta "problemas" que terminen con .txt

    sink = JSONLResultSink(Path("resultados.jsonl")) # Crear un sink para guardar los resultados en un archivo JSONL
    sink.write_all(results) # Guardar los resultados en el archivo JSONL

if __name__ == "__main__":
    main()