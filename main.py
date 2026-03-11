import os
import sys 

# Agrega el directorio raíz del proyecto al sys.path
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from pathlib import Path
from Analizador import Analizador
from Framework import Runner, JSONLResultSink, MultiSink, build_algorithms
from Fuerza_bruta_optimizado import Fuerza_bruta_optimizado  # necesario para registrar el algoritmo
from Fuerza_bruta import Fuerza_bruta
from Generador import Generador


def main():
    generador = Generador(5,5) # Crear un generador con 5 clausuras y 5 variables, se pueden cambiar estos valores para generar problemas más complejos o más simples
    generador.eliminar_problemas() # Eliminar archivos anteriores
    generador.generar_ficheros(10) # Generar 10 problemas con las dimensiones especificadas en el generador
    
    # for i in range(5, 31, 5):
    #     generador.cambiar_nClausuras(i)
    #     generador.generar_ficheros(10) # Generar más problemas con el nuevo
        
    carpeta = Path("problemas") # Carpeta donde se encuentran los archivos de problemas
    algorithms = build_algorithms(["fuerza_bruta"]) 
    # algorithms = build_algorithms(["fuerza_bruta", "fuerza_bruta_optimizado"]) 
    runner = Runner(algorithms, n_jobs=1) # Crear el runner con los algoritmos disponibles y n_jobs=1 para ejecución secuencial
    results = runner.run_directory(carpeta, pattern="*.txt") # Ejecutar los algoritmos en todos los archivos de la carpeta "problemas" que terminen con .txt

    analizador = Analizador()

    # MultiSink guarda en JSONL Y analiza a la vez
    sink = MultiSink(JSONLResultSink(Path("resultados.jsonl")), analizador)
    sink.write_all(results)

    analizador.imprimir_resumen()
    print("Resultados guardados en resultados.jsonl") # Imprimir mensaje de confirmación

if __name__ == "__main__":
    main()