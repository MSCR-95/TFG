import os
import sys

from pathlib import Path
from Analizador import Analizador
from Conversor import Conversor
from Framework import Runner, JSONLResultSink, MultiSink, build_algorithms
from Fuerza_bruta_optimizado import Fuerza_bruta_optimizado  # necesario para registrar el algoritmo
from Fuerza_bruta import Fuerza_bruta
from Generador import Generador


def main():
    # ------------------------------------------------------------------
    # 1. Generar problemas 3-SAT
    # ------------------------------------------------------------------
    carpeta_3sat = Path("problemas_3sat")
    carpeta_2sat = Path("problemas")

    generador = Generador(3)  # 3 literales por cláusula → 3-SAT

    # Limpiar ficheros anteriores
    for carpeta in [carpeta_3sat, carpeta_2sat]:
        carpeta.mkdir(exist_ok=True)
        for f in carpeta.glob("*.txt"):
            f.unlink()

    # generador.cambiar_nVariables(5)
    # for i in range(5, 31, 5):
    #     generador.cambiar_nClausuras(i)
    #     generador.generar_ficheros(10, carpeta_3sat)  # genera en carpeta_3sat

    # # ------------------------------------------------------------------
    # # 2. Convertir 3-SAT → Max-2-SAT
    # # ------------------------------------------------------------------
    # conversor = Conversor(carpeta_salida=str(carpeta_2sat))
    # conversor.convertir_carpeta(carpeta_3sat)
    
    
    ##############################################################################
    # Vamos a crear directamente los problemas Max-2-SAT 
    generador = Generador(2)  # 2 literales por cláusula → Max-2-SAT
    generador.cambiar_nVariables(5)
    for n_clausulas in [5, 10, 15, 20, 22, 24, 26, 28, 30]:
        generador.cambiar_nClausulas(n_clausulas)
        generador.generar_ficheros(10, carpeta_2sat) # genera directamente en carpeta_2sat

    # ------------------------------------------------------------------
    # 3. Ejecutar algoritmos sobre los ficheros Max-2-SAT
    # ------------------------------------------------------------------
    # algorithms = build_algorithms(["fuerza_bruta", "fuerza_bruta_optimizado"])
    algorithms = build_algorithms(["fuerza_bruta"])  
    n_jobs = 5
    runner = Runner(algorithms, n_jobs=n_jobs, backend='threading', verbose=True)
    results = runner.run_directory(carpeta_2sat, pattern="*.txt")

    # ------------------------------------------------------------------
    # 4. Analizar y guardar resultados
    # ------------------------------------------------------------------
    analizador = Analizador(n_jobs=n_jobs)
    sink = MultiSink(JSONLResultSink(Path("resultados.jsonl")), analizador)
    sink.write_all(results)
    analizador.imprimir_resumen()
    print("Resultados guardados en resultados.jsonl")


if __name__ == "__main__":
    main()