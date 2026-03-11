import os
import random

class Generador:
    
    def __init__(self, nClausuras, nVariables):
        self.nClausuras = nClausuras
        self.nVariables = nVariables

    def cambiar_nClausuras(self, n):
        self.nClausuras = n

    def cambiar_nVariables(self, n):
        self.nVariables = n

    def generar_ficheros(self, nFicheros: int):
        carpeta = 'problemas'
        os.makedirs(carpeta, exist_ok=True)  # Crea la carpeta si no existe

        for i in range(1, nFicheros + 1):
            nombre_fichero = os.path.join(
                carpeta,
                f"PROBLEM_{self.nClausuras:03}_{self.nVariables:03}_{i}.txt"
            )
            with open(nombre_fichero, 'w') as f:
                for _ in range(self.nClausuras):
                    variables_disponibles = list(range(1, self.nVariables + 1))
                    seleccion = random.sample(variables_disponibles, 3)
                    clausula = [num * random.choice([-1, 1]) for num in seleccion]
                    clausula = sorted(clausula, key=abs)  # ordena por valor absoluto
                    clausula_str = [str(num) for num in clausula]
                    clausula_str.append('0')
                    f.write(' '.join(clausula_str) + '\n')
    
    def eliminar_problemas(self):
        # Ruta a la carpeta 'problemas'
        carpeta_problemas = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "problemas"))

        # Recorremos todos los archivos de la carpeta
        for nombre_archivo in os.listdir(carpeta_problemas):
            ruta_archivo = os.path.join(carpeta_problemas, nombre_archivo)

            # Verificamos que sea un archivo antes de eliminarlo
            if os.path.isfile(ruta_archivo):
                os.remove(ruta_archivo)
                # print(f"Archivo eliminado: {nombre_archivo}")
                
    def eliminar_problemas_por_dimensiones(self, nClausuras, nVariables):
        carpeta_problemas = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "problemas"))

        prefijo = f"PROBLEM_{nClausuras:03}_{nVariables:03}_"

        for nombre_archivo in os.listdir(carpeta_problemas):
            if nombre_archivo.startswith(prefijo):
                ruta_archivo = os.path.join(carpeta_problemas, nombre_archivo)
                if os.path.isfile(ruta_archivo):
                    os.remove(ruta_archivo)