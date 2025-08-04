import os
import random

class Generador:
    
    def __init__(self, nClausuras=3, nTerminos=5):
        self.nClausuras = nClausuras
        self.nTerminos = nTerminos

    def cambiar_nClausuras(self, n):
        self.nClausuras = n

    def cambiar_nTerminos(self, n):
        self.nTerminos = n

    def generar_ficheros(self, nFicheros: int):
        carpeta = 'problemas'
        os.makedirs(carpeta, exist_ok=True)  # Crea la carpeta si no existe

        for i in range(1, nFicheros + 1):
            nombre_fichero = os.path.join(
                carpeta,
                f"PROBLEM_{self.nClausuras:03}_{self.nTerminos:03}_{i}.txt"
            )
            with open(nombre_fichero, 'w') as f:
                for _ in range(self.nClausuras):
                    terminos_disponibles = list(range(1, self.nTerminos + 1))
                    seleccion = random.sample(terminos_disponibles, 3)
                    clausula = [num * random.choice([-1, 1]) for num in seleccion]
                    clausula = sorted(clausula, key=abs)  # ordena por valor absoluto
                    clausula_str = [str(num) for num in clausula]
                    clausula_str.append('0')
                    f.write(' '.join(clausula_str) + '\n')