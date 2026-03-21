import os
import random
from pathlib import Path


class Generador:

    def __init__(self, nSat):
        self.nSat = nSat
        self.nClausuras = 0
        self.nVariables = 0

    def cambiar_nClausulas(self, n):
        self.nClausuras = n

    def cambiar_nVariables(self, n):
        self.nVariables = n

    def generar_ficheros(self, nFicheros: int, carpeta: str | Path = "problemas"):
        """Genera nFicheros ficheros SAT en la carpeta indicada.

        Args:
            nFicheros: número de ficheros a generar.
            carpeta:   directorio de destino (se crea si no existe).
        """
        carpeta = Path(carpeta)
        carpeta.mkdir(parents=True, exist_ok=True)

        for i in range(1, nFicheros + 1):
            nombre_fichero = carpeta / f"PROBLEM_{self.nClausuras:03}_{self.nVariables:03}_{i}.txt"
            with nombre_fichero.open("w") as f:
                for _ in range(self.nClausuras):
                    variables_disponibles = list(range(1, self.nVariables + 1))
                    seleccion = random.sample(variables_disponibles, self.nSat)
                    clausula = [num * random.choice([-1, 1]) for num in seleccion]
                    clausula = sorted(clausula, key=abs)
                    clausula_str = [str(num) for num in clausula]
                    clausula_str.append("0")
                    f.write(" ".join(clausula_str) + "\n")

    def eliminar_problemas(self, carpeta: str | Path = "problemas"):
        """Elimina todos los ficheros de la carpeta indicada."""
        carpeta = Path(carpeta)
        if not carpeta.exists():
            return
        for archivo in carpeta.iterdir():
            if archivo.is_file():
                archivo.unlink()

    def eliminar_problemas_por_dimensiones(
        self, nClausuras: int, nVariables: int, carpeta: str | Path = "problemas"
    ):
        """Elimina los ficheros que coincidan con las dimensiones dadas."""
        carpeta = Path(carpeta)
        prefijo = f"PROBLEM_{nClausuras:03}_{nVariables:03}_"
        for archivo in carpeta.iterdir():
            if archivo.name.startswith(prefijo) and archivo.is_file():
                archivo.unlink()