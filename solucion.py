import os
from Fuerza_bruta import Fuerza_bruta
from Generador import Generador

# Función para leer el archivo y extraer las condiciones
def leer_terminos(archivo):
    with open(archivo, 'r') as f:
        lineas = f.readlines()
    return [linea.strip().split() for linea in lineas]

def main():
    n_ficheros = 5
    generador1 = Generador()
    generador1.generar_ficheros(n_ficheros)
    # Carpeta donde están los ficheros
    carpeta_problemas = os.path.join(os.path.dirname(__file__), "problemas")
    todos_los_terminos=[]
    
    for i in range(1, n_ficheros+1):
        nombre_fichero = f"PROBLEM_003_005_{i}.txt"
        ruta_fichero = os.path.join(carpeta_problemas, nombre_fichero)
        
        terminos = leer_terminos(ruta_fichero)
        todos_los_terminos.append(terminos)


    soluciones = []
    fuerzaBruta = Fuerza_bruta()

    for i in range(0, n_ficheros):
        soluciones.append(fuerzaBruta.resolver(todos_los_terminos[i]))
        print("Solucion de fichero PROBLEM_003_005_",i+1,".txt")
        if soluciones:
            #print(soluciones)
             print([soluciones[i][0][j] for j in range(1, 6)]) # para mostrar la solucion
        else:
            print("No se encontraron soluciones que satisfagan todas las condiciones.")

    

if __name__ == "__main__":
    main()