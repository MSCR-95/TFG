# Función para leer el archivo y extraer las condiciones
def leer_condiciones(archivo):
    with open(archivo, 'r') as f:
        lineas = f.readlines()
    return [linea.strip().split() for linea in lineas]

# Función para evaluar una condición
def evaluar_condicion(condicion, valores):
    for literal in condicion:
        if literal == '0':  
            break
        var = abs(int(literal))  # Valor absoluto para obtener la variable
        valor = valores.get(var, 0)  # Si la variable no está en el diccionario, asumimos 0
        if (literal.startswith('-') and valor == 0) or (not literal.startswith('-') and valor == 1):
            return True  # La condición se cumple
    return False  # La condición no se cumple

# Función para generar todas las combinaciones posibles de valores
def generar_combinaciones(variables):
    from itertools import product
    return list(product([0, 1], repeat=len(variables)))

# Función principal
def main():
    archivo = "condiciones.txt"
    condiciones = leer_condiciones(archivo)

    # Obtener todas las variables únicas de las condiciones
    variables = set()
    for condicion in condiciones:
        for literal in condicion:
            if literal == '0':
                break
            var = abs(int(literal))
            variables.add(var)
    variables = sorted(variables)  # Ordenar las variables
    

    # Generar todas las combinaciones posibles de valores
    combinaciones = generar_combinaciones(variables)

    # Probar cada combinación
    soluciones = []
    for combinacion in combinaciones:
        valores = dict(zip(variables, combinacion))  # Asignar valores a las variables
        # Verificar si todas las condiciones se cumplen
        if all(evaluar_condicion(condicion, valores) for condicion in condiciones):
            soluciones.append(valores)

    # Mostrar las soluciones encontradas
    if soluciones:
        print("Soluciones encontradas:")
        for i, solucion in enumerate(soluciones, 1):
            print(f"Solución {i}: {solucion}")
    else:
        print("No se encontraron soluciones que satisfagan todas las condiciones.")

if __name__ == "__main__":
    main()