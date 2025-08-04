# Función para leer el archivo y extraer las condiciones
def leer_terminos(archivo):
    with open(archivo, 'r') as f:
        lineas = f.readlines()
    return [linea.strip().split() for linea in lineas]

# Función para evaluar un termino FUERZABRUTA
def evaluar_condicion(termino, valores):
    for literal in termino:
        if literal == '0':  # Fin del termino
            break
        var = abs(int(literal))  # Valor absoluto para obtener la variable
        valor = valores.get(var, 0)  # Si la variable no está en el diccionario, asumimos 0
        if (literal.startswith('-') and valor == 0) or (not literal.startswith('-') and valor == 1):
            print("devuelve TRUE el literal:", literal ,"del termino ",termino, " y el valor: ",valor ,"de valores: ",valores)
            return True  # La condición se cumple
        print("NO CUMPLE el literal:", literal ,"del termino ",termino, " y el valor: ",valor ,"de valores: ",valores)
    return False  # El termino no se cumple

# Función para generar todas las combinaciones posibles de valores FUERZABRUTA
def generar_combinaciones(variables):
    from itertools import product
    return list(product([0, 1], repeat=len(variables)))

# Función principal
def main():
    archivo = "condiciones.txt"
    terminos = leer_terminos(archivo) # se muestran los terminos con el 0 al final, pero este no se guarda en las variables

    #!!!!
    print("AÑADIDO: MOSTRAR CONDICIONES")
    print(terminos)

    # Obtener todas las variables únicas de las condiciones
    variables = set()
    for termino in terminos:
        for literal in termino:
            if literal == '0':
                break
            var = abs(int(literal))
            variables.add(var)
    variables = sorted(variables)  # Ordenar las variables

    #!!!!
    print("AÑADIDO: MOSTRAR VARIABLES")
    print(variables)

    # Generar todas las combinaciones posibles de valores
    combinaciones = generar_combinaciones(variables)

    #!!!!
    print("AÑADIDO: MOSTRAR COMBINACIONES")
    print(combinaciones)

    # Probar cada combinación
    soluciones = []
    for combinacion in combinaciones:
        valores = dict(zip(variables, combinacion))  # Asignar valores a las variables
        # Verificar si todas las condiciones se cumplen
        if all(evaluar_condicion(termino, valores) for termino in terminos):
            soluciones.append(valores)
            break # encuentra una solucion y sale
            
    # Mostrar las soluciones encontradas
    if soluciones:
        print("AÑADIDO: MOSTRAR Solucion")
        print(soluciones)
    else:
        print("No se encontraron soluciones que satisfagan todas las condiciones.")

if __name__ == "__main__":
    main()