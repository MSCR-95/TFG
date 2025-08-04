from sympy import symbols, And, Or, Not, satisfiable

# Función para leer el archivo y extraer las condiciones
def leer_condiciones(archivo):
    with open(archivo, 'r') as f:
        lineas = f.readlines()
    return [linea.strip() for linea in lineas]

# Función para convertir una línea en una cláusula lógica
def linea_a_clausula(linea, variables):
    literales = linea.split()[:-1]  # Ignoramos el 0 final
    clausula = []
    for lit in literales:
        valor = int(lit)
        if valor > 0:
            clausula.append(variables[valor - 1])  # x1, x2, etc.
        else:
            clausula.append(Not(variables[-valor - 1]))  # ¬x1, ¬x2, etc.
    return Or(*clausula)

# Leer las condiciones desde el archivo
archivo = 'condiciones.txt'
condiciones = leer_condiciones(archivo)

# Definir las variables (x1, x2, x3, ...)
num_variables = max(abs(int(lit)) for linea in condiciones for lit in linea.split()[:-1])
variables = symbols(f'x1:{num_variables + 1}')  # x1, x2, x3, ...

# Convertir las condiciones en una fórmula CNF
clausulas = [linea_a_clausula(linea, variables) for linea in condiciones]
formula = And(*clausulas)

# Verificar si la fórmula es satisfacible
satisfacible = satisfiable(formula)

if satisfacible:
    print("La fórmula es satisfacible.")
    print("Asignación que satisface la fórmula:", satisfacible)
else:
    print("La fórmula no es satisfacible.")