import dimod
from dimod.serialization.format import Formatter
# Leer el fichero de cláusulas en formato DIMACS CNF
def leer_clausulas(path: str) -> tuple[int, list[tuple[int, int]]]:
    clausulas = []
    n_variables = 0
    with open(path) as f:
        for linea in f:
            linea = linea.strip()
            if not linea or linea.startswith("c"):
                continue
            if linea.startswith("p cnf"):
                partes = linea.split()
                n_variables = int(partes[2])
                continue
            literales = [int(x) for x in linea.split() if x != "0"]
            if len(literales) == 2:
                clausulas.append((literales[0], literales[1]))
    return n_variables, clausulas

# Construir la matriz Q a partir de las cláusulas
#    Usando las penalizaciones:
#       Sin negaciones (xi ∨ xj):   1 - xi - xj + xi*xj
#       Una negación  (¬xi ∨ xj):   xj - xi*xj
#       Dos negaciones (¬xi ∨ ¬xj): xi*xj
def construir_qubo(n_variables: int, clausulas: list[tuple[int, int]]) -> tuple[dict, int]:
    # Q como diccionario {(i,j): coef} con variables indexadas desde 0
    Q = {}
    constante = 0

    def add(i, j, valor):
        if i == j:
            Q[(i, i)] = Q.get((i, i), 0) + valor
        else:
            Q[(i, j)] = Q.get((i, j), 0) + valor / 2
            Q[(j, i)] = Q.get((j, i), 0) + valor / 2

    for lit1, lit2 in clausulas:
        neg1 = lit1 < 0
        neg2 = lit2 < 0
        # Variables indexadas desde 0
        i = abs(lit1) - 1
        j = abs(lit2) - 1

        if not neg1 and not neg2:
            # xi ∨ xj  →  1 - xi - xj + xi*xj
            constante += 1
            add(i, i, -1)
            add(j, j, -1)
            add(i, j, +1)

        elif neg1 and not neg2:
            # ¬xi ∨ xj  →  xj - xi*xj
            add(i, i, +1)
            add(i, j, -1)

        elif not neg1 and neg2:
            # xi ∨ ¬xj  →  xi - xi*xj
            add(j, j, +1)
            add(i, j, -1)

        else:
            # ¬xi ∨ ¬xj  →  xi*xj
            add(i, j, +1)

    return Q, constante

# Resolver con el simulador de recocido simulado (neal)
def resolver(Q: dict, constante: int, n_reads: int = 20): # Devuelve la muestra, energía QUBO y constante
    # bqm = dimod.BinaryQuadraticModel.from_qubo(Q) # Convertir a formato BQM
    # sampler = neal.SimulatedAnnealingSampler() # Crear el sampler de recocido simulado
    # resultado = sampler.sample(bqm, num_reads=n_reads) # Ejecutar la muestra
    # mejor = resultado.first # Obtener la mejor muestra (la de menor energía)
    sampler = dimod.SimulatedAnnealingSampler().sample_qubo(Q, num_reads=n_reads)
    mejor = sampler.first
    # print(f"\nmuestra (usando sample_qubo): {mejor}")
    # Formatter().fprint(sampler)
    
    return (mejor.sample), mejor.energy, constante # Devolvemos la muestra, la energía QUBO y la constante para el conteo de cláusulas no satisfechas

# Función auxiliar para mostrar la matriz Q de forma legible
def mostrar_matriz_q(Q: dict, n_variables: int):
    # Construir matriz densa
    M = [[0.0] * n_variables for _ in range(n_variables)]
    for (i, j), v in Q.items():
        M[i][j] = v

    # Cabecera
    header = "        " + "".join(f"  x{j+1}   " for j in range(n_variables))
    print(header)

    # Filas
    for i in range(n_variables):
        fila = f"  x{i+1} [ "
        for j in range(n_variables):
            val = M[i][j]
            celda = f"{val:+.1f}"
            fila += f" {celda:>5} "
        fila += "]"
        print(fila)

# Main
if __name__ == "__main__":
    # PATH = "max2sat_43.txt"
    PATH = "PROBLEM_020_020_1.txt"

    n_vars, clausulas = leer_clausulas(PATH)
    print(f"Variables: {n_vars} | Cláusulas: {len(clausulas)}")

    Q, constante = construir_qubo(n_vars, clausulas)
    print(f"\nMatriz Q (términos no nulos):")
    # for (i, j), v in sorted(Q.items()):
    #     print(f"  Q[x{i+1}, x{j+1}] = {v}")
    mostrar_matriz_q(Q, n_vars)

    muestra, energia_qubo, constante = resolver(Q, constante)

    clausulas_no_satisfechas = energia_qubo + constante
    clausulas_satisfechas = len(clausulas) - clausulas_no_satisfechas # min y 

    print(f"\nSolución encontrada:")
    for var, val in sorted(muestra.items()):
        print(f"  x{var+1} = {val}")
    print(f"\nEnergía QUBO:            {energia_qubo}")
    print(f"Constante aditiva:       {constante}")
    print(f"Cláusulas no satisfechas: {int(clausulas_no_satisfechas)}")
    print(f"Cláusulas satisfechas:    {int(clausulas_satisfechas)} / {len(clausulas)}")

    # Verificación manual
    print(f"\nVerificación por cláusula:")
    for lit1, lit2 in clausulas:
        v1 = muestra[abs(lit1) - 1]
        v2 = muestra[abs(lit2) - 1]
        val1 = (1 - v1) if lit1 < 0 else v1
        val2 = (1 - v2) if lit2 < 0 else v2
        sat = "S" if (val1 or val2) else "N"
        print(f"  ({'+' if lit1>0 else '-'}x{abs(lit1)} ∨ {'+' if lit2>0 else '-'}x{abs(lit2)}) → {sat}")