from sympy import symbols, And, Or, Not, satisfiable

# Definimos las variables
x1, x2, x3 = symbols('x1 x2 x3')

# Definimos la fórmula en CNF
formula = And(          
    Or(x1, x2, Not(x3)),          # Cláusula 1: x1 ∨ x2
    Or(x1, x3),     # Cláusula 2: ¬x1 ∨ x2
    Or(x1, x2),     # Cláusula 3: x1 ∨ ¬x2
)

# Verificamos si la fórmula es satisfacible
satisfacible = satisfiable(formula)

if satisfacible:
    print("La fórmula es satisfacible.")
    print("Asignación que satisface la fórmula:", satisfacible)
else:
    print("La fórmula no es satisfacible.")