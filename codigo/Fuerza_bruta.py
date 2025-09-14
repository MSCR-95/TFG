from itertools import product 
class Fuerza_bruta:
 
    # Función para evaluar un termino FUERZABRUTA
    def evaluar_condicion(self, termino, valores):
        for literal in termino:
            if literal == '0':  # Fin del termino
                break
            var = abs(int(literal))  # Valor absoluto para obtener la variable
            valor = valores.get(var, 0)  # Si la variable no está en el diccionario, asumimos 0
            if (literal.startswith('-') and valor == 0) or (not literal.startswith('-') and valor == 1):
                # print("devuelve TRUE el literal:", literal ,"del termino ",termino, " y el valor: ",valor ,"de valores: ",valores)
                return True  # La condición se cumple
            # print("NO CUMPLE el literal:", literal ,"del termino ",termino, " y el valor: ",valor ,"de valores: ",valores)
        return False  # El termino no se cumple

    def resolver(self, terminos):
        variables = [1, 2, 3, 4, 5]
        # Generar todas las combinaciones binarias posibles para 5 variables
        combinaciones = list(product([0, 1], repeat=len(variables)))
        solucion = []
        for combinacion in combinaciones:
            valores = dict(zip(variables, combinacion))  # Asignar valores a las variables
            # 
            # print(valores)
            if all(self.evaluar_condicion(termino, valores) for termino in terminos):
                solucion.append(valores)
                break  # Solo queremos la primera solución valida
        return solucion

            