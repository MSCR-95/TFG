class Clausuras:

    def leer_condiciones(self, archivo):
        with open(archivo, 'r') as f:
            lineas = f.readlines()
        return [linea.strip().split() for linea in lineas]
    
    def cargar_terminos(self)
        

        
