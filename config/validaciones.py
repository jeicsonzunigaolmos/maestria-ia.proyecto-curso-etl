
from datetime import datetime
def validar_fecha(fecha_objeto):
    if fecha_objeto is None:
        return False
    else:
        return True
def validar_numero(valor):
    if valor is not None:
        try:
            int(valor)
            return True
        except ValueError:
            return False
    else:
        return False
    