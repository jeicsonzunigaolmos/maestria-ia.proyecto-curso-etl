from datetime import datetime
def extraer_dia_mes_anio(fecha_objeto):
    formato = "%d/%m/%Y"
    fecha_objeto = datetime.strptime(fecha_objeto, formato).date()
    dia = fecha_objeto.day
    mes = fecha_objeto.month
    anio = fecha_objeto.year
    return dia, mes, anio