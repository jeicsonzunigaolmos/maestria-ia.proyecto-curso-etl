import json
"""
    se definen todas las excepciones a tratar durante el proceso de 
    extraccion y transformacion de datos.
    debido a los errores presentados por el formato json mal formado entregado
    desde la api se desarrolla una funcion que valide el objeto, si el objeto se
    carga correctamente retorna el objecto de lo contrario le coloca valores nulos
"""
def excepciones_datos_faltantes(fila):
    objeto_json = json.loads(fila.strip())
    """
        valida si la fecha fecha_de_firma del contrato existe en el formato json
    """
    try:
        objeto_json["fecha_de_firma"]
    except Exception as e:
        objeto_json["fecha_de_firma"] = None
    """
        valida si la fecha fecha_de_inicio_del_contrato del contrato existe en el formato json
    """
    try:
        objeto_json["fecha_de_inicio_del_contrato"]
    except Exception as e:
        objeto_json["fecha_de_inicio_del_contrato"] = None
    """
        valida si la fecha fecha_de_fin_del_contrato del contrato existe en el formato json
    """
    try:
        objeto_json["fecha_de_fin_del_contrato"]
    except Exception as e:
        objeto_json["fecha_de_fin_del_contrato"] = None
    """
        valida si la fecha fecha_de_fin_del_contrato del contrato existe en el formato json
    """
    try:
        objeto_json["n_mero_de_cuenta"]
    except Exception as e:
        objeto_json["n_mero_de_cuenta"] = None
    """
        retorna el resultado
    """
    return objeto_json
def limpiar_archivo_excepciones():
    with open('data/excepciones.txt', 'w', encoding='utf-8') as archivo:
        return True
def excepciones_formato_mal_formado(fila):
    #fila = fila.replace("\\", "")
    #fila = fila.replace(", }", " }")
    #fila = fila.replace('} {', '}, {')
    #fila = fila.replace(',}', '}')
    #fila = fila.replace(",,", ",")
     #fila = fila.replace(", ,", ",")
    #fila = fila.replace(",", " ")
    try:
        json.loads(fila)
        return True
    except Exception as e:
        print(f"Ocurrió un error inesperado: {e}")
        print(f"formato json: {fila}")
        with open('data/excepciones.txt', 'a', encoding='utf-8') as archivo:
            archivo.write(f"{fila}\n")
        return False