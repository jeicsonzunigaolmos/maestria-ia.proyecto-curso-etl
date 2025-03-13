#--------------------------------------------------------------------------
"""
    realizamos toda la importacion de librerias que vamso a usar
"""
from sodapy import Socrata
import json
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine, text
from sqlalchemy import text
import config.consultas_sql as consultas_sql
import config.excepciones as excepciones
from config.config import Config
from datetime import datetime
import config.transformaciones as transformaciones
import config.validaciones as validaciones
class Transformacion:
    def __init__(self):
        ...
    """
        se consulta toda la informacion de la base de datos
    """
    def consultar_informacion_database(self):
        config = Config()
        configDataBase = config.retornar_config_por_nombre("configDataBase")
        db_name = "etl_proyecto_curso"
        engine = create_engine(f"postgresql://{configDataBase["usuario"]}:{configDataBase["contrasena"]}@{configDataBase["host"]}:{configDataBase["puerto"]}/{db_name}")
        resultado = None
        lista_dicionario = []
        with engine.connect() as conn:
            with conn.begin():
                resultado = conn.execute(text(consultas_sql.consultar_datos_database()))
        for registro in resultado:
            lista_dicionario.append(dict(registro._mapping))
        return lista_dicionario
    """
        empieza el proceso de filtro de la informacion de interes que se requiere
        para trnaformarla y cargarla en las datamart
    """
    def filtrar_datos(self, lista_dicionario, filtro_columnas):
        nueva_lista = []
        for registro in lista_dicionario:
            dicionario = {}
            for columna in filtro_columnas:
                dicionario[columna] = registro[columna]
            nueva_lista.append(dicionario)
        return nueva_lista
    def transformar_fecha_de_firma(self, lista_dicionario):
        for dicionario in lista_dicionario:
            if(validaciones.validar_fecha(dicionario["fecha_de_firma"])):
                dia, mes, anio = transformaciones.extraer_dia_mes_anio(dicionario["fecha_de_firma"])
                dicionario["fecha_de_firma_dia"] = dia
                dicionario["fecha_de_firma_mes"] = mes
                dicionario["fecha_de_firma_anio"] = anio
            else:
                dicionario["fecha_de_firma_dia"] = None
                dicionario["fecha_de_firma_mes"] = None
                dicionario["fecha_de_firma_anio"] = None
        return lista_dicionario
    def transformar_fecha_de_inicio_del_contrato(self, lista_dicionario):
        for dicionario in lista_dicionario:
            if(validaciones.validar_fecha(dicionario["fecha_de_inicio_del_contrato"])):
                dia, mes, anio = transformaciones.extraer_dia_mes_anio(dicionario["fecha_de_inicio_del_contrato"])
                dicionario["fecha_de_inicio_del_contrato_dia"] = dia
                dicionario["fecha_de_inicio_del_contrato_mes"] = mes
                dicionario["fecha_de_inicio_del_contrato_anio"] = anio
            else:
                dicionario["fecha_de_inicio_del_contrato_dia"] = None
                dicionario["fecha_de_inicio_del_contrato_mes"] = None
                dicionario["fecha_de_inicio_del_contrato_anio"] = None
        return lista_dicionario
    def transformar_fecha_de_fin_del_contrato(self, lista_dicionario):
        for dicionario in lista_dicionario:
            if(validaciones.validar_fecha(dicionario["fecha_de_fin_del_contrato"])):
                dia, mes, anio = transformaciones.extraer_dia_mes_anio(dicionario["fecha_de_fin_del_contrato"])
                dicionario["fecha_de_fin_del_contrato_dia"] = dia
                dicionario["fecha_de_fin_del_contrato_mes"] = mes
                dicionario["fecha_de_fin_del_contrato_anio"] = anio
            else:
                dicionario["fecha_de_fin_del_contrato_dia"] = None
                dicionario["fecha_de_fin_del_contrato_mes"] = None
                dicionario["fecha_de_fin_del_contrato_anio"] = None
        return lista_dicionario
    def transformar_clasificacion_valor_contrato(self, lista_dicionario):
        for dicionario in lista_dicionario:
            valor_del_contrato = int(dicionario["valor_del_contrato"])
            if(valor_del_contrato<20000000):
                dicionario["clasificacion_valor_del_contrato"] = "Menor de 20 millones"
            elif(valor_del_contrato>=20000000 and valor_del_contrato<50000000):
                dicionario["clasificacion_valor_del_contrato"] = "Entre 20 millones y 50 millones"
            elif(valor_del_contrato>50000000 and valor_del_contrato<=100000000):
                dicionario["clasificacion_valor_del_contrato"] = "Entre 50 millones y 100 millones"
            elif(valor_del_contrato>100000000 and valor_del_contrato<=500000000):
                dicionario["clasificacion_valor_del_contrato"] = "Entre 100 millones y 500 millones"
            elif(valor_del_contrato>500000000 and valor_del_contrato<=1000000000):
                dicionario["clasificacion_valor_del_contrato"] = "Entre 500 millones y 1000 millones"
            elif(valor_del_contrato>1000000000):
                dicionario["clasificacion_valor_del_contrato"] = "Mayor de 1000 millones"
            else:
                dicionario["clasificacion_valor_del_contrato"] = "Sin clasificar"
        return lista_dicionario
    def transformar_valor_pendiente_por_ejecutar(self, lista_dicionario):
        for dicionario in lista_dicionario:
            valor_del_contrato = int(dicionario["valor_del_contrato"])
            valor_pendiente_de_pago = int(dicionario["valor_pendiente_de_pago"])
            dicionario["valor_pendiente_por_ejecutar"] = valor_del_contrato - valor_pendiente_de_pago
        return lista_dicionario
    def transformar_porcentaje_valor_ejecutado(self, lista_dicionario):
        for dicionario in lista_dicionario:
            valor_del_contrato = int(dicionario["valor_del_contrato"])
            valor_pendiente_de_pago = int(dicionario["valor_pendiente_de_pago"])
            if(valor_del_contrato > 0):
                porcentaje_valor_contrato_ejecutado = (valor_pendiente_de_pago * 100)/valor_del_contrato
            else:
                porcentaje_valor_contrato_ejecutado = 0
            dicionario["porcentaje_valor_contrato_ejecutado"] = porcentaje_valor_contrato_ejecutado
        return lista_dicionario
    def transformar_contratos_inferiores_2024_pendientes_pago(self, lista_dicionario):
        for dicionario in lista_dicionario:
            #print(f"-------->dicionario:{dicionario}")
            if (validaciones.validar_numero(dicionario["fecha_de_fin_del_contrato_anio"])):
                if (dicionario["valor_pendiente_de_pago"] > 0 and dicionario["fecha_de_fin_del_contrato_anio"] <= 2024):
                    dicionario["contratos_inferiores_2024_pendientes_pago"] = True
                else:
                    dicionario["contratos_inferiores_2024_pendientes_pago"] = False
            else:
                dicionario["contratos_inferiores_2024_pendientes_pago"] = None
        return lista_dicionario
    def almacenar_datos(self, lista_dicionario):
        with open('data/transformaciones.json', 'w', encoding='utf-8') as archivo:
            json.dump(lista_dicionario, archivo, ensure_ascii=False, indent=4)