from extraccion import Extraccion
from transformacion import Transformacion
"""
    se define el metodo principal o metodo donde todo el proceso se ejecuta secuencialmente
    como un pipeline.
"""
#empieza el proceso de extraccion de datos
extraer = Extraccion()
extraer.extraer_api_staggin()
extraer.crear_data_base()
extraer.crear_tabla_apl1_contrato_digital()
extraer.insertar_datos_staggin_a_database()
#empieza el proceso de transformacion de datos
transformar = Transformacion()
lista_dicionario = transformar.consultar_informacion_database()
#se definen las columnas a filtar o columnas de interes del formato json original
filtro_columnas = ["departamento", "ciudad", "orden", "sector", "rama", "estado_contrato", "tipo_de_contrato", "fecha_de_firma", "fecha_de_inicio_del_contrato", "fecha_de_fin_del_contrato", "destino_gasto", "valor_del_contrato", "valor_pendiente_de_pago", "tipodocproveedor", "documento_proveedor", "proveedor_adjudicado", "descripcion_del_proceso"]
""""
    empieza el proceso de las transformaciones donde la salida de un proceso es la entrada del otro
"""
lista_dicionario = transformar.filtrar_datos(lista_dicionario, filtro_columnas)
lista_dicionario = transformar.transformar_fecha_de_firma(lista_dicionario)
lista_dicionario = transformar.transformar_fecha_de_inicio_del_contrato(lista_dicionario)
lista_dicionario = transformar.transformar_fecha_de_fin_del_contrato(lista_dicionario)
lista_dicionario = transformar.transformar_clasificacion_valor_contrato(lista_dicionario)
lista_dicionario = transformar.transformar_valor_pendiente_por_ejecutar(lista_dicionario)
lista_dicionario = transformar.transformar_porcentaje_valor_ejecutado(lista_dicionario)
lista_dicionario = transformar.transformar_contratos_inferiores_2024_pendientes_pago(lista_dicionario)
#imprime los datos y al final los almacena en un archivo de texto para hacer el proceso de carga de datamart
for dicionario in lista_dicionario:
    print(dicionario)
transformar.almacenar_datos(lista_dicionario)