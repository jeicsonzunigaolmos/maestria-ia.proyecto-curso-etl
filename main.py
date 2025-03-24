#------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------
from extraccion import Extraccion
from transformacion import Transformacion
from carga import Carga
#------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------
"""
    se define el metodo principal o metodo donde todo el proceso se ejecuta secuencialmente
    como un pipeline.
"""
#------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------
""""
    empieza el proceso de extraccion de datos
"""
extraer = Extraccion()
extraer.extraer_api_staggin()
extraer.crear_data_base()
extraer.crear_tabla_apl1_contrato_digital()
extraer.limpiar_tabla_apl1_contrato_digital()
extraer.insertar_datos_staggin_a_database()
#------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------
""""
    empieza el proceso de transformacion de datos donde la salida de un proceso es la 
    entrada del otro
"""
transformar = Transformacion()
lista_dicionario = transformar.consultar_informacion_apl1_contrato_digital()
filtro_columnas = ["departamento", "ciudad", "orden", "sector", "rama", "estado_contrato", "tipo_de_contrato", "fecha_de_firma", "fecha_de_inicio_del_contrato", "fecha_de_fin_del_contrato", "destino_gasto", "valor_del_contrato", "valor_pendiente_de_pago", "tipodocproveedor", "documento_proveedor", "proveedor_adjudicado", "descripcion_del_proceso"]
lista_dicionario = transformar.filtrar_datos(lista_dicionario, filtro_columnas)
lista_dicionario = transformar.transformar_fecha_de_firma(lista_dicionario)
lista_dicionario = transformar.transformar_fecha_de_inicio_del_contrato(lista_dicionario)
lista_dicionario = transformar.transformar_fecha_de_fin_del_contrato(lista_dicionario)
lista_dicionario = transformar.transformar_clasificacion_valor_contrato(lista_dicionario)
lista_dicionario = transformar.transformar_valor_pendiente_por_ejecutar(lista_dicionario)
lista_dicionario = transformar.transformar_porcentaje_valor_ejecutado(lista_dicionario)
lista_dicionario = transformar.transformar_contratos_inferiores_2024_pendientes_pago(lista_dicionario)
for dicionario in lista_dicionario:
    print(dicionario)
transformar.almacenar_datos_formato_json(lista_dicionario)
#------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------
""""
    empieza el proceso de carga de datos
"""
carga = Carga()
carga.crear_tabla_apl1_consolidado()
carga.limpiar_tabla_apl1_consolidado()
carga.carga_en_database()
#------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------