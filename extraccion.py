#--------------------------------------------------------------------------
"""
    realizamos toda la importacion de librerias que vamso a usar
"""
from sodapy import Socrata
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine, text
from sqlalchemy import text
from config.config import Config
from config.conexion_postgres import ConexionPostgres
import config.consultas_sql as consultas_sql
import config.excepciones as excepciones
class Extraccion:
    def __init__(self):
        ...
    #--------------------------------------------------------------------------
    def extraer_api_staggin(self):
        """se realiza la solicitud a la api donde se define un limite en el archivo
        .yaml y dicha informacion se guarda en un archivo llamado informacion.txt
        para el proyecto se pidieron minimo 10mil registros y 10 columnas por cada
        registro, se pasan como parametros la url, usuario, contraseña y el token"""
        config = Config()
        configApi = config.retornar_config_por_nombre("configApi")
        print(f"---------->{configApi}")
        client = Socrata(configApi["Url"], configApi["MyAppToken"], configApi["username"], configApi["password"])
        resultado = client.get("jbjy-vk9h", limit = configApi["limit"])
        with open('data/informacion.txt', 'w', encoding='utf-8') as archivo:
            for dato in resultado:
                archivo.write(f"{dato}\n")
        print("El archivo ha sido guardado.")
    def crear_data_base(self):
        conexion_postgres = ConexionPostgres()
        conexion_postgres.crear_base_datos()
    def crear_tabla_apl1_contrato_digital(self):
        conexion_postgres = ConexionPostgres()
        conexion_postgres.ejecutar_sql(consultas_sql.crear_tabla_apl1_contrato_digital())
    def limpiar_tabla_apl1_contrato_digital(self):
        conexion_postgres = ConexionPostgres()
        conexion_postgres.ejecutar_sql(consultas_sql.limpiar_tabla_apl1_contrato_digital())
    def insertar_datos_staggin_a_database(self):
        """
            se define un cointador que se va imprimiendo por pantalla un consecutivo 
            para tener idea de cuantos registros se van guardando en base de datos, se lee
            el archivo informacion.txt que contiene toda la informacion proporcionada por
            la api de datos abiertos, se recorre linea por linea y se envia a la funcion de
            validar registro para evitar que el programa se detenga con los errores de
            formateo ocasionados por los json mal formados, para cada uno de los registros
            se lee dato por dato y se va formado el query para realizar el insert en la
            tabla con nombre apl1_contrato_digital, con esto se garantiza que la informacion
            se va almacenando en la tabla mencionada y se genera un mensaje en el momento en
            el numero de registro que presento el error
        """
        conexion_postgres = ConexionPostgres()
        contador, insertados, rechazados = 0, 0, 0
        excepciones.limpiar_archivo_excepciones()
        try:
            with open('data/informacion.txt', 'r', encoding='utf-8', errors='ignore') as archivo:
                for fila in archivo:
                    contador += 1
                    print(f"contador extraccion ----> {contador}")
                    fila = fila.replace("'", '"')
                    if excepciones.excepciones_formato_mal_formado(fila):
                        objeto_json = excepciones.excepciones_datos_faltantes(fila)
                        conexion_postgres.ejecutar_sql_dicionario(text(consultas_sql.insert_apl1_contrato_digital()), {
							'nombre_entidad': objeto_json["nombre_entidad"],
							'nit_entidad': objeto_json["nit_entidad"],
							'departamento': objeto_json["departamento"],
							'ciudad': objeto_json["ciudad"],
							'localizaci_n': objeto_json["localizaci_n"],
							'orden': objeto_json["orden"],
							'sector': objeto_json["sector"],
							'rama': objeto_json["rama"],
							'entidad_centralizada': objeto_json["entidad_centralizada"],
							'proceso_de_compra': objeto_json["proceso_de_compra"],
							'id_contrato': objeto_json["id_contrato"],
							'referencia_del_contrato': objeto_json["referencia_del_contrato"],
							'estado_contrato': objeto_json.get('estado_contrato', 'PENDIENTE'), #datos["estado_contrato"],
							'codigo_de_categoria_principal': objeto_json["codigo_de_categoria_principal"],
							'descripcion_del_proceso': objeto_json["descripcion_del_proceso"],
							'tipo_de_contrato': objeto_json["tipo_de_contrato"],
							'modalidad_de_contratacion': objeto_json["modalidad_de_contratacion"],
							'justificacion_modalidad_de': objeto_json["justificacion_modalidad_de"],
							'fecha_de_firma': objeto_json["fecha_de_firma"],
							'fecha_de_inicio_del_contrato': objeto_json["fecha_de_inicio_del_contrato"],
							'fecha_de_fin_del_contrato': objeto_json["fecha_de_fin_del_contrato"],
							'condiciones_de_entrega': objeto_json["condiciones_de_entrega"],
							'tipodocproveedor': objeto_json["tipodocproveedor"],
							'documento_proveedor': objeto_json["documento_proveedor"],
							'proveedor_adjudicado': 'PENDIENTE', # datos["proveedor_adjudicado"],
							'es_grupo': objeto_json["es_grupo"],
							'es_pyme': objeto_json["es_pyme"],
							'habilita_pago_adelantado': objeto_json["habilita_pago_adelantado"],
							'liquidaci_n': objeto_json["liquidaci_n"],
							'obligaci_n_ambiental': objeto_json["obligaci_n_ambiental"],
							'obligaciones_postconsumo': objeto_json["obligaciones_postconsumo"],
							'reversion': objeto_json["reversion"],
							'origen_de_los_recursos': objeto_json["origen_de_los_recursos"],
							'destino_gasto': objeto_json["destino_gasto"],
							'valor_del_contrato': objeto_json["valor_del_contrato"],
							'valor_de_pago_adelantado': objeto_json["valor_de_pago_adelantado"],
							'valor_facturado': objeto_json["valor_facturado"],
							'valor_pendiente_de_pago': objeto_json["valor_pendiente_de_pago"],
							'valor_pagado': objeto_json["valor_pagado"],
							'valor_amortizado': objeto_json["valor_amortizado"],
							'valor_pendiente_de': objeto_json["valor_pendiente_de"],
							'valor_pendiente_de_ejecucion': objeto_json["valor_pendiente_de_ejecucion"],
							'estado_bpin': objeto_json["estado_bpin"],
							'c_digo_bpin': objeto_json["c_digo_bpin"],
							'anno_bpin': objeto_json["anno_bpin"],
							'saldo_cdp': objeto_json["saldo_cdp"],
							'saldo_vigencia': objeto_json["saldo_vigencia"],
							'espostconflicto': objeto_json["espostconflicto"],
							'dias_adicionados': objeto_json["dias_adicionados"],
							'puntos_del_acuerdo': objeto_json["puntos_del_acuerdo"],
							'pilares_del_acuerdo': objeto_json["pilares_del_acuerdo"],
							'urlproceso': 'PENDIENTE', #datos["urlproceso"],
							'nombre_representante_legal': objeto_json["nombre_representante_legal"],
							'nacionalidad_representante_legal': objeto_json["nacionalidad_representante_legal"],
							'domicilio_representante_legal': objeto_json["domicilio_representante_legal"],
							'tipo_de_identificaci_n_representante_legal': objeto_json["tipo_de_identificaci_n_representante_legal"],
							'identificaci_n_representante_legal': objeto_json["identificaci_n_representante_legal"],
							'g_nero_representante_legal': objeto_json["g_nero_representante_legal"],
							'presupuesto_general_de_la_nacion_pgn': objeto_json["presupuesto_general_de_la_nacion_pgn"],
							'sistema_general_de_participaciones': objeto_json["sistema_general_de_participaciones"],
							'sistema_general_de_regal_as': objeto_json["sistema_general_de_regal_as"],
							'recursos_propios_alcald_as_gobernaciones_y_resguardos_ind_genas_': objeto_json["recursos_propios_alcald_as_gobernaciones_y_resguardos_ind_genas_"],
							'recursos_de_credito': objeto_json["recursos_de_credito"],
							'recursos_propios': objeto_json["recursos_propios"],
							'codigo_entidad': objeto_json["codigo_entidad"],
							'codigo_proveedor': objeto_json["codigo_proveedor"],
							'objeto_del_contrato': objeto_json["objeto_del_contrato"],
							'duraci_n_del_contrato': objeto_json["duraci_n_del_contrato"],
							'nombre_del_banco': objeto_json["nombre_del_banco"],
							'tipo_de_cuenta': objeto_json["tipo_de_cuenta"],
							'n_mero_de_cuenta': objeto_json["n_mero_de_cuenta"],
							'el_contrato_puede_ser_prorrogado': objeto_json["el_contrato_puede_ser_prorrogado"],
							'nombre_ordenador_del_gasto': objeto_json["nombre_ordenador_del_gasto"],
							'tipo_de_documento_ordenador_del_gasto': objeto_json["tipo_de_documento_ordenador_del_gasto"],
							'n_mero_de_documento_ordenador_del_gasto': objeto_json["n_mero_de_documento_ordenador_del_gasto"],
							'nombre_supervisor': objeto_json["nombre_supervisor"],
							'tipo_de_documento_supervisor': objeto_json["tipo_de_documento_supervisor"],
							'n_mero_de_documento_supervisor': objeto_json["n_mero_de_documento_supervisor"],
							'nombre_ordenador_de_pago': objeto_json["nombre_ordenador_de_pago"],
							'tipo_de_documento_ordenador_de_pago': objeto_json["tipo_de_documento_ordenador_de_pago"],
							'n_mero_de_documento_ordenador_de_pago': objeto_json["n_mero_de_documento_ordenador_de_pago"]
						})
                        insertados += 1
                    else:
                        rechazados += 1
            print("Datos insertados exitosamente en 'apl1_contrato_digital'.")
            print(f"Resumen: contador = {contador}, insertados = {insertados}, rechazados = {rechazados}")
        except FileNotFoundError:
            print("El archivo 'informacion.txt' no se encuentra.")
        except Exception as e:
            print(f"Ocurrió un error inesperado: {e}")