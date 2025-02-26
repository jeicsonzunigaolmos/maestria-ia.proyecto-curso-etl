#--------------------------------------------------------------------------
"""realizamos toda la importacion de librerias que vamso a usar"""
import pandas as pd
from sodapy import Socrata
import yaml
import psycopg2 
from psycopg2 import sql
from sqlalchemy import create_engine, text
import json
from sqlalchemy import text
#--------------------------------------------------------------------------
"""se define una funncion para cargar los archivos .yaml que van a contener
informacion del archivo de configuracion de base de datos y apikey de la 
api de datos abiertos
"""
def load_config(file_path="config.yaml"):
    with open(file_path, "r") as file:
        return yaml.safe_load(file)
#--------------------------------------------------------------------------
"""se llama la funcion anterior y se asignan las variables que se usaran para
conexion con la api de datos abiertos"""
config = load_config()
configApi = config["configApi"]
Url = configApi["Url"]
MyAppToken = configApi["MyAppToken"]
username = configApi["username"]
password = configApi["password"]
limite = configApi["limit"]
print(Url)
#--------------------------------------------------------------------------
"""se realiza la solicitud a la api donde se define un limite en el archivo
.yaml y dicha informacion se guarda en un archivo llamado informacion.txt
para el proyecto se pidieron minimo 10mil registros y 10 columnas por cada
registro, se pasan como parametros la url, usuario, contraseña y el token"""
client = Socrata(Url, MyAppToken, username, password)
resultado = client.get("jbjy-vk9h", limit=limite)
for dato in resultado:
    with open('informacion.txt', 'a', encoding='utf-8') as archivo:
        archivo.write(f"{dato}\n")
print("El archivo ha sido guardado.")
#--------------------------------------------------------------------------
"""se asignan las variables de conexion al objeto psycopg2 por el metodo 
connect y se realiza la conexion a postgres"""
configDataBase = config["database"]
conn = psycopg2.connect(
    host=configDataBase["host"],
    user=configDataBase["usuario"],
    password=configDataBase["contrasena"],
    port=configDataBase["puerto"]
)
#--------------------------------------------------------------------------
"""se define un autocommit que es el encargado de guardar cambios sobre las
transaciones de base de datos, se crea la base de datos con nombre
etl_proyecto_curso, se define un cursor y se ejecuta el query que crea la
base de datos en postgres, si la base de datos esta creada simplemente
ignora el proceso"""
conn.autocommit = True
db_name = "etl_proyecto_curso"
try:
    with conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
        print(f"Base de datos '{db_name}' creada exitosamente.")
except psycopg2.errors.DuplicateDatabase:
    print(f"La base de datos '{db_name}' ya existe.")
finally:
    conn.close()
#--------------------------------------------------------------------------
"""se crea la cadena de conexion con el usuario, contraseña, host, puerto
y nombre de la base de datos, se define el query del proceso ddl para la creacion
de la tabla apl1_contrato_digital que sera la encargada de guardar toda la informacion
de los contratos del secop II"""
engine = create_engine(f"postgresql://{configDataBase["usuario"]}:{configDataBase["contrasena"]}@{configDataBase["host"]}:{configDataBase["puerto"]}/{db_name}")
with engine.connect() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS apl1_contrato_digital (
            id SERIAL PRIMARY KEY,
            nombre_entidad VARCHAR(4000),
            nit_entidad VARCHAR(4000),
            departamento VARCHAR(4000),
            ciudad VARCHAR(4000),
            localizaci_n VARCHAR(4000),
            orden VARCHAR(4000),
            sector VARCHAR(4000),
            rama VARCHAR(4000),
            entidad_centralizada VARCHAR(4000),
            proceso_de_compra VARCHAR(4000),
            id_contrato VARCHAR(4000),
            referencia_del_contrato VARCHAR(4000),
            estado_contrato VARCHAR(4000),
            codigo_de_categoria_principal VARCHAR(4000),
            descripcion_del_proceso VARCHAR(4000),
            tipo_de_contrato VARCHAR(4000),
            modalidad_de_contratacion VARCHAR(4000),
            justificacion_modalidad_de VARCHAR(4000),
            fecha_de_firma TIMESTAMP,
            fecha_de_inicio_del_contrato TIMESTAMP,
            fecha_de_fin_del_contrato TIMESTAMP,
            condiciones_de_entrega VARCHAR(4000),
            tipodocproveedor VARCHAR(4000),
            documento_proveedor VARCHAR(4000),
            proveedor_adjudicado VARCHAR(4000),
            es_grupo VARCHAR(4000),
            es_pyme VARCHAR(4000),
            habilita_pago_adelantado VARCHAR(4000),
            liquidaci_n VARCHAR(4000),
            obligaci_n_ambiental VARCHAR(4000),
            obligaciones_postconsumo VARCHAR(4000),
            reversion VARCHAR(4000),
            origen_de_los_recursos VARCHAR(4000),
            destino_gasto VARCHAR(4000),
            valor_del_contrato VARCHAR(4000),
            valor_de_pago_adelantado VARCHAR(4000),
            valor_facturado VARCHAR(4000),
            valor_pendiente_de_pago VARCHAR(4000),
            valor_pagado VARCHAR(4000),
            valor_amortizado VARCHAR(4000),
            valor_pendiente_de VARCHAR(4000),
            valor_pendiente_de_ejecucion VARCHAR(4000),
            estado_bpin VARCHAR(4000),
            c_digo_bpin VARCHAR(4000),
            anno_bpin VARCHAR(4000),
            saldo_cdp VARCHAR(4000),
            saldo_vigencia VARCHAR(4000),
            espostconflicto VARCHAR(4000),
            dias_adicionados VARCHAR(4000),
            puntos_del_acuerdo VARCHAR(4000),
            pilares_del_acuerdo VARCHAR(4000),
            urlproceso VARCHAR(4000),
            nombre_representante_legal VARCHAR(4000),
            nacionalidad_representante_legal VARCHAR(4000),
            domicilio_representante_legal VARCHAR(4000),
            tipo_de_identificaci_n_representante_legal VARCHAR(4000),
            identificaci_n_representante_legal VARCHAR(4000),
            g_nero_representante_legal VARCHAR(4000),
            presupuesto_general_de_la_nacion_pgn VARCHAR(4000),
            sistema_general_de_participaciones VARCHAR(4000),
            sistema_general_de_regal_as VARCHAR(4000),
            recursos_propios_alcald_as_gobernaciones_y_resguardos_ind_genas_ VARCHAR(4000),
            recursos_de_credito VARCHAR(4000),
            recursos_propios VARCHAR(4000),
            codigo_entidad VARCHAR(4000),
            codigo_proveedor VARCHAR(4000),
            objeto_del_contrato VARCHAR(4000),
            duraci_n_del_contrato VARCHAR(4000),
            nombre_del_banco VARCHAR(4000),
            tipo_de_cuenta VARCHAR(4000),
            n_mero_de_cuenta VARCHAR(4000),
            el_contrato_puede_ser_prorrogado VARCHAR(4000),
            nombre_ordenador_del_gasto VARCHAR(4000),
            tipo_de_documento_ordenador_del_gasto VARCHAR(4000),
            n_mero_de_documento_ordenador_del_gasto VARCHAR(4000),
            nombre_supervisor VARCHAR(4000),
            tipo_de_documento_supervisor VARCHAR(4000),
            n_mero_de_documento_supervisor VARCHAR(4000),
            nombre_ordenador_de_pago VARCHAR(4000),
            tipo_de_documento_ordenador_de_pago VARCHAR(4000),
            n_mero_de_documento_ordenador_de_pago VARCHAR(4000)
        );
    """))
    conn.commit()
    print("Tabla 'apl1_contrato_digital' creada exitosamente en PostgreSQL.")
#--------------------------------------------------------------------------
"""debido a los errores presentados por el formato json mal formado entregado
desde la api se desarrolla una funcion que valide el objeto, si el objeto se
carga correctamente retorna un true de lo contrario un false"""
def validar_registro(fila):
    try:
        datos = json.loads(fila.strip())
        datos["fecha_de_firma"]
        datos["fecha_de_inicio_del_contrato"]
        datos["fecha_de_fin_del_contrato"]
        return True
    except Exception as e:
        print(f"Ocurrió un error inesperado: {e}")
        return False
#--------------------------------------------------------------------------
"""se define un cointador que se va imprimiendo por pantalla un consecutivo 
para tener idea de cuantos registros se van guardando en base de datos, se lee
el archivo informacion.txt que contiene toda la informacion proporcionada por
la api de datos abiertos, se recorre linea por linea y se envia a la funcion de
validar registro para evitar que el programa se detenga con los errores de
formateo ocasionados por los json mal formados, para cada uno de los registros
se lee dato por dato y se va formado el query para realizar el insert en la
tabla con nombre apl1_contrato_digital, con esto se garantiza que la informacion
se va almacenando en la tabla mencionada y se genera un mensaje en el momento en
el numero de registro que presento el error"""
contador = 0
try:
    with open('informacion.txt', 'r', encoding='utf-8', errors='ignore') as archivo:
        for fila in archivo:
            contador+=1
            print(contador)
            fila = fila.replace("'", '"')
            #fila = fila.replace("\\", "")
            #fila = fila.replace(", }", " }")
            #fila = fila.replace('} {', '}, {')
            #fila = fila.replace(',}', '}')
            #fila = fila.replace(",,", ",")
            #fila = fila.replace(", ,", ",")
            #fila = fila.replace(",", " ")
            if (validar_registro(fila)):
                datos = json.loads(fila.strip())
                with engine.connect() as conn:
                    with conn.begin():
                        sql = """INSERT INTO apl1_contrato_digital (nombre_entidad
                            ,   nit_entidad
                            ,   departamento
                            ,   ciudad
                            ,   localizaci_n
                            ,   orden
                            ,   sector
                            ,   rama
                            ,   entidad_centralizada
                            ,   proceso_de_compra
                            ,   id_contrato
                            ,   referencia_del_contrato
                            ,   estado_contrato
                            ,   codigo_de_categoria_principal
                            ,   descripcion_del_proceso
                            ,   tipo_de_contrato
                            ,   modalidad_de_contratacion
                            ,   justificacion_modalidad_de
                            ,   fecha_de_firma
                            ,   fecha_de_inicio_del_contrato
                            ,   fecha_de_fin_del_contrato
                            ,   condiciones_de_entrega
                            ,   tipodocproveedor
                            ,   documento_proveedor
                            ,   proveedor_adjudicado
                            ,   es_grupo
                            ,   es_pyme
                            ,   habilita_pago_adelantado
                            ,   liquidaci_n
                            ,   obligaci_n_ambiental
                            ,   obligaciones_postconsumo
                            ,   reversion
                            ,   origen_de_los_recursos
                            ,   destino_gasto
                            ,   valor_del_contrato
                            ,   valor_de_pago_adelantado
                            ,   valor_facturado
                            ,   valor_pendiente_de_pago
                            ,   valor_pagado
                            ,   valor_amortizado
                            ,   valor_pendiente_de
                            ,   valor_pendiente_de_ejecucion
                            ,   estado_bpin
                            ,   c_digo_bpin
                            ,   anno_bpin
                            ,   saldo_cdp
                            ,   saldo_vigencia
                            ,   espostconflicto
                            ,   dias_adicionados
                            ,   puntos_del_acuerdo
                            ,   pilares_del_acuerdo
                            ,   urlproceso
                            ,   nombre_representante_legal
                            ,   nacionalidad_representante_legal
                            ,   domicilio_representante_legal
                            ,   tipo_de_identificaci_n_representante_legal
                            ,   identificaci_n_representante_legal
                            ,   g_nero_representante_legal
                            ,   presupuesto_general_de_la_nacion_pgn
                            ,   sistema_general_de_participaciones
                            ,   sistema_general_de_regal_as
                            ,   recursos_propios_alcald_as_gobernaciones_y_resguardos_ind_genas_
                            ,   recursos_de_credito
                            ,   recursos_propios
                            ,   codigo_entidad
                            ,   codigo_proveedor
                            ,   objeto_del_contrato
                            ,   duraci_n_del_contrato
                            ,   nombre_del_banco
                            ,   tipo_de_cuenta
                            ,   n_mero_de_cuenta
                            ,   el_contrato_puede_ser_prorrogado
                            ,   nombre_ordenador_del_gasto
                            ,   tipo_de_documento_ordenador_del_gasto
                            ,   n_mero_de_documento_ordenador_del_gasto
                            ,   nombre_supervisor
                            ,   tipo_de_documento_supervisor
                            ,   n_mero_de_documento_supervisor
                            ,   nombre_ordenador_de_pago
                            ,   tipo_de_documento_ordenador_de_pago
                            ,   n_mero_de_documento_ordenador_de_pago
                        ) VALUES (:nombre_entidad
                            ,   :nit_entidad
                            ,   :departamento
                            ,   :ciudad
                            ,   :localizaci_n
                            ,   :orden
                            ,   :sector
                            ,   :rama
                            ,   :entidad_centralizada
                            ,   :proceso_de_compra
                            ,   :id_contrato
                            ,   :referencia_del_contrato
                            ,   :estado_contrato
                            ,   :codigo_de_categoria_principal
                            ,   :descripcion_del_proceso
                            ,   :tipo_de_contrato
                            ,   :modalidad_de_contratacion
                            ,   :justificacion_modalidad_de
                            ,   :fecha_de_firma
                            ,   :fecha_de_inicio_del_contrato
                            ,   :fecha_de_fin_del_contrato
                            ,   :condiciones_de_entrega
                            ,   :tipodocproveedor
                            ,   :documento_proveedor
                            ,   :proveedor_adjudicado
                            ,   :es_grupo
                            ,   :es_pyme
                            ,   :habilita_pago_adelantado
                            ,   :liquidaci_n
                            ,   :obligaci_n_ambiental
                            ,   :obligaciones_postconsumo
                            ,   :reversion
                            ,   :origen_de_los_recursos
                            ,   :destino_gasto
                            ,   :valor_del_contrato
                            ,   :valor_de_pago_adelantado
                            ,   :valor_facturado
                            ,   :valor_pendiente_de_pago
                            ,   :valor_pagado
                            ,   :valor_amortizado
                            ,   :valor_pendiente_de
                            ,   :valor_pendiente_de_ejecucion
                            ,   :estado_bpin
                            ,   :c_digo_bpin
                            ,   :anno_bpin
                            ,   :saldo_cdp
                            ,   :saldo_vigencia
                            ,   :espostconflicto
                            ,   :dias_adicionados
                            ,   :puntos_del_acuerdo
                            ,   :pilares_del_acuerdo
                            ,   :urlproceso
                            ,   :nombre_representante_legal
                            ,   :nacionalidad_representante_legal
                            ,   :domicilio_representante_legal
                            ,   :tipo_de_identificaci_n_representante_legal
                            ,   :identificaci_n_representante_legal
                            ,   :g_nero_representante_legal
                            ,   :presupuesto_general_de_la_nacion_pgn
                            ,   :sistema_general_de_participaciones
                            ,   :sistema_general_de_regal_as
                            ,   :recursos_propios_alcald_as_gobernaciones_y_resguardos_ind_genas_
                            ,   :recursos_de_credito
                            ,   :recursos_propios
                            ,   :codigo_entidad
                            ,   :codigo_proveedor
                            ,   :objeto_del_contrato
                            ,   :duraci_n_del_contrato
                            ,   :nombre_del_banco
                            ,   :tipo_de_cuenta
                            ,   :n_mero_de_cuenta
                            ,   :el_contrato_puede_ser_prorrogado
                            ,   :nombre_ordenador_del_gasto
                            ,   :tipo_de_documento_ordenador_del_gasto
                            ,   :n_mero_de_documento_ordenador_del_gasto
                            ,   :nombre_supervisor
                            ,   :tipo_de_documento_supervisor
                            ,   :n_mero_de_documento_supervisor
                            ,   :nombre_ordenador_de_pago
                            ,   :tipo_de_documento_ordenador_de_pago
                            ,   :n_mero_de_documento_ordenador_de_pago
                        )"""
                        conn.execute(text(sql), {
                            'nombre_entidad': datos["nombre_entidad"],
                            'nit_entidad': datos["nit_entidad"],
                            'departamento': datos["departamento"],
                            'ciudad': datos["ciudad"],
                            'localizaci_n': datos["localizaci_n"],
                            'orden': datos["orden"],
                            'sector': datos["sector"],
                            'rama': datos["rama"],
                            'entidad_centralizada': datos["entidad_centralizada"],
                            'proceso_de_compra': datos["proceso_de_compra"],
                            'id_contrato': datos["id_contrato"],
                            'referencia_del_contrato': datos["referencia_del_contrato"],
                            'estado_contrato': datos.get('estado_contrato', 'PENDIENTE'), #datos["estado_contrato"],
                            'codigo_de_categoria_principal': datos["codigo_de_categoria_principal"],
                            'descripcion_del_proceso': datos["descripcion_del_proceso"],
                            'tipo_de_contrato': datos["tipo_de_contrato"],
                            'modalidad_de_contratacion': datos["modalidad_de_contratacion"],
                            'justificacion_modalidad_de': datos["justificacion_modalidad_de"],
                            'fecha_de_firma': datos["fecha_de_firma"],
                            'fecha_de_inicio_del_contrato': datos["fecha_de_inicio_del_contrato"],
                            'fecha_de_fin_del_contrato': datos["fecha_de_fin_del_contrato"],
                            'condiciones_de_entrega': datos["condiciones_de_entrega"],
                            'tipodocproveedor': datos["tipodocproveedor"],
                            'documento_proveedor': datos["documento_proveedor"],
                            'proveedor_adjudicado': 'PENDIENTE', # datos["proveedor_adjudicado"],
                            'es_grupo': datos["es_grupo"],
                            'es_pyme': datos["es_pyme"],
                            'habilita_pago_adelantado': datos["habilita_pago_adelantado"],
                            'liquidaci_n': datos["liquidaci_n"],
                            'obligaci_n_ambiental': datos["obligaci_n_ambiental"],
                            'obligaciones_postconsumo': datos["obligaciones_postconsumo"],
                            'reversion': datos["reversion"],
                            'origen_de_los_recursos': datos["origen_de_los_recursos"],
                            'destino_gasto': datos["destino_gasto"],
                            'valor_del_contrato': datos["valor_del_contrato"],
                            'valor_de_pago_adelantado': datos["valor_de_pago_adelantado"],
                            'valor_facturado': datos["valor_facturado"],
                            'valor_pendiente_de_pago': datos["valor_pendiente_de_pago"],
                            'valor_pagado': datos["valor_pagado"],
                            'valor_amortizado': datos["valor_amortizado"],
                            'valor_pendiente_de': datos["valor_pendiente_de"],
                            'valor_pendiente_de_ejecucion': datos["valor_pendiente_de_ejecucion"],
                            'estado_bpin': datos["estado_bpin"],
                            'c_digo_bpin': datos["c_digo_bpin"],
                            'anno_bpin': datos["anno_bpin"],
                            'saldo_cdp': datos["saldo_cdp"],
                            'saldo_vigencia': datos["saldo_vigencia"],
                            'espostconflicto': datos["espostconflicto"],
                            'dias_adicionados': datos["dias_adicionados"],
                            'puntos_del_acuerdo': datos["puntos_del_acuerdo"],
                            'pilares_del_acuerdo': datos["pilares_del_acuerdo"],
                            'urlproceso': 'PENDIENTE', #datos["urlproceso"],
                            'nombre_representante_legal': datos["nombre_representante_legal"],
                            'nacionalidad_representante_legal': datos["nacionalidad_representante_legal"],
                            'domicilio_representante_legal': datos["domicilio_representante_legal"],
                            'tipo_de_identificaci_n_representante_legal': datos["tipo_de_identificaci_n_representante_legal"],
                            'identificaci_n_representante_legal': datos["identificaci_n_representante_legal"],
                            'g_nero_representante_legal': datos["g_nero_representante_legal"],
                            'presupuesto_general_de_la_nacion_pgn': datos["presupuesto_general_de_la_nacion_pgn"],
                            'sistema_general_de_participaciones': datos["sistema_general_de_participaciones"],
                            'sistema_general_de_regal_as': datos["sistema_general_de_regal_as"],
                            'recursos_propios_alcald_as_gobernaciones_y_resguardos_ind_genas_': datos["recursos_propios_alcald_as_gobernaciones_y_resguardos_ind_genas_"],
                            'recursos_de_credito': datos["recursos_de_credito"],
                            'recursos_propios': datos["recursos_propios"],
                            'codigo_entidad': datos["codigo_entidad"],
                            'codigo_proveedor': datos["codigo_proveedor"],
                            'objeto_del_contrato': datos["objeto_del_contrato"],
                            'duraci_n_del_contrato': datos["duraci_n_del_contrato"],
                            'nombre_del_banco': datos["nombre_del_banco"],
                            'tipo_de_cuenta': datos["tipo_de_cuenta"],
                            'n_mero_de_cuenta': datos["n_mero_de_cuenta"],
                            'el_contrato_puede_ser_prorrogado': datos["el_contrato_puede_ser_prorrogado"],
                            'nombre_ordenador_del_gasto': datos["nombre_ordenador_del_gasto"],
                            'tipo_de_documento_ordenador_del_gasto': datos["tipo_de_documento_ordenador_del_gasto"],
                            'n_mero_de_documento_ordenador_del_gasto': datos["n_mero_de_documento_ordenador_del_gasto"],
                            'nombre_supervisor': datos["nombre_supervisor"],
                            'tipo_de_documento_supervisor': datos["tipo_de_documento_supervisor"],
                            'n_mero_de_documento_supervisor': datos["n_mero_de_documento_supervisor"],
                            'nombre_ordenador_de_pago': datos["nombre_ordenador_de_pago"],
                            'tipo_de_documento_ordenador_de_pago': datos["tipo_de_documento_ordenador_de_pago"],
                            'n_mero_de_documento_ordenador_de_pago': datos["n_mero_de_documento_ordenador_de_pago"]
                        })
    print("Datos insertados exitosamente en 'apl1_contrato_digital'.")
except FileNotFoundError:
    print("El archivo 'informacion.txt' no se encuentra.")
except Exception as e:
    print(f"Ocurrió un error inesperado: {e}")
#--------------------------------------------------------------------------
#--------------------------------------------------------------------------
#--------------------------------------------------------------------------
#--------------------------------------------------------------------------