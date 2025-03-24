from config.conexion_postgres import ConexionPostgres
import config.consultas_sql as consultas_sql
from sqlalchemy import text
import json
class Carga:
    def __init__(self):
        ...
    
    def carga_en_base_datos():
        ...
    def crear_tabla_apl1_consolidado(self):
        conexion_postgres = ConexionPostgres()
        conexion_postgres.ejecutar_sql(consultas_sql.crear_tabla_apl1_consolidado())
    def limpiar_tabla_apl1_consolidado(self):
        conexion_postgres = ConexionPostgres()
        conexion_postgres.ejecutar_sql(consultas_sql.limpiar_tabla_apl1_consolidado())
    def carga_en_database(self):
        conexion_postgres = ConexionPostgres()
        contador = 0
        try:
            with open('data/transformaciones.json', 'r', encoding='utf-8', errors='ignore') as archivo:
                archivo = json.load(archivo)
                for fila in archivo:
                    contador += 1
                    print(f"contador carga ----> {contador} - {fila["departamento"]}")
                    conexion_postgres.ejecutar_sql_dicionario(text(consultas_sql.insert_apl1_consolidado()), {
						'departamento': fila["departamento"],
						'ciudad': fila["ciudad"],
						'orden': fila["orden"],
						'sector': fila["sector"],
						'rama': fila["rama"],
						'estado_contrato': fila["estado_contrato"],
						'tipo_de_contrato': fila["tipo_de_contrato"],
						'fecha_de_firma': fila["fecha_de_firma"],
						'fecha_de_inicio_del_contrato': fila["fecha_de_inicio_del_contrato"],
						'fecha_de_fin_del_contrato': fila["fecha_de_fin_del_contrato"],
						'destino_gasto': fila["destino_gasto"],
						'valor_del_contrato': fila["valor_del_contrato"],
						'valor_pendiente_de_pago': fila["valor_pendiente_de_pago"],
						'tipodocproveedor': fila["tipodocproveedor"],
						'documento_proveedor': fila["documento_proveedor"],
						'proveedor_adjudicado': fila["proveedor_adjudicado"],
						'descripcion_del_proceso': fila["descripcion_del_proceso"],
						'fecha_de_firma_dia': fila["fecha_de_firma_dia"],
						'fecha_de_firma_mes': fila["fecha_de_firma_mes"],
						'fecha_de_firma_anio': fila["fecha_de_firma_anio"],
						'fecha_de_inicio_del_contrato_dia': fila["fecha_de_inicio_del_contrato_dia"],
						'fecha_de_inicio_del_contrato_mes': fila["fecha_de_inicio_del_contrato_mes"],
						'fecha_de_inicio_del_contrato_anio': fila["fecha_de_inicio_del_contrato_anio"],
						'fecha_de_fin_del_contrato_dia': fila["fecha_de_fin_del_contrato_dia"],
						'fecha_de_fin_del_contrato_mes': fila["fecha_de_fin_del_contrato_mes"],
						'fecha_de_fin_del_contrato_anio': fila["fecha_de_fin_del_contrato_anio"],
						'clasificacion_valor_del_contrato': fila["clasificacion_valor_del_contrato"],
						'valor_pendiente_por_ejecutar': fila["valor_pendiente_por_ejecutar"],
						'porcentaje_valor_contrato_ejecutado': fila["porcentaje_valor_contrato_ejecutado"],
						'contratos_inferiores_2024_pendientes_pago': fila["contratos_inferiores_2024_pendientes_pago"],
					})
            print("Datos insertados exitosamente en 'apl1_consolidado'.")
            print(f"Resumen: contador = {contador}")
        except FileNotFoundError:
            print("El archivo 'transformaciones.json' no se encuentra.")
        except Exception as e:
            print(f"Ocurrió un error inesperado en la carga: {e}")