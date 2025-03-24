from sqlalchemy import create_engine, text
from config.config import Config
from sqlalchemy import text
from psycopg2 import sql
import psycopg2
class ConexionPostgres:
    config = None
    def __init__(self):
        self.config = Config()
        self.configDataBase = self.config.retornar_config_por_nombre("configDataBase")
        self.engine = create_engine(f"postgresql://{self.configDataBase["usuario"]}:{self.configDataBase["contrasena"]}@{self.configDataBase["host"]}:{self.configDataBase["puerto"]}/{self.configDataBase["base_datos"]}")
    def crear_base_datos(self):
        conn = psycopg2.connect(
            host = self.configDataBase["host"],
            user = self.configDataBase["usuario"],
            password = self.configDataBase["contrasena"],
            port = self.configDataBase["puerto"]
        )
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
    def ejecutar_select(self, sql):
        resultado = None
        lista_dicionario = []
        with self.engine.connect() as conn:
            with conn.begin():
                resultado = conn.execute(text(sql))
        for registro in resultado:
            lista_dicionario.append(dict(registro._mapping))
        return lista_dicionario
    def ejecutar_sql(self, sql):
        with self.engine.connect() as conn:
            conn.execute(text(sql))
            conn.commit()
    def ejecutar_sql_dicionario(self, sql, diccionario):
        with self.engine.connect() as conn:
            conn.execute(sql, diccionario)
            conn.commit()