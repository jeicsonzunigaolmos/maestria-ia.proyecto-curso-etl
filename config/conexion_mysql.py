import pymysql
import yaml
class ConexionMysql:
    miConexion = None
    cursor = None
    def __init__(self, config_database_name):
        config = self.load_config()
        configDataBase = config[config_database_name]
        self.miConexion = pymysql.connect(host = configDataBase["host"]
            ,   db = configDataBase["database"]
            ,   user = configDataBase["usuario"]
            ,   passwd = configDataBase["contrasena"]
        )
    def ejecutar_select(self, sql):
        print(sql)
        self.cursor = self.miConexion.cursor()
        self.cursor.execute(sql)
        resultado = self.cursor.fetchall()
        # self.miConexion.commit()
        # self.miConexion.close()
        return resultado
    def ejecutar_sql(self, sql):
        print(sql)
        self.cursor = self.miConexion.cursor()
        self.cursor.execute(sql)
        self.miConexion.commit()
    def cerrar_conexion(self):
        self.miConexion.close()
    @staticmethod
    def load_config(file_path="config.yaml"):
        with open(file_path, "r") as file:
            return yaml.safe_load(file)