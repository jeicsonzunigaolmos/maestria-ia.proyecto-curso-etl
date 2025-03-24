import yaml
class Config:
    def __init__(self):
        ...
    def retornar_config_por_nombre(self, tipo_config):
        """
            se establece un metodo general que recibe parametro de los datos del config
            reuqeridos por nombre
        """
        config = Config.load_config()
        config = config[tipo_config]
        return config
    @staticmethod
    def load_config(file_path="config/config.yaml"):
        """
            se define una funncion para cargar los archivos .yaml que van a contener
            informacion del archivo de configuracion de base de datos y apikey de la 
            api de datos abiertos
        """
        with open(file_path, "r") as file:
            return yaml.safe_load(file)