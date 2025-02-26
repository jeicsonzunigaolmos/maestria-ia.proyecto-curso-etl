# Título del Proyecto: Analisis de datos contratacion Secop II
# Pre-requisitos:
# Si dentro del repositorio no encuentra la carpeta venv siga los siguientes pasos
# Dentro de la carpeta ejecutamos
python -m venv venv
# El ambiente se crea pero no se encuentra activo
# La forma de activarlo en windows es:
venv\scripts\activate
# La forma de activarlo en linux es source:
source venv/Scripts/activate
# Si se presentan errores ejecutar en consola:
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
# Instalar las siguientes librerias
pip install pandas
pip install sodapy
pip install pyyaml
pip install psycopg2
pip install sqlalchemy
python main.py
# Cree y configure el archivo .yaml
<!-->
configApi:
  Url: "www.datos.gov.co"
  MyAppToken: ""
  username: ""
  password: ""
  limit: 100000
database:
  host: ""
  usuario: ""
  contrasena: ""
  puerto: ""
-->
# Dirijase a la carpeta proyectoetl y ejecute el siguiente comando:
python main.py
# Autores:
# 1. Jeicson Andres Zuñiga Olmos
# 2. Carlos Fernando Escobar
# Proyecto Curso ETL - Maestria en IA.