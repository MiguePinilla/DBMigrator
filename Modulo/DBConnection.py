from sqlalchemy import create_engine, text
import pandas as pd
import re

class SQLServerConnection:
    
    def __init__(self, usuario, password, servidor, base_datos, puerto = 1433):
        """
        Inicializa la conexión con los atributos necesarios.
        
        :param usuario: Nombre de usuario para la base de datos
        :param password: Contraseña del usuario
        :param servidor: Dirección o IP del servidor SQL Server
        :param base_datos: Nombre de la base de datos a la que se conectará
        :param puerto: Puerto de la base de datos (por defecto 1433)
        """
        self.usuario = usuario
        self.password = password
        self.servidor = servidor
        self.puerto = puerto
        self.base_datos = base_datos
        self.engine = None

    def crear_conexion(self):
        """
        Crea la conexión a la base de datos utilizando SQLAlchemy.
        """
        try:
            print('Conectando')
            conexion_str = f"mssql+pyodbc://{self.usuario}:{self.password}@{self.servidor}:{self.puerto}/{self.base_datos}?driver=ODBC+Driver+17+for+SQL+Server"
            self.engine = create_engine(conexion_str)
            
            with self.engine.connect():
                return True
        
        except Exception as e:
            return e

    def ejecutar_consulta(self, query):
        """
        Ejecuta una consulta (SELECT) y devuelve los resultados en un DataFrame de pandas.
        
        :param query: Consulta SQL a ejecutar
        :return: DataFrame de pandas con los resultados de la consulta
        """
        try:
            if self.engine is None:
                print("No hay una conexión activa.")
                return None

            with self.engine.connect() as conexion:
                resultado = pd.read_sql(text(query), conexion)
            return resultado
        
        except Exception as e:
            return e

    def ejecutar_sentencia(self, sentencia, raw=False):
        """
        Ejecuta una o más sentencias SQL, dividiéndolas en lotes si es necesario.
        
        Este método permite ejecutar sentencias SQL de creación de tablas, inserciones u 
        otras modificaciones de base de datos. Divide la sentencia en lotes utilizando 'GO' 
        como delimitador, permitiendo ejecutar múltiples comandos dentro de una única transacción.
        
        :param sentencia: La sentencia SQL que se desea ejecutar. Puede incluir múltiples 
                        comandos separados por 'GO'.
        :param raw: Si se establece en True, utiliza una conexión sin procesar. Por 
                    defecto es False.
        :return: Retorna True si la ejecución fue exitosa; de lo contrario, devuelve la 
                excepción si ocurre un error durante la ejecución de la sentencia.
        """
        try:
            if self.engine is None:
                print("No hay una conexión activa.")
                return None
            
            lotes = self.dividir_en_lotes(sentencia)

            if raw:
                con = self.engine.raw_connection()
                cursor = con.cursor()
                for lote in lotes:
                    if lote.strip():  # Ignorar lotes vacíos
                        cursor.execute(lote)
                con.commit()
                cursor.close()
            else:
                with self.engine.connect() as conexion:
                    for lote in lotes:
                        if lote.strip():  # Ignorar lotes vacíos
                            conexion.execute(text(lote))
                    conexion.commit()
            return True
        except Exception as e:
            return e
    
    def dividir_en_lotes(self, sentencia):
        """
        Divide una sentencia SQL en lotes separados por el delimitador 'GO'.
        
        SQL Server permite dividir comandos SQL mediante la palabra clave 'GO', que se usa 
        como delimitador entre múltiples comandos. Este método separa cada lote para su 
        ejecución individual, ignorando casos donde 'GO' no aparece solo en una línea.
        
        :param sentencia: La sentencia SQL que puede contener múltiples comandos separados 
                        por 'GO'.
        :return: Una lista de lotes de comandos SQL, donde cada lote es un comando separado 
                listo para ser ejecutado.
        """

        lotes = re.split(r'^\s*GO\s*$', sentencia, flags=re.MULTILINE | re.IGNORECASE)
        return [lote.strip() for lote in lotes if lote.strip()]

# Ejemplo de uso:
# conexion = SQLServerConnection('usuario', 'password', 'servidor', 'base_datos', 'puerto')
# df = conexion.ejecutar_consulta("SELECT * FROM mi_tabla")
# conexion.ejecutar_sentencia("CREATE TABLE nueva_tabla (id INT, nombre VARCHAR(50))")