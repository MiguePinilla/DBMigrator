from . import QueryTemplates as qt
import json

class Migration:
    def __init__(self):
        """
        Inicializa la clase Migration.
        Establece los atributos origin_structure y destiny_create_table como None.
        """
        self.origin_structure = None
        self.destiny_create_table = None

    def set_origin(self, database_object, database, schema, table):
        """
        Establece la conexión y parámetros para la base de datos de origen.

        :param database_object: Objeto de conexión a la base de datos de origen.
        :param database: Nombre de la base de datos de origen.
        :param schema: Esquema de la base de datos de origen.
        :param table: Nombre de la tabla de origen.
        """
        self.origin_connection = database_object
        self.origin_database = database
        self.origin_schema = schema
        self.origin_table = table

    def set_destiny(self, database_object, database, schema, table):
        """
        Establece la conexión y parámetros para la base de datos de destino.

        :param database_object: Objeto de conexión a la base de datos de destino.
        :param database: Nombre de la base de datos de destino.
        :param schema: Esquema de la base de datos de destino.
        :param table: Nombre de la tabla de destino.
        """
        self.destiny_connection = database_object
        self.destiny_database = database
        self.destiny_schema = schema
        self.destiny_table = table

    def migration_type(self):
        """
        Devuelve el tipo de migración entre las bases de datos de origen y destino.

        :return: Cadena que indica el tipo de migración en el formato "origen --> destino".
        """
        return f'{self.origin_connection.type} --> {self.destiny_connection.type}'
    
    def get_origin_structure(self):
        """
        Obtiene la estructura de la tabla de origen y la almacena en origin_structure.
        Realiza una consulta para obtener las columnas y tipos de datos de la tabla de origen.

        :return: DataFrame de pandas con la estructura de la tabla de origen.
        """
        
        query = qt.templates[self.origin_connection.type]["schema"].replace('@@DATABASE@@', self.origin_database.upper()).replace('@@SCHEMA@@', self.origin_schema.upper()).replace('@@TABLE@@', self.origin_table.upper())

        self.origin_structure = self.origin_connection.ejecutar_consulta(query)

        self.origin_structure.columns = self.origin_structure.columns.str.upper()
        return self.origin_structure
    
    def generate_row_syntax(self, row):
        """
        Genera la sintaxis de la fila para la creación de la tabla en la base de datos de destino.
        Mapea los tipos de datos y determina la longitud y características basadas en la tabla de origen.

        :param row: Fila del DataFrame que representa una columna de la tabla de origen.
        :return: Cadena que representa la definición de la columna en la base de datos de destino.
        """

        origin_database_type = self.origin_connection.type
        destiny_database_type = self.destiny_connection.type

        with open('Modulo/Datatypes.json', 'r') as type_mapping:
                types = json.load(type_mapping)[origin_database_type]

        #Obtener Tipo de Dato
        column_name = row["COLUMN_NAME"]
        data_type = row["DATA_TYPE"].lower()  
        data_length = int(row["DATA_LENGTH"])
        data_precision = int(row["NUMERIC_SCALE"])
        data_scale = int(row["NUMERIC_SCALE"])
        data_null = 'NOT NULL' if row["IS_NULLABLE"] == 'NO' else 'NULL'

        
        #Tipo de Migración Heterogénea
        if origin_database_type != destiny_database_type:

            #Mapeo de Tipo de Dato
            if f'{data_type}({data_length})' in types:
                data_type = f'{data_type}({data_length})'
                final_type = types[data_type][destiny_database_type]
                features = types[data_type]["feature"]

            elif data_type in types:
                final_type = types[data_type][destiny_database_type]
                features = types[data_type]["feature"]
            
            #Si no encuentra del Tipo de dato en el diccionario
            else:
                final_type = data_type
                features = False

                final_length = '/*NOT FOUND*/'
            
            #Determina si tiene Longitud o Precisión y escala
            if features:
                if features["length"] == True:
                    final_length = f'({data_length})'
                elif features["precision_scale"] == True:
                    final_length = f'({data_precision},{data_scale})'
                else:
                    final_length = ''
        
        #Tipo de Migración Homogénea
        else:
            final_type = data_type
            #Mapeo de Tipo de Dato
            if data_type in types:
                features = types[data_type]["feature"]
                if features["length"] == True:
                    final_length = f'({data_length})'
                elif features["precision_scale"] == True:
                    final_length = f'({data_precision},{data_scale})'
                else:
                    final_length = ''
            else:
                #Si no encuentra del Tipo de dato en el diccionario
                final_length = '/*DATATYPE NOT FOUND*/'

        syntax = f'{column_name} {final_type.upper()}{final_length} {data_null}'
        return syntax

    def generate_destiny_create_table(self):
        """
        Genera la sentencia SQL para crear la tabla en la base de datos de destino.
        Utiliza la estructura de la tabla de origen para construir la sentencia de creación.

        :return: Cadena que representa la sentencia SQL para crear la tabla en la base de datos de destino.
        """

        if type(self.origin_structure) == type(None):
            self.get_origin_structure()

        columns = self.origin_structure.apply(self.generate_row_syntax, axis = 1)
        columns = ',\n        '.join(columns)
        query = qt.templates[self.destiny_connection.type]["create_table"].replace('@@DATABASE@@', self.destiny_database).replace('@@SCHEMA@@', self.destiny_schema).replace('@@TABLE@@', self.destiny_table)
        
        self.destiny_create_table = query.replace('@@COLUMNS@@', columns)
        return self.destiny_create_table
    
    def run_structure_migration(self):
        """
        Ejecuta la migración de la estructura de la tabla al destino.
        Crea la tabla en la base de datos de destino utilizando la sentencia generada.

        :return: Resultado de la ejecución de la sentencia para crear la tabla en la base de datos de destino. (True/Error)
        """
        if type(self.destiny_create_table) == type(None):
            self.generate_destiny_create_table()

        result = self.destiny_connection.ejecutar_sentencia(self.destiny_create_table)
        return result
    






