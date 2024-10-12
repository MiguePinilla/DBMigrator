from . import QueryTemplates as qt
import json

class Migration:
    def __init__(self):
        self.origin_structure = None
        self.destiny_create_table = None
        pass

    def set_origin(self, database_object, database, schema, table):
        self.origin_connection = database_object
        self.origin_database = database
        self.origin_schema = schema
        self.origin_table = table

    def set_destiny(self, database_object, database, schema, table):
        self.destiny_connection = database_object
        self.destiny_database = database
        self.destiny_schema = schema
        self.destiny_table = table

    def migration_type(self):
        return f'{self.origin_connection.type} --> {self.destiny_connection.type}'
    
    def get_origin_structure(self):
        
        query = qt.templates[self.origin_connection.type]["schema"].replace('@@DATABASE@@', self.origin_database.upper()).replace('@@SCHEMA@@', self.origin_schema.upper()).replace('@@TABLE@@', self.origin_table.upper())

        self.origin_structure = self.origin_connection.ejecutar_consulta(query)

        self.origin_structure.columns = self.origin_structure.columns.str.upper()
        return self.origin_structure
    
    def generate_row_syntax(self, row):
        with open('Modulo/Datatypes.json', 'r') as type_mapping:
                types = json.load(type_mapping)

        #Obtener Tipo de Dato
        datatype = row["DATA_TYPE"].lower()  

        #Manejo de longitud y precision/escala
        features = types[self.origin_connection.type][datatype]["feature"]
        if features:
            if features["length"]:
                length = f'({int(row["DATA_LENGTH"])})'
            elif features["precision_scale"]:
                length = f'({int(row["NUMERIC_PRECISION"])},{int(row["NUMERIC_SCALE"])})' 
            else:
                length = ''
        else:
            length = ''


        if self.origin_connection.type != self.destiny_connection.type:
            if f'{datatype}({int(row["DATA_LENGTH"])})' in types[self.origin_connection.type]: #para identificar data_type(l)
                datatype = types[self.origin_connection.type][f'{datatype}({int(row["DATA_LENGTH"])})'][self.destiny_connection.type]
    

            elif datatype in types[self.origin_connection.type]:
                datatype = types[self.origin_connection.type][datatype][self.destiny_connection.type]
                datatype = f'{datatype}{length}'
                


        #Construcción de cadena NULL/NOT NULL
        null_constraint = 'NOT NULL' if row["IS_NULLABLE"] == 'NO' else 'NULL'
        
        #Construcción de sintaxis
        syntax = f'{row["COLUMN_NAME"]} {datatype.upper()} {null_constraint}'
        return syntax

    def generate_destiny_create_table(self):

        if type(self.origin_structure) == type(None):
            self.get_origin_structure()

        columns = self.origin_structure.apply(self.generate_row_syntax, axis = 1)
        columns = ',\n        '.join(columns)
        query = qt.templates[self.destiny_connection.type]["create_table"].replace('@@DATABASE@@', self.destiny_database).replace('@@SCHEMA@@', self.destiny_schema).replace('@@TABLE@@', self.destiny_table)
        
        self.destiny_create_table = query.replace('@@COLUMNS@@', columns)
        return self.destiny_create_table
    
    def run_structure_migration(self):
        if type(self.destiny_create_table) == type(None):
            self.generate_destiny_create_table()

        result = self.destiny_connection.ejecutar_sentencia(self.destiny_create_table)
        return result
    






