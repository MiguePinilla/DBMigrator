from . import StructureMigration as st
from . import QueryTemplates as qt

class DataMigration(st.StructureMigration):
    
    def truncate_destiny_table(self):
        try:
            print('EJECUTANDO TRUNCATE')
            truncate_query = qt.templates[self.destiny_connection.type]["truncate_Table"].replace(
                '@@DATABASE@@', self.destiny_database
            ).replace(
                '@@SCHEMA@@', self.destiny_schema
            ).replace(
                '@@TABLE@@', self.destiny_table)
                    
            result = self.destiny_connection.ejecutar_sentencia(truncate_query)
            if result == True:
                return result
            else:
                return f'Error al truncar tabla en destino: {result}'
            
        except Exception as e:
            return f'Error al truncar tabla en destino: {e}'

    def check_destiny_table_exists(self):
        #Comprobar si la estructura ya existe
        destiny_table_query = qt.templates[self.destiny_connection.type]["table"].replace(
            '@@DATABASE@@', self.destiny_database
        ).replace(
            '@@SCHEMA@@', self.destiny_schema
        ).replace(
            '@@TABLE@@', self.destiny_table)

        destiny_table_created = self.destiny_connection.ejecutar_consulta_dataframe(destiny_table_query)

        if destiny_table_created.empty:
            #print(f'La tabla {self.destiny_table} no existe en el destino')
            return False
        else:
            return True
    
    def generate_columns(self):

        columns = '\n'.join(self.destiny_create_table.splitlines()[11:-3]).replace('NOT NULL', '').replace('NULL', '')  
        return columns
    
    def run_data_migration(self, truncate=False):
        try:
            if type(self.destiny_create_table) == type(None):
                self.generate_destiny_create_table()

            destiny_already_exists = self.check_destiny_table_exists()

            if destiny_already_exists == True:
                if truncate == True:
                    self.truncate_destiny_table()
            else:
                self.run_structure_migration()

            origin_database = f'{self.origin_database}.' if self.origin_database != '' else ''
            origin_schema = f'{self.origin_schema}.' if self.origin_schema != '' else ''
            origin_table_route = origin_database + origin_schema + self.origin_table
            origin_data = self.origin_connection.ejecutar_consulta_json(f'SELECT * FROM {origin_table_route}')

            columns = self.generate_columns()

            insert_query = qt.templates[self.destiny_connection.type]["insert_json"].replace(
                '@@JSON@@', origin_data
            ).replace(
                "@@DATABASE@@", self.destiny_database
            ).replace(
                "@@SCHEMA@@", self.destiny_schema
            ).replace(
                '@@TABLE@@', self.destiny_table
            ).replace('@@COLUMNS@@', columns)

            result = self.destiny_connection.ejecutar_sentencia(insert_query, True)
            if result == True:
                return result
            else:
                return f'Error al ejecutar la Migración de Datos: {result}'
        except Exception as e:
            return f'Error en la Migración de Datos: {str(e)}'