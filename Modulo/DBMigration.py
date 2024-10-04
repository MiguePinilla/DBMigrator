from . import QueryTemplates as qt
import pdb


class DBMigration:

    def __init__(self, SqlObject):
        self.SqlObject = SqlObject
        self.Querys = {}

    def getQuerys(self):
        return self.Querys

    def runCreateTable(self):
        for key, value in self.Querys.items():
            print(f"Ejecutando: {key}\n")
            ejecucion = self.SqlObject.ejecutar_sentencia(value, True)
            if ejecucion == True:
                print('Query ejecutada con exito!')
            else:
                print(f'Ha surgido un error en la ejecución:\n{ejecucion}')

    def setOrigin(self, bd, schema, table):
        self.Origin = {
            "BD" : bd
            ,"SCHEMA": schema
            ,"TABLE": table
        }
        self.OriginBD = bd
        self.OriginSchema = schema
        self.OriginTable = table

    def setDestiny(self, bd, schema):
        self.DestinyBD = bd
        self.DestinySchema = schema
    
    def genCreateTable(self):

        con_message = self.SqlObject.crear_conexion()
        if con_message == True:
            print('Conexión exitosa')
            print('Realizando Consulta')
            
            table_schema_query = qt.SQLServer_Schema.replace('@@BD@@', self.OriginBD).replace('@@ESQUEMA@@', self.OriginSchema).replace('@@TABLA@@', self.OriginTable)

            #CREACION DE SENTENCIA CREATE TABLE
            esquema = self.SqlObject.ejecutar_consulta(table_schema_query)
            columnas = '\n  ,'.join(esquema["SYNTAXIS"])
            createTable = qt.TableCreation.replace('@@BD@@', self.DestinyBD).replace('@@ESQUEMA@@', self.DestinySchema).replace('@@TABLA@@', self.OriginTable).replace('@@COLUMNAS@@', columnas)

            self.Querys[f'{self.DestinyBD}.{self.DestinySchema}.{self.OriginTable}'] = createTable

            return True
        
        else:
            return (f'Ha surgido un error en la conexión:\n{con_message}')