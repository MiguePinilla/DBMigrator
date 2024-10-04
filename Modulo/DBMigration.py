from . import QueryTemplates as qt
#import pandas as pd
import pdb


def genCreateTable(esquema):
    columnas = '\n  ,'.join(esquema["SYNTAXIS"])
    query = qt.TableCreation.replace('@@BD@@', bd).replace('@@ESQUEMA@@', 'PRUEBA').replace('@@TABLA@@', table).replace('@@COLUMNAS@@', columnas)

    # with open(f'createTable_{schema}_{table}.sql', 'w') as file:
    #     file.write(query)
    return query

def main(SqlObject, bd, schema, table, runQuery = False):
    print('Creacion de objeto de conexión')

    con_message = SqlObject.crear_conexion()
    if con_message == True:
        print('Conexión exitosa')
        print('Realizando Consulta')
        
        
        table_schema_query = qt.SQLServer_Schema.replace('@@BD@@', bd).replace('@@ESQUEMA@@', schema).replace('@@TABLA@@', table)

        df = SqlObject.ejecutar_consulta(table_schema_query)
        createTable = genCreateTable(df)

        if runQuery ==True:
            ejecucion = SqlObject.ejecutar_sentencia(createTable, True)
            if ejecucion == True:
                print('Query ejecutada con exito!')
            else:
                print(f'Ha surgido un error en la ejecución:\n{ejecucion}')

        return createTable
    else:
        print(f'Ha surgido un error en la conexión:\n{con_message}')