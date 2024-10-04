from DBConnection import SQLServerConnection
import QueryTemplates as qt

#import pdb

def genCreateTable(db, schema, table, esquema):
    columnas = '\n  ,'.join(esquema["SYNTAXIS"])
    query = qt.TableCreation.replace('@@BD@@', db).replace('@@ESQUEMA@@', schema).replace('@@TABLA@@', table).replace('@@COLUMNAS@@', columnas)

    # with open(f'createTable_{schema}_{table}.sql', 'w') as file:
    #     file.write(query)
    return query

def main(datos):
    print('Creacion de objeto de conexión')
    sql = SQLServerConnection('python', 'contra', 'LAPTOP-H6RKFCA1\SQLEXPRESS', 'DWH_MIG', 1433)

    con_message = sql.crear_conexion()
    if con_message == True:
        print('Conexión exitosa')
        print('Realizando Consulta')
        
        for i in range(len(datos)-1):

            db = datos['DB'][i]
            schema = datos['SCHEMA'][i]
            table = datos['TABLE'][i]
            table_schema_query = qt.SQLServer_Schema.replace('@@BD@@', db).replace('@@ESQUEMA@@', schema).replace('@@TABLA@@', table)

            df = sql.ejecutar_consulta(table_schema_query)
            createTable = genCreateTable(db, schema, table, df)
            print(createTable)
        
        # ejecucion = sql.ejecutar_sentencia(createTable, True)
        # if ejecucion == True:
        #     print('Query ejecutada con exito!')
        # else:
        #     print(f'Ha surgido un error en la ejecución:\n{ejecucion}')
        # sql.ejecutar_sentencia()
    else:
        print(f'Ha surgido un error en la conexión:\n{con_message}')

if __name__ == "__main__":
    
    #main(datos)

    datos = {'DB': ['DWH_MIG', 'DWH_MIG']
        ,'SCHEMA': ['ADP', 'dbo']
        ,'TABLE': ['ATOMO_CREDITOS', 'ACTIVOS']}

    main(datos)
    #pdb.set_trace()