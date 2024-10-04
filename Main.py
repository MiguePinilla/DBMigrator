import Modulo
import Modulo.DBConnection as SqlObject
from Modulo.DBMigration import DBMigration

if __name__ == "__main__":

    datos = {'DB': ['DWH_MIG', 'DWH_MIG']
            ,'SCHEMA': ['ADP', 'dbo']
            ,'TABLE': ['ATOMO_CREDITOS', 'ACTIVOS']}

    sql = SqlObject.SQLServerConnection('python', 'contra', 'LAPTOP-H6RKFCA1\SQLEXPRESS', 'DWH_MIG', 1433)

    migration = DBMigration(sql)
    migration.setOrigin(datos['DB'][0], datos['SCHEMA'][0], datos['TABLE'][0])
    migration.setDestiny('DWH_MIG', 'PRUEBA')
    migration.genCreateTable()

    createTable = migration.getQuerys()
    #print(createTable["DWH_MIG.PRUEBA.ATOMO_CREDITOS"])
    migration.runCreateTable()