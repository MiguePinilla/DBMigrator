from . import QueryTemplates as qt

class Migration:
    def __init__(self):
        pass

    def setOrigin(self, dbObject, database, schema, table):
        self.OriginCon = dbObject
        self.OriginDatabase = database
        self.OriginSchema = schema
        self.OriginTable = table

    def setDestiny(self, dbObject, database, schema, table):
        self.DestinyCon = dbObject
        self.DestinyDatabase = database
        self.DestinySchema = schema
        self.DestinyTable = table

    def migrationType(self):
        return f'{self.OriginCon.type} --> {self.DestinyCon.type}'
    
    def getOriginStr(self):
        
        query = qt.templates[self.OriginCon.type]["schema"].replace('@@DATABASE@@', self.OriginDatabase).replace('@@SCHEMA@@', self.OriginSchema).replace('@@TABLE@@', self.OriginTable)

        self.OriginStructure = self.OriginCon.ejecutar_consulta(query)
        return self.OriginStructure
    
    def genDestinyQuery(self):

        if self.OriginCon.type == self.DestinyCon.type:
            #Proceso de migracion de estrucutras a bases de datos Homogéneas
            qt.templates["sql_server"]["TableCreation"].replace('@@DATABASE@@', self.DestinyDatabase).replace('@@SCHEMA@@', self.DestinySchema).replace('@@TABLE@@', self.DestinyTable)





