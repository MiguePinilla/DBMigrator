from . import QueryTemplates as qt
import json

class Migration:
    def __init__(self):
        self.OriginStructure = None
        self.DestinyCreateTable = None
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
    
    def getOriginStructure(self):
        
        query = qt.templates[self.OriginCon.type]["schema"].replace('@@DATABASE@@', self.OriginDatabase.upper()).replace('@@SCHEMA@@', self.OriginSchema.upper()).replace('@@TABLE@@', self.OriginTable.upper())

        self.OriginStructure = self.OriginCon.ejecutar_consulta(query)
        self.OriginStructure.columns = self.OriginStructure.columns.str.upper()

        return self.OriginStructure
    
    def genSyntax(self, row):
        with open('Modulo/Datatypes.json', 'r') as type_mapping:
                types = json.load(type_mapping)

        #Obtener Tipo de Dato
        datatype = row["DATA_TYPE"].lower()

        #Construcción de cadena NULL/NOT NULL
        null_constraint = 'NOT NULL' if row["IS_NULLABLE"] == 'NO' else 'NULL'

        #Manejo de longitud y precision/escala
        features = types[self.OriginCon.type][datatype]["feature"]
        if features["length"]:
            length = f'({int(row["DATA_LENGTH"])})'
        elif features["precision_scale"]:
            length = f'({int(row["NUMERIC_PRECISION"])},{int(row["NUMERIC_SCALE"])})' 
        else:
            length = ''

        if self.OriginCon.type != self.DestinyCon.type:
            if f'{datatype}({int(row["DATA_LENGTH"])})' in types[self.OriginCon.type]: #para identificar data_type(l)
                datatype = types[self.OriginCon.type][f'{datatype}({int(row["DATA_LENGTH"])})'][self.DestinyCon.type]

            elif datatype in types[self.OriginCon.type]:
                datatype = types[self.OriginCon.type][datatype][self.DestinyCon.type]
                datatype = f'{datatype}{length}'

        #Construcción de sintaxis
        syntax = f'{row["COLUMN_NAME"]} {datatype.upper()} {null_constraint}'
        return syntax

    def genDestinyQuery(self):

        if type(self.OriginStructure) == type(None):
            self.getOriginStructure()

        columns = self.OriginStructure.apply(self.genSyntax, axis = 1)
        columns = ',\n        '.join(columns)
        query = qt.templates[self.DestinyCon.type]["TableCreation"].replace('@@DATABASE@@', self.DestinyDatabase).replace('@@SCHEMA@@', self.DestinySchema).replace('@@TABLE@@', self.DestinyTable)
        self.DestinyCreateTable = query.replace('@@COLUMNS@@', columns)
        return self.DestinyCreateTable
    
    def runStructureMigration(self):
        if type(self.DestinyCreateTable) == type(None):
            self.genDestinyQuery()

        result = self.DestinyCon.ejecutar_sentencia(self.DestinyCreateTable)
        return result
    






