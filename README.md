# Módulo de Migración de Estructuras y Datos entre Bases de Datos

Este proyecto proporciona un conjunto de herramientas para la migración de estructuras y datos entre diferentes tipos de bases de datos (SQL Server, Oracle, entre otras). Utiliza SQLAlchemy para las conexiones y pandas para manipular datos en consultas. El sistema soporta migraciones tanto homogéneas (dentro del mismo tipo de base de datos) como heterogéneas (entre diferentes tipos de bases de datos).

## Características Principales

- Migración de estructuras de tablas entre bases de datos (SQL Server, Oracle, PostgreSQL, MySQL).
- Migración de datos entre bases de datos utilizando sentencias SQL.
- Consultas personalizadas que retornan los resultados en formato JSON o DataFrames de pandas.
- Creación de tablas en bases de datos de destino, con soporte para distintos tipos de datos y conversiones entre tipos.

## Archivos Clave

### `DBConnection.py`

Define las clases base para manejar las conexiones a las bases de datos, así como las implementaciones específicas para SQL Server y Oracle.

#### Clases principales:
- **DatabaseConnection (ABC)**: Clase abstracta que define los métodos comunes para todas las conexiones.
- **SQLServer**: Implementa la conexión a una base de datos SQL Server. Soporta consultas y ejecuciones de sentencias, tanto en DataFrame como en formato JSON.
- **Oracle**: Implementa la conexión a una base de datos Oracle. Soporta las mismas operaciones que SQLServer, incluyendo la ejecución de consultas en JSON.

#### Métodos relevantes:
- `crear_conexion()`: Crea una conexión a la base de datos.
- `ejecutar_consulta_dataframe(query)`: Ejecuta una consulta y devuelve los resultados en un DataFrame de pandas.
- `ejecutar_sentencia(sentencia, raw=False)`: Ejecuta sentencias SQL, dividiendo en lotes si es necesario.
  
### `Migration.py`

Este archivo define la clase `Migration` que maneja la migración de estructuras y datos de tablas entre diferentes bases de datos.

#### Características clave:
- **set_origin(database_object, database, schema, table)**: Establece la conexión a la base de datos de origen y define los parámetros de la tabla.
- **set_destiny(database_object, database, schema, table)**: Establece la conexión a la base de datos de destino.
- **generate_destiny_create_table()**: Genera el SQL para crear la tabla en la base de datos de destino, basándose en la estructura de la base de datos de origen.
- **migracion.run_data_migration()**: Ejecuta el proceso de migración de datos entre bases de datos y la migración de estructura de ser necesario.
  

### `Datatypes.json`

Un archivo JSON que contiene el mapeo de tipos de datos entre diferentes sistemas de bases de datos (SQL Server, Oracle, PostgreSQL, MySQL). Este archivo es usado para traducir tipos de datos entre bases de datos heterogéneas.

### `QueryTemplates.py`

Contiene plantillas SQL para operaciones comunes en cada tipo de base de datos. Estas plantillas son utilizadas para generar las consultas y sentencias necesarias para la migración de estructuras y datos.

## Instalación

1. Clonar el repositorio.
2. Instalar las dependencias utilizando `pip`:

   ```bash
   pip install -r requirements.txt
3. Configurar las credenciales de base de datos para SQL Server u Oracle en las clases SQLServer u Oracle.

## Uso
### Ejemplo de uso básico:
```python
import DBConnection as db
from StructureMigration import Migration

# Conexión a la base de datos SQL Server de origen
conexion_origen = db.SQLServer('usuario', 'password', 'servidor', 'base_datos')
conexion_origen.crear_conexion()

# Configuración de migración
migration = Migration()
migration.set_origin(conexion_origen, 'mi_base_datos', 'dbo', 'mi_tabla')

# Conexión a la base de datos de destino (Oracle en este caso)
conexion_destino = db.Oracle('usuario', 'password', 'servidor', 'servicio')
conexion_destino.crear_conexion()

migration.set_destiny(conexion_destino, 'base_destino', 'esquema_destino', 'tabla_destino')

# Ejecutar la migración de estructura
migration.run_structure_migration()

# Ejecutar la migración de datos
migration.run_data_migration()
```
## Migración de Estructura:
1. Establecer las conexiones de origen y destino.
2. Generar la tabla en el destino utilizando generate_destiny_create_table().
3. Ejecutar la migración de estructura utilizando run_structure_migration().

## Contribuciones
Si deseas contribuir, por favor crea un pull request o abre un issue para discutir los cambios propuestos.

## Licencia
Este proyecto está bajo la Licencia MIT. Consulta el archivo LICENSE para más detalles.