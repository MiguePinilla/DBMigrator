templates = {"sqlserver": {
    "create_table" : '''
USE @@DATABASE@@
GO

IF NOT EXISTS (SELECT SCHEMA_ID FROM sys.schemas WHERE [name] = '@@SCHEMA@@')
BEGIN
	EXECUTE('CREATE SCHEMA @@SCHEMA@@');
END;
GO

CREATE TABLE @@SCHEMA@@.@@TABLE@@ (
	@@COLUMNS@@
)
GO
	''',
    
    "schema": '''
SELECT
	ORDINAL_POSITION AS ORDEN
	,TABLE_CATALOG
	,TABLE_SCHEMA
	,TABLE_NAME 
	,COLUMN_NAME
	,IS_NULLABLE
	,DATA_TYPE
	,COALESCE(CHARACTER_MAXIMUM_LENGTH, 0) AS DATA_LENGTH
	,COALESCE(NUMERIC_PRECISION, 0) AS NUMERIC_PRECISION
	,COALESCE(NUMERIC_SCALE, 0) AS NUMERIC_SCALE

FROM @@DATABASE@@.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = '@@SCHEMA@@' AND TABLE_NAME = '@@TABLE@@'
	''',
    
	"table": '''
SELECT 1
FROM @@DATABASE@@.INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = '@@SCHEMA@@' AND TABLE_NAME = '@@TABLE@@'
	''',
    
	"truncate_Table": '''
TRUNCATE TABLE @@DATABASE@@.@@SCHEMA@@.@@TABLE@@
	''',
    
    "insert_json": '''
DECLARE @JSON AS NVARCHAR(MAX) = '@@JSON@@'

INSERT INTO @@DATABASE@@.@@SCHEMA@@.@@TABLE@@ SELECT * FROM OPENJSON(@JSON) WITH(
	@@COLUMNS@@
)
	'''
	},
    
    "oracle": {
        "create_table": '''
CREATE TABLE @@TABLE@@ (
	@@COLUMNS@@
)
		''',
    
    	"schema": '''
SELECT
    COLUMN_ID AS ORDEN
    ,'' AS TABLE_CATALOG
    ,OWNER AS TABLE_SCHEMA
    ,TABLE_NAME
    ,COLUMN_NAME
    ,CASE NULLABLE 
        WHEN 'N' THEN 'NO'
        ELSE 'YES'
    END AS IS_NULLABLE
    ,DATA_TYPE
    ,COALESCE(DATA_LENGTH, 0) AS DATA_LENGTH
    ,COALESCE(DATA_PRECISION, 0) AS NUMERIC_PRECISION
    ,COALESCE(DATA_SCALE, 0) AS NUMERIC_SCALE
    
FROM all_tab_columns
WHERE  TABLE_NAME = '@@TABLE@@' --AND OWNER = '@@SCHEMA@@'
	''',
    
	"table": '''
SELECT 1 FROM ALL_TABLES WHERE TABLE_NAME = '@@TABLE@@' --AND OWNER = '@@SCHEMA@@'
	''',
    
	"truncate_Table": '''
TRUNCATE TABLE @@TABLE@@
	''',
    
    "insert_json": '''
DECLARE
    v_json CLOB;
    
BEGIN
    v_json := '@@JSON@@';
    
    INSERT INTO @@TABLE@@
    SELECT * FROM JSON_TABLE(
        v_json,
        '$[*]' COLUMNS (
            @@COLUMNS@@
        )
    );
    
END;
/
	'''
	}
}