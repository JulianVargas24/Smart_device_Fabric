CREATE PROCEDURE dbo.sp_full_load
(
    @SourceDB      sysname,             -- Lakehouse / DB origen (ej: lh_gold)
    @SourceSchema  sysname,             -- Esquema origen (ej: dbo)
    @SourceTable   sysname,             -- Tabla origen (ej: dim_brand)
    @TargetSchema  sysname = 'dbo',     -- Esquema destino en el Warehouse
    @TargetTable   sysname = NULL       -- Tabla destino (si NULL usa @SourceTable)
)
AS
BEGIN
    SET NOCOUNT ON;

    -- Si no se especifica tabla destino, usamos el mismo nombre que la tabla origen
    IF @TargetTable IS NULL
        SET @TargetTable = @SourceTable;

    -- Construimos nombres completos [schema].[table] y [db].[schema].[table]
    DECLARE @TargetFullName  nvarchar(400);
    DECLARE @SourceFullName  nvarchar(400);
    DECLARE @sql             nvarchar(max);

    SET @TargetFullName = QUOTENAME(@TargetSchema) + N'.' + QUOTENAME(@TargetTable);
    SET @SourceFullName = QUOTENAME(@SourceDB) + N'.' + QUOTENAME(@SourceSchema) + N'.' + QUOTENAME(@SourceTable);

    -- SQL dinámico: si existe la tabla, la borra, luego hace CTAS desde el Lakehouse
    SET @sql = N'
IF OBJECT_ID(N''' + @TargetFullName + N''', ''U'') IS NOT NULL
    DROP TABLE ' + @TargetFullName + N';

CREATE TABLE ' + @TargetFullName + N' AS
SELECT *
FROM ' + @SourceFullName + N';';

    -- Ejecutamos
    EXEC sys.sp_executesql @sql;
END;
