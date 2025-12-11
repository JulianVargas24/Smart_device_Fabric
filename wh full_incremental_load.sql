CREATE PROCEDURE dbo.sp_incremental_load
(
    @SourceDB         sysname,             -- Lakehouse / DB origen (ej: lh_gold)
    @SourceSchema     sysname,             -- Esquema origen (ej: dbo)
    @SourceTable      sysname,             -- Tabla origen (ej: dim_brand)
    @KeyColumn        sysname,             -- Columna clave (ej: id_brand)
    @WatermarkColumn  sysname,             -- Columna fecha/hora para incremental (ej: last_update_date)
    @TargetSchema     sysname = 'dbo',     -- Esquema destino en el Warehouse
    @TargetTable      sysname = NULL       -- Tabla destino (si NULL usa @SourceTable)
)
AS
BEGIN
    SET NOCOUNT ON;

    IF @TargetTable IS NULL
        SET @TargetTable = @SourceTable;

    DECLARE @TargetFullName      nvarchar(400);
    DECLARE @SourceFullName      nvarchar(400);
    DECLARE @sql                 nvarchar(max);
    DECLARE @MaxWatermark        datetime2;
    DECLARE @ColumnList          nvarchar(max);
    DECLARE @ColumnListSource    nvarchar(max);
    DECLARE @UpdateList          nvarchar(max);

    SET @TargetFullName = QUOTENAME(@TargetSchema) + N'.' + QUOTENAME(@TargetTable);
    SET @SourceFullName = QUOTENAME(@SourceDB) + N'.' + QUOTENAME(@SourceSchema) + N'.' + QUOTENAME(@SourceTable);

    -------------------------------------------------------------------
    -- 1) Si la tabla destino NO existe → carga completa inicial (CTAS)
    -------------------------------------------------------------------
    IF OBJECT_ID(@TargetFullName, 'U') IS NULL
    BEGIN
        SET @sql = N'
            CREATE TABLE ' + @TargetFullName + N' AS
            SELECT *
            FROM ' + @SourceFullName + N';';

        EXEC sys.sp_executesql @sql;
        RETURN;
    END;

    -------------------------------------------------------------------
    -- 2) Obtener la lista de columnas de la tabla destino
    -------------------------------------------------------------------
    SELECT 
        @ColumnList = STRING_AGG(QUOTENAME(name), ','),
        @ColumnListSource = STRING_AGG('S.' + QUOTENAME(name), ',')
    FROM sys.columns
    WHERE object_id = OBJECT_ID(@TargetFullName);

    -- Lista de columnas para el UPDATE (todas menos la clave)
    SELECT 
        @UpdateList = STRING_AGG('T.' + QUOTENAME(name) + ' = S.' + QUOTENAME(name), ',')
    FROM sys.columns
    WHERE object_id = OBJECT_ID(@TargetFullName)
      AND name <> @KeyColumn; -- opcionalmente también podrías excluir @WatermarkColumn

    -------------------------------------------------------------------
    -- 3) Leer la máxima fecha/hora (watermark) que ya está en destino
    -------------------------------------------------------------------
    SET @sql = N'
        SELECT @MaxWM_OUT = ISNULL(MAX(' + QUOTENAME(@WatermarkColumn) + N'), ''1900-01-01'')
        FROM ' + @TargetFullName + N';';

    EXEC sys.sp_executesql
        @sql,
        N'@MaxWM_OUT datetime2 OUTPUT',
        @MaxWM_OUT = @MaxWatermark OUTPUT;

    -------------------------------------------------------------------
    -- 4) MERGE incremental: solo filas nuevas/actualizadas
    -------------------------------------------------------------------
    SET @sql = N'
        MERGE ' + @TargetFullName + N' AS T
        USING (
            SELECT *
            FROM ' + @SourceFullName + N'
            WHERE ' + QUOTENAME(@WatermarkColumn) + N' > @MaxWM
        ) AS S
        ON T.' + QUOTENAME(@KeyColumn) + N' = S.' + QUOTENAME(@KeyColumn) + N'
        WHEN MATCHED THEN
            UPDATE SET ' + @UpdateList + N'
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (' + @ColumnList + N')
            VALUES (' + @ColumnListSource + N');';

    EXEC sys.sp_executesql
        @sql,
        N'@MaxWM datetime2',
        @MaxWM = @MaxWatermark;
END;
