CREATE OR ALTER PROCEDURE [dbo].[sp_GenericDataImport]
    @ImportFileName VARCHAR(1000),
    @DropOrAppend VARCHAR(1)
AS
BEGIN
    SET NOCOUNT ON;

    -- 1. Table Management
    IF @DropOrAppend = 'D'
    BEGIN
        IF OBJECT_ID('[dbo].[GenericDataImport]', 'U') IS NOT NULL 
            DROP TABLE [dbo].[GenericDataImport];

        CREATE TABLE [dbo].[GenericDataImport] (
            ID INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
            FN VARCHAR(1000),
            OriginalValue VARCHAR(MAX),
            col01varchar VARCHAR(1000), col02varchar VARCHAR(1000), col03varchar VARCHAR(1000), col04varchar VARCHAR(1000), col05varchar VARCHAR(1000),
            col06varchar VARCHAR(1000), col07varchar VARCHAR(1000), col08varchar VARCHAR(1000), col09varchar VARCHAR(1000), col10varchar VARCHAR(1000),
            col01other VARCHAR(1000), col02other VARCHAR(1000), col03other VARCHAR(1000), col04other VARCHAR(1000), col05other VARCHAR(1000),
            col06other VARCHAR(1000), col07other VARCHAR(1000), col08other VARCHAR(1000), col09other VARCHAR(1000), col10other VARCHAR(1000),
            col11other VARCHAR(1000), col12other VARCHAR(1000), col13other VARCHAR(1000), col14other VARCHAR(1000), col15other VARCHAR(1000),
            col16other VARCHAR(1000), col17other VARCHAR(1000), col18other VARCHAR(1000), col19other VARCHAR(1000), col20other VARCHAR(1000),
            uf1 VARCHAR(1000), uf2 VARCHAR(1000), uf3 VARCHAR(1000),
            leftovers VARCHAR(1000)
        );
    END

    -- 2. Use Session-Scoped Temp Table (Safer than persistent TempDataCollector)
    IF OBJECT_ID('tempdb..#TempDataCollector') IS NOT NULL 
        DROP TABLE #TempDataCollector;
    
    CREATE TABLE #TempDataCollector (Field1 VARCHAR(MAX));

    -- 3. Dynamic Bulk Insert
    DECLARE @SQL NVARCHAR(MAX);
    SET @SQL = N'BULK INSERT #TempDataCollector
                 FROM ''' + @ImportFileName + '''
                 WITH
                 (
                    FIRSTROW = 1,
                    FIELDTERMINATOR = ''~'',
                    ROWTERMINATOR = ''\n'',
                    TABLOCK
                 )';
    
    BEGIN TRY
        EXEC sp_executesql @SQL;
    END TRY
    BEGIN CATCH
        PRINT 'Error importing file. Check filename and permissions.';
        THROW; -- Rethrow error to alert caller
    END CATCH

    -- Trim whitespace
    UPDATE #TempDataCollector SET Field1 = RTRIM(Field1);

    -- 4. Process and Insert using the new TVF
    -- This single statement replaces all the loops and scalar calls
    INSERT INTO [dbo].[GenericDataImport] (
        FN, OriginalValue,
        col01varchar, col02varchar, col03varchar, col04varchar, col05varchar,
        col06varchar, col07varchar, col08varchar, col09varchar, col10varchar,
        col01other, col02other, col03other, col04other, col05other,
        col06other, col07other, col08other, col09other, col10other,
        col11other, col12other, col13other, col14other, col15other,
        col16other, col17other, col18other, col19other, col20other,
        uf1, uf2, uf3, leftovers
    )
    SELECT 
        @ImportFileName,
        T.Field1,
        -- The TVF columns align with the table columns
        P.c1, P.c2, P.c3, P.c4, P.c5, P.c6, P.c7, P.c8, P.c9, P.c10,
        P.o1, P.o2, P.o3, P.o4, P.o5, P.o6, P.o7, P.o8, P.o9, P.o10,
        P.o11, P.o12, P.o13, P.o14, P.o15, P.o16, P.o17, P.o18, P.o19, P.o20,
        '', '', '', -- Empty UF fields
        P.FinalLeftovers
    FROM 
        #TempDataCollector T
    CROSS APPLY 
        [dbo].[tvf_ParseGenericRow](T.Field1) P;

    -- Cleanup
    DROP TABLE #TempDataCollector;
END