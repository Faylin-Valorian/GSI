CREATE OR ALTER PROCEDURE [dbo].[sp_ModernBulkInsert]
    @DATABASE VARCHAR(1000),    -- e.g., 'MyDb.dbo.'
    @county VARCHAR(1000),      -- e.g., 'logan_1821'
    @provider VARCHAR(1000),    -- e.g., 'fromkellpro'
    @docPath VARCHAR(1000)      -- e.g., 'C:\FromKellpro' (No trailing slash)
AS
BEGIN
    SET NOCOUNT ON;

    -- 1. CLEANUP PATH & VARIABLE DECLARATIONS
    -- Ensure path has no trailing slash, but one is added for operations
    DECLARE @CleanDocPath VARCHAR(1000) = CASE 
        WHEN RIGHT(@docPath, 1) = '\' THEN LEFT(@docPath, LEN(@docPath) - 1) 
        ELSE @docPath 
    END;

    DECLARE @cmd VARCHAR(MAX);
    DECLARE @FullTableName NVARCHAR(MAX);
    DECLARE @FileName NVARCHAR(255);
    DECLARE @HeaderLine NVARCHAR(MAX);
    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @Params NVARCHAR(MAX);
    DECLARE @DropTableSQL NVARCHAR(MAX);

    -- Temp table to hold file list
    IF OBJECT_ID('tempdb..#FileList') IS NOT NULL DROP TABLE #FileList;
    CREATE TABLE #FileList (
        ID INT IDENTITY(1,1) PRIMARY KEY,
        FileName NVARCHAR(255)
    );

    -- 2. GET FILE LIST (Using xp_cmdshell)
    -- We filter for .csv files only
    SET @cmd = 'dir /b "' + @CleanDocPath + '\*.csv"';
    
    INSERT INTO #FileList (FileName)
    EXEC xp_cmdshell @cmd;

    -- Remove NULLs (output of xp_cmdshell sometimes) and non-csv noise
    DELETE FROM #FileList WHERE FileName IS NULL OR FileName NOT LIKE '%.csv';

    -- 3. LOOP THROUGH FILES
    DECLARE @i INT = 1;
    DECLARE @Count INT = (SELECT COUNT(*) FROM #FileList);
    
    WHILE @i <= @Count
    BEGIN
        SELECT @FileName = FileName FROM #FileList WHERE ID = @i;
        
        -- Clean Filename for Table Name (Remove extension)
        DECLARE @RawTableName NVARCHAR(255) = REPLACE(@FileName, '.csv', '');
        
        -- Construct standard Table Name: [Database].[Schema].[CountyProviderFileName]
        -- We use QUOTENAME to safely handle spaces or weird characters in the name.
        SET @FullTableName = @DATABASE + QUOTENAME(@county + @provider + @RawTableName);

        PRINT 'Processing File: ' + @FileName + ' -> Table: ' + @FullTableName;

        -------------------------------------------------------------------------
        -- STEP A: READ THE HEADER ROW TO DETERMINE COLUMNS
        -------------------------------------------------------------------------
        -- We use a dynamic query to read just the first line of the file into a variable.
        IF OBJECT_ID('tempdb..#RawHeader') IS NOT NULL DROP TABLE #RawHeader;
        CREATE TABLE #RawHeader (Line NVARCHAR(MAX));
        
        -- Note: using FORMAT='CSV' on just the header row might fail if header has no quotes but data does.
        -- We read the header as a raw string first.
        SET @SQL = N'BULK INSERT #RawHeader FROM ''' + @CleanDocPath + '\' + @FileName + ''' ' +
                   N'WITH (FIRSTROW = 1, LASTROW = 1, ROWTERMINATOR = ''0x0a'')';
        
        BEGIN TRY
            EXEC sp_executesql @SQL;
            SELECT TOP 1 @HeaderLine = Line FROM #RawHeader;
        END TRY
        BEGIN CATCH
            PRINT 'Error reading header for ' + @FileName + '; skipping.';
            GOTO NextFile;
        END CATCH

        -------------------------------------------------------------------------
        -- STEP B: PARSE HEADER & BUILD CREATE TABLE SCRIPT
        -------------------------------------------------------------------------
        -- We use OPENJSON to split by comma while PRESERVING ORDER. 
        -- STRING_SPLIT (pre-2022) does not guarantee order.
        -- Transform "Col1,Col2,Col3" into '["Col1","Col2","Col3"]' for JSON parsing.
        DECLARE @JsonHeader NVARCHAR(MAX) = '["' + REPLACE(@HeaderLine, ',', '","') + '"]';

        -- Build the Column List string (e.g., "[Col1] VARCHAR(MAX), [Col2] VARCHAR(MAX)...")
        DECLARE @ColsDefinition NVARCHAR(MAX) = '';
        
        SELECT @ColsDefinition = @ColsDefinition + 
               QUOTENAME(TRIM(REPLACE(value, '"', ''))) + ' VARCHAR(MAX), '
        FROM OPENJSON(@JsonHeader)
        ORDER BY CAST([key] AS INT); -- Important: Order by the JSON index to match CSV order

        -- Remove trailing comma
        IF LEN(@ColsDefinition) > 0
            SET @ColsDefinition = LEFT(@ColsDefinition, LEN(@ColsDefinition) - 1);
        ELSE
        BEGIN
            PRINT 'Empty header found in ' + @FileName + '; skipping.';
            GOTO NextFile;
        END

        -------------------------------------------------------------------------
        -- STEP C: DROP & CREATE TABLE
        -------------------------------------------------------------------------
        SET @SQL = N'DROP TABLE IF EXISTS ' + @FullTableName + '; ' +
                   N'CREATE TABLE ' + @FullTableName + ' (' + @ColsDefinition + ');';
        EXEC sp_executesql @SQL;

        -------------------------------------------------------------------------
        -- STEP D: BULK INSERT DATA (MODERN METHOD)
        -------------------------------------------------------------------------
        -- FORMAT = 'CSV' handles quoted data containing commas/newlines automatically.
        -- FIELDQUOTE = '"' specifies the quote character.
        SET @SQL = N'BULK INSERT ' + @FullTableName + ' FROM ''' + @CleanDocPath + '\' + @FileName + ''' ' +
                   N'WITH (
                       FORMAT = ''CSV'', 
                       FIRSTROW = 2, 
                       FIELDQUOTE = ''"'', 
                       FIELDTERMINATOR = '','', 
                       ROWTERMINATOR = ''0x0a''
                   );';
        
        BEGIN TRY
            EXEC sp_executesql @SQL;
        END TRY
        BEGIN CATCH
            PRINT 'Error Bulk Inserting data for ' + @FileName + '. Check file locks or format.';
            PRINT ERROR_MESSAGE();
            GOTO NextFile;
        END CATCH

        -------------------------------------------------------------------------
        -- STEP E: RESIZE COLUMNS (OPTIMIZED)
        -------------------------------------------------------------------------
        -- Instead of looping ALTER TABLE 50 times, we generate the script and run it.
        -- Logic: Calculate MAX(LEN(Col)) + 100 for every column.
        
        DECLARE @AlterScript NVARCHAR(MAX) = '';
        DECLARE @ColName NVARCHAR(255);
        DECLARE @MaxLen INT;
        
        -- Cursor to calculate lengths (We need to query the data we just inserted)
        -- We generate a dynamic SQL to get the MAX LEN for each column
        DECLARE ColCursor CURSOR FOR 
            SELECT name FROM sys.columns WHERE object_id = OBJECT_ID(@FullTableName);
        
        OPEN ColCursor;
        FETCH NEXT FROM ColCursor INTO @ColName;
        
        WHILE @@FETCH_STATUS = 0
        BEGIN
            DECLARE @LenSQL NVARCHAR(MAX);
            DECLARE @CalcLen INT;
            
            -- Dynamic query to get max length
            SET @LenSQL = N'SELECT @MaxOut = ISNULL(MAX(LEN(' + QUOTENAME(@ColName) + ')), 0) FROM ' + @FullTableName;
            EXEC sp_executesql @LenSQL, N'@MaxOut INT OUTPUT', @MaxOut = @CalcLen OUTPUT;
            
            -- Add buffer of 100
            SET @CalcLen = @CalcLen + 100;
            -- Cap at 8000 (standard VARCHAR max) or stick to MAX if huge
            IF @CalcLen > 8000 SET @CalcLen = 8000; 

            -- Build ALTER statement
            -- Note: If data is truly huge (>8000 chars), we leave it as VARCHAR(MAX)
            -- Current logic: If it fits in 8000, we resize. If not, we leave it as MAX.
            IF @CalcLen <= 8000
            BEGIN
                SET @AlterScript = @AlterScript + 
                    'ALTER TABLE ' + @FullTableName + ' ALTER COLUMN ' + QUOTENAME(@ColName) + ' VARCHAR(' + CAST(@CalcLen AS VARCHAR) + '); ' + CHAR(13);
            END

            FETCH NEXT FROM ColCursor INTO @ColName;
        END
        
        CLOSE ColCursor;
        DEALLOCATE ColCursor;

        -- Execute all ALTER statements in one go (or cleaner batch)
        IF LEN(@AlterScript) > 0
        BEGIN
            EXEC sp_executesql @AlterScript;
        END

        PRINT 'Successfully Processed: ' + @FileName;

        NextFile:
        SET @i = @i + 1;
    END

    -- Cleanup
    DROP TABLE #FileList;
    IF OBJECT_ID('tempdb..#RawHeader') IS NOT NULL DROP TABLE #RawHeader;
END