CREATE OR ALTER FUNCTION [dbo].[tvf_ParseGenericRow](@RowData VARCHAR(MAX))
RETURNS TABLE AS
RETURN
(
    -- STEP 1: The "Waterfall" - Peel off the 10 Quoted Columns one by one
    -- This mimics the logic of fn_FVBDQ but performs it inline for speed.
    WITH 
    L1 AS (
        SELECT 
            -- Find positions of the first pair of quotes
            CHARINDEX('"', @RowData) AS P1, 
            CHARINDEX('"', @RowData, CHARINDEX('"', @RowData) + 1) AS P2
    ),
    Step1 AS (
        SELECT 
            -- Extract Value between quotes
            LTRIM(RTRIM(SUBSTRING(@RowData, P1 + 1, P2 - P1 - 1))) AS C1,
            -- Store the Remainder ("Leftovers")
            STUFF(@RowData, 1, P2, '') AS Rem1
        FROM L1
    ),
    Step2 AS (
        SELECT C1, Rem1, 
               P1 = CHARINDEX('"', Rem1), P2 = CHARINDEX('"', Rem1, CHARINDEX('"', Rem1) + 1)
        FROM Step1
    ),
    Step3 AS (
        SELECT C1, 
               C2 = LTRIM(RTRIM(SUBSTRING(Rem1, P1 + 1, P2 - P1 - 1))), 
               Rem2 = STUFF(Rem1, 1, P2, '') 
        FROM Step2
    ),
    Step4 AS (
        SELECT C1, C2, Rem2, 
               P1 = CHARINDEX('"', Rem2), P2 = CHARINDEX('"', Rem2, CHARINDEX('"', Rem2) + 1)
        FROM Step3
    ),
    Step5 AS (
        SELECT C1, C2, 
               C3 = LTRIM(RTRIM(SUBSTRING(Rem2, P1 + 1, P2 - P1 - 1))), 
               Rem3 = STUFF(Rem2, 1, P2, '') 
        FROM Step4
    ),
    Step6 AS (
        SELECT C1, C2, C3, Rem3, 
               P1 = CHARINDEX('"', Rem3), P2 = CHARINDEX('"', Rem3, CHARINDEX('"', Rem3) + 1)
        FROM Step5
    ),
    Step7 AS (
        SELECT C1, C2, C3, 
               C4 = LTRIM(RTRIM(SUBSTRING(Rem3, P1 + 1, P2 - P1 - 1))), 
               Rem4 = STUFF(Rem3, 1, P2, '') 
        FROM Step6
    ),
    Step8 AS (
        SELECT C1, C2, C3, C4, Rem4, 
               P1 = CHARINDEX('"', Rem4), P2 = CHARINDEX('"', Rem4, CHARINDEX('"', Rem4) + 1)
        FROM Step7
    ),
    Step9 AS (
        SELECT C1, C2, C3, C4, 
               C5 = LTRIM(RTRIM(SUBSTRING(Rem4, P1 + 1, P2 - P1 - 1))), 
               Rem5 = STUFF(Rem4, 1, P2, '') 
        FROM Step8
    ),
    -- Continue waterfall for columns 6-10 (Simplified for readability, expanding logic)
    -- To keep script concise and robust, we apply the logic repeatedly. 
    -- For production clarity, we jump to the final state logic below.
    -- (Note: In a full production script, you would continue the Step CTEs to Step 19. 
    --  Here, we will use a recursive Trick or just chained Cross Applies to ensure exactness.)
    
    -- RE-IMPLEMENTATION WITH CHAINED APPLIES (Cleaner than named CTEs for deep nesting)
    CalculatedData AS (
        SELECT 
            -- Extract 1
            v1.Val as c1, r1.Rem as rem1,
            -- Extract 2
            v2.Val as c2, r2.Rem as rem2,
            v3.Val as c3, r3.Rem as rem3,
            v4.Val as c4, r4.Rem as rem4,
            v5.Val as c5, r5.Rem as rem5,
            v6.Val as c6, r6.Rem as rem6,
            v7.Val as c7, r7.Rem as rem7,
            v8.Val as c8, r8.Rem as rem8,
            v9.Val as c9, r9.Rem as rem9,
            v10.Val as c10, r10.Rem as FinalLeftovers
        FROM (SELECT @RowData as Raw) Base
        -- 1
        CROSS APPLY (SELECT CHARINDEX('"', Raw) as pA, CHARINDEX('"', Raw, CHARINDEX('"', Raw)+1) as pB) p1
        CROSS APPLY (SELECT LTRIM(RTRIM(SUBSTRING(Raw, p1.pA+1, p1.pB-p1.pA-1))) as Val) v1
        CROSS APPLY (SELECT STUFF(Raw, 1, p1.pB, '') as Rem) r1
        -- 2
        CROSS APPLY (SELECT CHARINDEX('"', r1.Rem) as pA, CHARINDEX('"', r1.Rem, CHARINDEX('"', r1.Rem)+1) as pB) p2
        CROSS APPLY (SELECT LTRIM(RTRIM(SUBSTRING(r1.Rem, p2.pA+1, p2.pB-p2.pA-1))) as Val) v2
        CROSS APPLY (SELECT STUFF(r1.Rem, 1, p2.pB, '') as Rem) r2
        -- 3
        CROSS APPLY (SELECT CHARINDEX('"', r2.Rem) as pA, CHARINDEX('"', r2.Rem, CHARINDEX('"', r2.Rem)+1) as pB) p3
        CROSS APPLY (SELECT LTRIM(RTRIM(SUBSTRING(r2.Rem, p3.pA+1, p3.pB-p3.pA-1))) as Val) v3
        CROSS APPLY (SELECT STUFF(r2.Rem, 1, p3.pB, '') as Rem) r3
        -- 4
        CROSS APPLY (SELECT CHARINDEX('"', r3.Rem) as pA, CHARINDEX('"', r3.Rem, CHARINDEX('"', r3.Rem)+1) as pB) p4
        CROSS APPLY (SELECT LTRIM(RTRIM(SUBSTRING(r3.Rem, p4.pA+1, p4.pB-p4.pA-1))) as Val) v4
        CROSS APPLY (SELECT STUFF(r3.Rem, 1, p4.pB, '') as Rem) r4
        -- 5
        CROSS APPLY (SELECT CHARINDEX('"', r4.Rem) as pA, CHARINDEX('"', r4.Rem, CHARINDEX('"', r4.Rem)+1) as pB) p5
        CROSS APPLY (SELECT LTRIM(RTRIM(SUBSTRING(r4.Rem, p5.pA+1, p5.pB-p5.pA-1))) as Val) v5
        CROSS APPLY (SELECT STUFF(r4.Rem, 1, p5.pB, '') as Rem) r5
        -- 6
        CROSS APPLY (SELECT CHARINDEX('"', r5.Rem) as pA, CHARINDEX('"', r5.Rem, CHARINDEX('"', r5.Rem)+1) as pB) p6
        CROSS APPLY (SELECT LTRIM(RTRIM(SUBSTRING(r5.Rem, p6.pA+1, p6.pB-p6.pA-1))) as Val) v6
        CROSS APPLY (SELECT STUFF(r5.Rem, 1, p6.pB, '') as Rem) r6
        -- 7
        CROSS APPLY (SELECT CHARINDEX('"', r6.Rem) as pA, CHARINDEX('"', r6.Rem, CHARINDEX('"', r6.Rem)+1) as pB) p7
        CROSS APPLY (SELECT LTRIM(RTRIM(SUBSTRING(r6.Rem, p7.pA+1, p7.pB-p7.pA-1))) as Val) v7
        CROSS APPLY (SELECT STUFF(r6.Rem, 1, p7.pB, '') as Rem) r7
        -- 8
        CROSS APPLY (SELECT CHARINDEX('"', r7.Rem) as pA, CHARINDEX('"', r7.Rem, CHARINDEX('"', r7.Rem)+1) as pB) p8
        CROSS APPLY (SELECT LTRIM(RTRIM(SUBSTRING(r7.Rem, p8.pA+1, p8.pB-p8.pA-1))) as Val) v8
        CROSS APPLY (SELECT STUFF(r7.Rem, 1, p8.pB, '') as Rem) r8
        -- 9
        CROSS APPLY (SELECT CHARINDEX('"', r8.Rem) as pA, CHARINDEX('"', r8.Rem, CHARINDEX('"', r8.Rem)+1) as pB) p9
        CROSS APPLY (SELECT LTRIM(RTRIM(SUBSTRING(r8.Rem, p9.pA+1, p9.pB-p9.pA-1))) as Val) v9
        CROSS APPLY (SELECT STUFF(r8.Rem, 1, p9.pB, '') as Rem) r9
        -- 10
        CROSS APPLY (SELECT CHARINDEX('"', r9.Rem) as pA, CHARINDEX('"', r9.Rem, CHARINDEX('"', r9.Rem)+1) as pB) p10
        CROSS APPLY (SELECT LTRIM(RTRIM(SUBSTRING(r9.Rem, p10.pA+1, p10.pB-p10.pA-1))) as Val) v10
        CROSS APPLY (SELECT STUFF(r9.Rem, 1, p10.pB, '') as Rem) r10
    ),
    -- STEP 2: The XML Splitter - Split the "FinalLeftovers" by comma
    -- This replaces the "parse" function loop.
    Splitter AS (
        SELECT 
            c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, FinalLeftovers,
            -- Convert the comma-separated leftovers into XML to split efficiently
            CAST('<x>' + REPLACE(FinalLeftovers, ',', '</x><x>') + '</x>' AS XML) AS X
        FROM CalculatedData
    )
    SELECT
        c1, c2, c3, c4, c5, c6, c7, c8, c9, c10,
        -- Extract the 20 'other' columns from the XML
        LTRIM(RTRIM(X.value('(/x)[1]', 'VARCHAR(1000)'))) as o1,
        LTRIM(RTRIM(X.value('(/x)[2]', 'VARCHAR(1000)'))) as o2,
        LTRIM(RTRIM(X.value('(/x)[3]', 'VARCHAR(1000)'))) as o3,
        LTRIM(RTRIM(X.value('(/x)[4]', 'VARCHAR(1000)'))) as o4,
        LTRIM(RTRIM(X.value('(/x)[5]', 'VARCHAR(1000)'))) as o5,
        LTRIM(RTRIM(X.value('(/x)[6]', 'VARCHAR(1000)'))) as o6,
        LTRIM(RTRIM(X.value('(/x)[7]', 'VARCHAR(1000)'))) as o7,
        LTRIM(RTRIM(X.value('(/x)[8]', 'VARCHAR(1000)'))) as o8,
        LTRIM(RTRIM(X.value('(/x)[9]', 'VARCHAR(1000)'))) as o9,
        LTRIM(RTRIM(X.value('(/x)[10]', 'VARCHAR(1000)'))) as o10,
        LTRIM(RTRIM(X.value('(/x)[11]', 'VARCHAR(1000)'))) as o11,
        LTRIM(RTRIM(X.value('(/x)[12]', 'VARCHAR(1000)'))) as o12,
        LTRIM(RTRIM(X.value('(/x)[13]', 'VARCHAR(1000)'))) as o13,
        LTRIM(RTRIM(X.value('(/x)[14]', 'VARCHAR(1000)'))) as o14,
        LTRIM(RTRIM(X.value('(/x)[15]', 'VARCHAR(1000)'))) as o15,
        LTRIM(RTRIM(X.value('(/x)[16]', 'VARCHAR(1000)'))) as o16,
        LTRIM(RTRIM(X.value('(/x)[17]', 'VARCHAR(1000)'))) as o17,
        LTRIM(RTRIM(X.value('(/x)[18]', 'VARCHAR(1000)'))) as o18,
        LTRIM(RTRIM(X.value('(/x)[19]', 'VARCHAR(1000)'))) as o19,
        LTRIM(RTRIM(X.value('(/x)[20]', 'VARCHAR(1000)'))) as o20,
        FinalLeftovers
    FROM Splitter
)