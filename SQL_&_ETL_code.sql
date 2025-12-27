-- Create database and schema

-- 1. Create DB and schema
IF DB_ID('BIS_Spend_DB_Copy') IS NULL
BEGIN
    CREATE DATABASE BIS_Spend_DB_Copy;
END
GO

USE BIS_Spend_DB;
GO

--  dedicated schema
CREATE SCHEMA warehousing;
GO

-- Create staging table (adapting columns to match the CSV)


-- Adjust column names/lengths to match CSV header exactly
DROP TABLE IF EXISTS warehousing.Staging_Invoices;
GO

DROP TABLE IF EXISTS warehousing.Staging_Invoices;
GO

CREATE TABLE warehousing.Staging_Invoices (
    [Department]          NVARCHAR(200),
    [Entity]              NVARCHAR(200),
    [Date_of_Payment]     NVARCHAR(50),
    [Expense_Type]        NVARCHAR(200),
    [Expense_Area]        NVARCHAR(300),
    [Supplier]            NVARCHAR(400),
    [Transaction_Number]  NVARCHAR(100),
    [Amount]              NVARCHAR(50),
    [Description]         NVARCHAR(MAX),
    [Supplier_Post_Code]  NVARCHAR(50),
    [Supplier_Type]       NVARCHAR(50),
    [Contract_Number]     NVARCHAR(100),
    [Project_Code]        NVARCHAR(100),
    [Expenditure_Type]    NVARCHAR(200),
    SourceFile            NVARCHAR(100)
);
GO

-- Insert all four files into this staging table
INSERT INTO warehousing.Staging_Invoices (
    [Department],
    [Entity],
    [Date_of_Payment],
    [Expense_Type],
    [Expense_Area],
    [Supplier],
    [Transaction_Number],
    [Amount],
    [Description],
    [Supplier_Post_Code],
    [Supplier_Type],
    [Contract_Number],
    [Project_Code],
    [Expenditure_Type]
)
SELECT 
    [Department],
    [Entity],
    [Date_of_Payment],
    [Expense_Type],
    [Expense_Area],
    [Supplier],
    [Transaction_Number],
    [Amount],
    [Description],
    [Supplier_Post_Code],
    [Supplier_Type],
    [Contract_Number],
    [Project_Code],
    [Expenditure_Type]
FROM BIS_July_2015;

INSERT INTO warehousing.Staging_Invoices (
    [Department],
    [Entity],
    [Date_of_Payment],
    [Expense_Type],
    [Expense_Area],
    [Supplier],
    [Transaction_Number],
    [Amount],
    [Description],
    [Supplier_Post_Code],
    [Supplier_Type],
    [Contract_Number],
    [Project_Code],
    [Expenditure_Type]
)
SELECT 
    [Department],
    [Entity],
    [Date_of_Payment],
    [Expense_Type],
    [Expense_Area],
    [Supplier],
    [Transaction_Number],
    [Amount],
    [Description],
    [Supplier_Post_Code],
    [Supplier_Type],
    [Contract_Number],
    NULL AS [Project_Code],       -- Missing in August csv
    NULL AS [Expenditure_Type]    -- Missing in August csv
FROM BIS_August_2015;



INSERT INTO warehousing.Staging_Invoices (
    [Department],[Entity],[Date_of_Payment],[Expense_Type],[Expense_Area],
    [Supplier],[Transaction_Number],[Amount],[Description],[Supplier_Post_Code],
    [Supplier_Type],[Contract_Number],[Project_Code],[Expenditure_Type]
)
SELECT 
    [Department],[Entity],[Date_of_Payment],[Expense_Type],[Expense_Area],
    [Supplier],[Transaction_Number],[Amount],[Description],[Supplier_Post_Code],
    [Supplier_Type],[Contract_Number],[Project_Code],[Expenditure_Type]
FROM BIS_september_2015;

INSERT INTO warehousing.Staging_Invoices (
    [Department],[Entity],[Date_of_Payment],[Expense_Type],[Expense_Area],
    [Supplier],[Transaction_Number],[Amount],[Description],[Supplier_Post_Code],
    [Supplier_Type],[Contract_Number],[Project_Code],[Expenditure_Type]
)
SELECT 
    [Department],[Entity],[Date_of_Payment],[Expense_Type],[Expense_Area],
    [Supplier],[Transaction_Number],[Amount],[Description],[Supplier_Post_Code],
    [Supplier_Type],[Contract_Number],[Project_Code],[Expenditure_Type]
FROM BIS_october_2015;


-- Step 2 ETL Cleaning (Date parsing + Amount conversion)

--  STEP 2.1: Add Cleaned Columns to Staging Table
ALTER TABLE warehousing.Staging_Invoices
ADD 
    Clean_Date DATE NULL,
    Clean_Amount DECIMAL(18,2) NULL,
    Clean_Supplier NVARCHAR(400) NULL;
GO

SELECT *
FROM warehousing.Staging_Invoices
WHERE Clean_Date IS NULL;

-- Delete all rows where all major fields are NULL
DELETE 
FROM warehousing.Staging_Invoices
WHERE Clean_Date IS NULL
  AND Clean_Amount IS NULL
  AND Clean_Supplier IS NULL
  AND Department IS NULL
  AND Entity IS NULL
  AND Expense_Type IS NULL
  AND Supplier IS NULL
  AND Amount IS NULL;

  SELECT COUNT(*) AS NullEntireRowCount
FROM warehousing.Staging_Invoices
WHERE 
    Department IS NULL AND Entity IS NULL AND Date_of_Payment IS NULL AND
    Expense_Type IS NULL AND Expense_Area IS NULL AND Supplier IS NULL AND
    Transaction_Number IS NULL AND Amount IS NULL AND Description IS NULL;


--  STEP 2.2: Clean & Convert DATE Column
UPDATE warehousing.Staging_Invoices
SET Clean_Date = TRY_CONVERT(DATE, [Date_of_Payment], 105);
GO
SELECT [Date_of_Payment] 
FROM warehousing.Staging_Invoices
WHERE Clean_Date IS NULL;

-- STEP 2.3: Clean & Convert AMOUNT Column
UPDATE warehousing.Staging_Invoices
SET Clean_Amount =
    TRY_CONVERT(DECIMAL(18,2),
        REPLACE(REPLACE(REPLACE([Amount], '£', ''), ',', ''), '"', '')
    );
GO
SELECT [Amount]
FROM warehousing.Staging_Invoices
WHERE Clean_Amount IS NULL;

-- STEP 2.4: Replace Withheld Supplier Names with Your Student Number
UPDATE warehousing.Staging_Invoices
SET Clean_Supplier = '24005856'
WHERE LTRIM(RTRIM([Supplier])) = 'Personal Expense, Name Withheld';

-- Copy remaining supplier names normally
UPDATE warehousing.Staging_Invoices
SET Clean_Supplier = [Supplier]
WHERE Clean_Supplier IS NULL;
GO

SELECT *
FROM warehousing.Staging_Invoices;

-- STEP 2.5: Remove Invalid Suppliers Using XML File

-- Load XML into SQL:

SET NOCOUNT ON;
BEGIN TRY
    -- Declare variable for raw XML text (use N'' for unicode)
    DECLARE @Raw NVARCHAR(MAX);

    -- IMPORTANT: If your XML contains single quotes (apostrophes), this block will convert them to &apos; below.
    SET @Raw = N'
<Suppliers>
    <Supplier SupplierName="ACCO UK LTD" Entity="Department for Business, Innovation and Skills" ExpenseArea="Finance, Commercial & Digital Transformation - Estates"/>
    <Supplier SupplierName="ADAM PHONES LTD" Entity="Department for Business, Innovation and Skills" ExpenseArea="Finance, Commercial & Digital Transformation - Digital Directorate"/>
    <Supplier SupplierName="ALCOVE TECHNOLOGIES LTD" Entity="UK Shared Business Services Ltd" ExpenseArea="Core Services"/>
    <Supplier SupplierName="APEX OFFICE INTERIORS LTD" Entity="Department for Business, Innovation and Skills" ExpenseArea="Finance, Commercial & Digital Transformation - Estates"/>
    <Supplier SupplierName="APOGEE CORPORATION LTD" Entity="Department for Business, Innovation and Skills" ExpenseArea="Finance, Commercial & Digital Transformation - Estates"/>
    <Supplier SupplierName="ARCSERVE UK LTD" Entity="UK Shared Business Services Ltd" ExpenseArea="Technology Services"/>
    <Supplier SupplierName="BIG YELLOW SELF STORAGE" Entity="UK Shared Business Services Ltd" ExpenseArea="Core Services"/>
    <Supplier SupplierName="BOSS PRINT LTD" Entity="Department for Business, Innovation and Skills" ExpenseArea="Finance, Commercial & Digital Transformation - Digital Directorate"/>
    <Supplier SupplierName="CHASE TEMPLETON LTD" Entity="Department for Business, Innovation and Skills" ExpenseArea="People, Strategy & Performance - Human Resources"/>
    <Supplier SupplierName="COPPICE LEA TRAINING" Entity="Department for Business, Innovation and Skills" ExpenseArea="People, Strategy & Performance - Learning & Development"/>
    <Supplier SupplierName="CROMWELL BUSINESS SYSTEMS" Entity="Department for Business, Innovation and Skills" ExpenseArea="Enterprise & Skills - Enterprise and Business Growth"/>
    <Supplier SupplierName="DATA EXPERTS LTD" Entity="UK Shared Business Services Ltd" ExpenseArea="Core Services"/>
    <Supplier SupplierName="DECISIONING FACTORY LTD" Entity="Department for Business, Innovation and Skills" ExpenseArea="Finance, Commercial & Digital Transformation - Digital Directorate"/>
    <Supplier SupplierName="DIGITAL EVIDENCE SOLUTIONS LTD" Entity="UK Shared Business Services Ltd" ExpenseArea="Core Services"/>
    <Supplier SupplierName="EDGAR S" Entity="Department for Business, Innovation and Skills" ExpenseArea="People, Strategy & Performance - Learning & Development"/>
    <Supplier SupplierName="ELITE OFFICE FURNITURE" Entity="Department for Business, Innovation and Skills" ExpenseArea="Finance, Commercial & Digital Transformation - Estates"/>
    <Supplier SupplierName="ESPO" Entity="Department for Business, Innovation and Skills" ExpenseArea="Finance, Commercial & Digital Transformation - Estates"/>
    <Supplier SupplierName="EUROPEAN INFORMATION SERVICE" Entity="Department for Business, Innovation and Skills" ExpenseArea="Business & Science - Business Environment"/>
    <Supplier SupplierName="EXCELLENCE IN CAREER DEVELOPMENT" Entity="Department for Business, Innovation and Skills" ExpenseArea="People, Strategy & Performance - Learning & Development"/>
    <Supplier SupplierName="FASTER RETURN" Entity="Department for Business, Innovation and Skills" ExpenseArea="People, Strategy & Performance - Learning & Development"/>
    <Supplier SupplierName="FIREBOX.COM LTD" Entity="UK Shared Business Services Ltd" ExpenseArea="Core Services"/>
    <Supplier SupplierName="FIRST RESPONSE FACILITIES LTD" Entity="Department for Business, Innovation and Skills" ExpenseArea="Finance, Commercial & Digital Transformation - Estates"/>
    <Supplier SupplierName="GAP PERSONNEL GROUP LTD" Entity="UK Shared Business Services Ltd" ExpenseArea="Core Services"/>
    <Supplier SupplierName="GRIDLINE CONSULTING LTD" Entity="Department for Business, Innovation and Skills" ExpenseArea="Enterprise & Skills - Enterprise and Business Growth"/>
    <Supplier SupplierName="HILSON MORAN PARTNERSHIP" Entity="Department for Business, Innovation and Skills" ExpenseArea="Finance, Commercial & Digital Transformation - Estates"/>
    <Supplier SupplierName="INK EXPRESSIONS" Entity="Department for Business, Innovation and Skills" ExpenseArea="Finance, Commercial & Digital Transformation - Digital Directorate"/>
    <Supplier SupplierName="INSIGHT DIRECT UK LTD" Entity="UK Shared Business Services Ltd" ExpenseArea="Technology Services"/>
    <Supplier SupplierName="JSA SERVICES LTD" Entity="UK Shared Business Services Ltd" ExpenseArea="Core Services"/>
    <Supplier SupplierName="JUPITER GROUP LTD" Entity="Department for Business, Innovation and Skills" ExpenseArea="People, Strategy & Performance - Learning & Development"/>
    <Supplier SupplierName="KEY TRAINING LTD" Entity="Department for Business, Innovation and Skills" ExpenseArea="People, Strategy & Performance - Learning & Development"/>
    <Supplier SupplierName="KINGSBRIDGE RISK SOLUTIONS LTD" Entity="UK Shared Business Services Ltd" ExpenseArea="Core Services"/>
    <Supplier SupplierName="LIFETIME TRAINING GROUP LTD" Entity="Department for Business, Innovation and Skills" ExpenseArea="People, Strategy & Performance - Learning & Development"/>
    <Supplier SupplierName="MARTIN CURRIE" Entity="Department for Business, Innovation and Skills" ExpenseArea="Enterprise & Skills - Enterprise and Business Growth"/>
    <Supplier SupplierName="MERSEYSIDE FIRE & RESCUE SERVICE" Entity="UK Shared Business Services Ltd" ExpenseArea="Core Services"/>
    <Supplier SupplierName="METROPOLITAN POLICE SERVICE" Entity="UK Shared Business Services Ltd" ExpenseArea="Core Services"/>
    <Supplier SupplierName="NEW FUTURES NETWORK" Entity="Department for Business, Innovation and Skills" ExpenseArea="Enterprise & Skills - Enterprise and Business Growth"/>
    <Supplier SupplierName="NQA LTD" Entity="Department for Business, Innovation and Skills" ExpenseArea="Enterprise & Skills - Enterprise and Business Growth"/>
    <Supplier SupplierName="ORACLE CORPORATION UK LTD" Entity="UK Shared Business Services Ltd" ExpenseArea="Technology Services"/>
    <Supplier SupplierName="PCM LTD" Entity="UK Shared Business Services Ltd" ExpenseArea="Technology Services"/>
    <Supplier SupplierName="PEARL WINDOW SYSTEMS" Entity="Department for Business, Innovation and Skills" ExpenseArea="Finance, Commercial & Digital Transformation - Estates"/>
    <Supplier SupplierName="PHOENIX DIRECT SERVICES" Entity="Department for Business, Innovation and Skills" ExpenseArea="Finance, Commercial & Digital Transformation - Estates"/>
    <Supplier SupplierName="PRO-LINE TRAINING LTD" Entity="Department for Business, Innovation and Skills" ExpenseArea="People, Strategy & Performance - Learning & Development"/>
    <Supplier SupplierName="REED LEARNING LTD" Entity="Department for Business, Innovation and Skills" ExpenseArea="People, Strategy & Performance - Learning & Development"/>
    <Supplier SupplierName="RESTORE PLC" Entity="UK Shared Business Services Ltd" ExpenseArea="Core Services"/>
    <Supplier SupplierName="RICOH UK LTD" Entity="UK Shared Business Services Ltd" ExpenseArea="Technology Services"/>
    <Supplier SupplierName="SKILLS TEAM LTD" Entity="Department for Business, Innovation and Skills" ExpenseArea="People, Strategy & Performance - Learning & Development"/>
    <Supplier SupplierName="THE BRILLIANT WAY" Entity="Department for Business, Innovation and Skills" ExpenseArea="People, Strategy & Performance - Learning & Development"/>
    <Supplier SupplierName="TOSHIBA TEC UK IMAGING" Entity="Department for Business, Innovation and Skills" ExpenseArea="Finance, Commercial & Digital Transformation - Estates"/>
    <Supplier SupplierName="TOTAL MANUFACTURING SERVICES LTD" Entity="Department for Business, Innovation and Skills" ExpenseArea="Enterprise & Skills - Enterprise and Business Growth"/>
    <Supplier SupplierName="TROPOCUS LTD" Entity="Department for Business, Innovation and Skills" ExpenseArea="Finance, Commercial & Digital Transformation - Digital Directorate"/>
    <Supplier SupplierName="UNISERVE HOLDINGS LTD" Entity="UK Shared Business Services Ltd" ExpenseArea="Core Services"/>
    <Supplier SupplierName="WORKPLACE SOLUTIONS LTD" Entity="Department for Business, Innovation and Skills" ExpenseArea="Finance, Commercial & Digital Transformation - Estates"/>
    <Supplier SupplierName="ZUHLKE ENGINEERING LTD" Entity="Department for Business, Innovation and Skills" ExpenseArea="Business & Science - Business Innovation"/>
</Suppliers>';

    -- Replace characters that break XML parsing
    SET @Raw = REPLACE(@Raw, '&', '&amp;');       -- ampersand first
    SET @Raw = REPLACE(@Raw, NCHAR(8211), '-');  -- en dash – (unicode)
    SET @Raw = REPLACE(@Raw, NCHAR(8212), '-');  -- em dash —
    SET @Raw = REPLACE(@Raw, NCHAR(8220), '"');  -- left double quote “
    SET @Raw = REPLACE(@Raw, NCHAR(8221), '"');  -- right double quote ”
    -- Replace single apostrophe ' with XML entity to avoid breaking SQL literal if any present inside the XML
    SET @Raw = REPLACE(@Raw, '''', '&apos;');

    -- Convert to XML type
    DECLARE @XML XML;
    SET @XML = TRY_CAST(@Raw AS XML);

    IF @XML IS NULL
    BEGIN
        RAISERROR('Converted XML is NULL or malformed. Inspect @Raw content for invalid characters.', 16, 1);
    END
    ELSE
    BEGIN
        PRINT 'XML loaded successfully. Now extracting supplier list...';

        -- Create a temp table to hold invalid supplier names
        IF OBJECT_ID('tempdb..#InvalidSuppliers') IS NOT NULL DROP TABLE #InvalidSuppliers;
        CREATE TABLE #InvalidSuppliers (SupplierName NVARCHAR(400));

        INSERT INTO #InvalidSuppliers (SupplierName)
        SELECT T.C.value('@SupplierName','NVARCHAR(400)') FROM @XML.nodes('/Suppliers/Supplier') AS T(C);

        SELECT COUNT(*) AS InvalidCount FROM #InvalidSuppliers;
        SELECT TOP (50) * FROM #InvalidSuppliers;  -- show sample

        -- Now delete matching rows from staging (use LTRIM/RTRIM to avoid whitespace issues)
        DELETE s
        FROM warehousing.Staging_Invoices s
        JOIN #InvalidSuppliers i
          ON LTRIM(RTRIM(s.[Supplier])) = LTRIM(RTRIM(i.SupplierName));

        PRINT 'Delete complete. Rows removed from staging (if any).';
        SELECT @@ROWCOUNT AS RowsDeleted;
    END
END TRY
BEGIN CATCH
    SELECT 
        ERROR_NUMBER() AS ErrNo,
        ERROR_SEVERITY() AS Severity,
        ERROR_STATE() AS ErrState,
        ERROR_PROCEDURE() AS ErrProc,
        ERROR_LINE() AS ErrLine,
        ERROR_MESSAGE() AS ErrMessage;
END CATCH;

-- Putting all invalid suppliers into a temp table
IF OBJECT_ID('tempdb..#InvalidSuppliers') IS NOT NULL DROP TABLE #InvalidSuppliers;

CREATE TABLE #InvalidSuppliers (SupplierName NVARCHAR(400));

INSERT INTO #InvalidSuppliers (SupplierName)
VALUES
('ACCO UK LTD'),
('ADAM PHONES LTD'),
('ALCOVE TECHNOLOGIES LTD'),
('APEX OFFICE INTERIORS LTD'),
('APOGEE CORPORATION LTD'),
('ARCSERVE UK LTD'),
('BIG YELLOW SELF STORAGE'),
('BOSS PRINT LTD'),
('CHASE TEMPLETON LTD'),
('COPPICE LEA TRAINING'),
('CROMWELL BUSINESS SYSTEMS'),
('DATA EXPERTS LTD'),
('DECISIONING FACTORY LTD'),
('DIGITAL EVIDENCE SOLUTIONS LTD'),
('EDGAR S'),
('ELITE OFFICE FURNITURE'),
('ESPO'),
('EUROPEAN INFORMATION SERVICE'),
('EXCELLENCE IN CAREER DEVELOPMENT'),
('FASTER RETURN'),
('FIREBOX.COM LTD'),
('FIRST RESPONSE FACILITIES LTD'),
('GAP PERSONNEL GROUP LTD'),
('GRIDLINE CONSULTING LTD'),
('HILSON MORAN PARTNERSHIP'),
('INK EXPRESSIONS'),
('INSIGHT DIRECT UK LTD'),
('JSA SERVICES LTD'),
('JUPITER GROUP LTD'),
('KEY TRAINING LTD'),
('KINGSBRIDGE RISK SOLUTIONS LTD'),
('LIFETIME TRAINING GROUP LTD'),
('MARTIN CURRIE'),
('MERSEYSIDE FIRE & RESCUE SERVICE'),
('METROPOLITAN POLICE SERVICE'),
('NEW FUTURES NETWORK'),
('NQA LTD'),
('ORACLE CORPORATION UK LTD'),
('PCM LTD'),
('PEARL WINDOW SYSTEMS'),
('PHOENIX DIRECT SERVICES'),
('PRO-LINE TRAINING LTD'),
('REED LEARNING LTD'),
('RESTORE PLC'),
('RICOH UK LTD'),
('SKILLS TEAM LTD'),
('THE BRILLIANT WAY'),
('TOSHIBA TEC UK IMAGING'),
('TOTAL MANUFACTURING SERVICES LTD'),
('TROPOCUS LTD'),
('ZUHLKE ENGINEERING LTD');   -- Missing one added

EXEC sp_help 'warehousing.Staging_Invoices';
SELECT DISTINCT Supplier
FROM warehousing.Staging_Invoices
WHERE Supplier IN (
    'ACCO UK LTD',
    'ADAM PHONES LTD',
    'APEX OFFICE INTERIORS LTD'
);

-- Delete them from Staging Table
DELETE S
FROM warehousing.Staging_Invoices S
JOIN #InvalidSuppliers I
    ON REPLACE(REPLACE(REPLACE(LOWER(S.Supplier), ' ', ''), CHAR(9), ''), CHAR(160), '') 
     = REPLACE(REPLACE(REPLACE(LOWER(I.SupplierName), ' ', ''), CHAR(9), ''), CHAR(160), '');

SELECT @@ROWCOUNT AS RowsDeleted;


SELECT DISTINCT Supplier
FROM warehousing.Staging_Invoices
WHERE Supplier IS NOT NULL
ORDER BY Supplier;


SELECT S.Supplier, I.SupplierName
FROM warehousing.Staging_Invoices S
JOIN #InvalidSuppliers I
    ON REPLACE(REPLACE(REPLACE(LOWER(S.Supplier), ' ', ''), CHAR(9), ''), CHAR(160), '') 
     = REPLACE(REPLACE(REPLACE(LOWER(I.SupplierName), ' ', ''), CHAR(9), ''), CHAR(160), '');


-- STEP 3 — PART 1: ADDING CLEANED COLUMNS TO STAGING TABLE
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA='warehousing' 
      AND TABLE_NAME='Staging_Invoices' 
      AND COLUMN_NAME='Clean_Date'
)
ALTER TABLE warehousing.Staging_Invoices ADD Clean_Date DATE;

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA='warehousing' 
      AND TABLE_NAME='Staging_Invoices' 
      AND COLUMN_NAME='Clean_Amount'
)
ALTER TABLE warehousing.Staging_Invoices ADD Clean_Amount DECIMAL(18,2);

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA='warehousing' 
      AND TABLE_NAME='Staging_Invoices' 
      AND COLUMN_NAME='Clean_Supplier'
)
ALTER TABLE warehousing.Staging_Invoices ADD Clean_Supplier NVARCHAR(400);

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA='warehousing' 
      AND TABLE_NAME='Staging_Invoices' 
      AND COLUMN_NAME='Clean_Dept'
)
ALTER TABLE warehousing.Staging_Invoices ADD Clean_Dept NVARCHAR(200);

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA='warehousing' 
      AND TABLE_NAME='Staging_Invoices' 
      AND COLUMN_NAME='Clean_ExpenseType'
)
ALTER TABLE warehousing.Staging_Invoices ADD Clean_ExpenseType NVARCHAR(200);

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA='warehousing' 
      AND TABLE_NAME='Staging_Invoices' 
      AND COLUMN_NAME='Clean_ExpenseArea'
)
ALTER TABLE warehousing.Staging_Invoices ADD Clean_ExpenseArea NVARCHAR(300);

SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'Staging_Invoices'
  AND TABLE_SCHEMA = 'warehousing';

-- STEP 3 — PART 2: CLEANING THE DATA
-- 2.1 Convert Date

UPDATE warehousing.Staging_Invoices
SET Clean_Date = TRY_CONVERT(DATE, Date_of_Payment, 105);

-- 2.2 Convert Amount
UPDATE warehousing.Staging_Invoices
SET Clean_Amount =
    TRY_CONVERT(DECIMAL(18,2),
        REPLACE(REPLACE(REPLACE(Amount, '£', ''), ',', ''), '"', '')
    );

-- 2.3 Clean Supplier Name
UPDATE warehousing.Staging_Invoices
SET Clean_Supplier =
    CASE 
        WHEN Supplier = 'Personal Expense, Name Withheld' THEN '24005856'
        ELSE Supplier
    END;

-- 2.4 Copy Clean Dept / Expense Fields
UPDATE warehousing.Staging_Invoices
SET 
    Clean_Dept        = Department,
    Clean_ExpenseType = Expense_Type,
    Clean_ExpenseArea = Expense_Area;


--  STEP 3 — PART 3: CREATE DIMENSION TABLES
-- 3.1 DimDate
CREATE TABLE DimDate (
    DateKey       INT PRIMARY KEY,
    FullDate      DATE,
    Day           INT,
    Month         INT,
    MonthName     NVARCHAR(20),
    Quarter       INT,
    Year          INT
);

INSERT INTO DimDate (DateKey, FullDate, Day, Month, MonthName, Quarter, Year)
SELECT DISTINCT
    CONVERT(INT, FORMAT(Clean_Date, 'yyyyMMdd')),
    Clean_Date,
    DAY(Clean_Date),
    MONTH(Clean_Date),
    DATENAME(MONTH, Clean_Date),
    DATEPART(QUARTER, Clean_Date),
    YEAR(Clean_Date)
FROM warehousing.Staging_Invoices
WHERE Clean_Date IS NOT NULL;

-- 3.2 DimSupplier
CREATE TABLE DimSupplier (
    SupplierKey INT IDENTITY(1,1) PRIMARY KEY,
    SupplierName NVARCHAR(400)
);

INSERT INTO DimSupplier (SupplierName)
SELECT DISTINCT Clean_Supplier
FROM warehousing.Staging_Invoices
WHERE Clean_Supplier IS NOT NULL;

-- 3.3 DimExpenseType
CREATE TABLE DimExpenseType (
    ExpenseTypeKey INT IDENTITY(1,1) PRIMARY KEY,
    ExpenseType NVARCHAR(200)
);

INSERT INTO DimExpenseType (ExpenseType)
SELECT DISTINCT Clean_ExpenseType
FROM warehousing.Staging_Invoices
WHERE Clean_ExpenseType IS NOT NULL;

-- 3.4 DimExpenseArea
CREATE TABLE DimExpenseArea (
    ExpenseAreaKey INT IDENTITY(1,1) PRIMARY KEY,
    ExpenseArea NVARCHAR(300)
);

INSERT INTO DimExpenseArea (ExpenseArea)
SELECT DISTINCT Clean_ExpenseArea
FROM warehousing.Staging_Invoices
WHERE Clean_ExpenseArea IS NOT NULL;


-- TASK 4(a) — Top 3 Suppliers (4 months + monthly)
CREATE OR ALTER PROCEDURE dbo.usp_Top3Suppliers_Overall_And_Monthly
AS
BEGIN
    SET NOCOUNT ON;

    ----------------------------------------------------
    -- PART 1: Overall Top 3 Suppliers (4-month period)
    ----------------------------------------------------
    SELECT TOP 3
        s.SupplierName,
        SUM(f.Amount) AS TotalSpend
    FROM dbo.FactInvoice f
    INNER JOIN dbo.DimSupplier s
        ON f.SupplierKey = s.SupplierKey
    GROUP BY s.SupplierName
    ORDER BY TotalSpend DESC;


    ----------------------------------------------------
    -- PART 2: Monthly Top 3 Suppliers (per month)
    ----------------------------------------------------
    WITH MonthlySupplierSpend AS (
        SELECT
            d.Year,
            d.MonthName,
            s.SupplierName,
            SUM(f.Amount) AS MonthlySpend,
            RANK() OVER (
                PARTITION BY d.Year, d.MonthName
                ORDER BY SUM(f.Amount) DESC
            ) AS RankPosition
        FROM dbo.FactInvoice f
        INNER JOIN dbo.DimSupplier s
            ON f.SupplierKey = s.SupplierKey
        INNER JOIN dbo.DimDate d
            ON f.DateKey = d.DateKey
        GROUP BY
            d.Year,
            d.MonthName,
            s.SupplierName
    )
    SELECT
        Year,
        MonthName,
        SupplierName,
        MonthlySpend,
        RankPosition
    FROM MonthlySupplierSpend
    WHERE RankPosition <= 3
    ORDER BY
        Year,
        MonthName,
        RankPosition;

END;
GO
EXEC dbo.usp_Top3Suppliers_Overall_And_Monthly;


-- TASK 4(b) — Expense Types > Average (2-Month) + JSON
CREATE OR ALTER PROCEDURE dbo.usp_ExpenseType_Above_Average_2Month_JSON
AS
BEGIN
    SET NOCOUNT ON;

    -- STEP 1: Calculate 2-Month Total Spend per Expense Type

    WITH ExpenseTypeSpend AS (
        SELECT
            et.ExpenseType,
            d.Year,
            d.MonthName,
            SUM(f.Amount) AS MonthlySpend
        FROM dbo.FactInvoice f
        INNER JOIN dbo.DimExpenseType et
            ON f.ExpenseTypeKey = et.ExpenseTypeKey
        INNER JOIN dbo.DimDate d
            ON f.DateKey = d.DateKey
        GROUP BY
            et.ExpenseType,
            d.Year,
            d.MonthName
    ),

    ----------------------------------------------------
    -- STEP 2: Aggregate to 2-Month Spend per Expense Type
    ----------------------------------------------------
    TwoMonthTotals AS (
        SELECT
            ExpenseType,
            SUM(MonthlySpend) AS TwoMonthSpend
        FROM ExpenseTypeSpend
        GROUP BY ExpenseType
    ),

    ----------------------------------------------------
    -- STEP 3: Calculate Average 2-Month Spend
    ----------------------------------------------------
    AverageSpend AS (
        SELECT
            AVG(TwoMonthSpend) AS AvgTwoMonthSpend
        FROM TwoMonthTotals
    )

    ----------------------------------------------------
    -- STEP 4: Select Expense Types Above Average + Export JSON
    ----------------------------------------------------
    SELECT
        t.ExpenseType,
        t.TwoMonthSpend
    FROM TwoMonthTotals t
    CROSS JOIN AverageSpend a
    WHERE t.TwoMonthSpend > a.AvgTwoMonthSpend
    ORDER BY t.TwoMonthSpend DESC
    FOR JSON PATH, ROOT('ExpenseTypesAboveAverage');

END;
GO

EXEC dbo.usp_ExpenseType_Above_Average_2Month_JSON;


-- TASK 4(c) — Monthly Top 10 Expense Areas + Rank Movement
CREATE OR ALTER PROCEDURE dbo.usp_Monthly_Top10_ExpenseArea_Ranking
AS
BEGIN
    SET NOCOUNT ON;

    -------------------------------------------------------
    -- STEP 1: Monthly Spend per Expense Area
    -------------------------------------------------------
    WITH MonthlySpend AS (
        SELECT
            d.Year,
            d.Month,
            d.MonthName,
            ea.ExpenseArea,
            SUM(f.Amount) AS MonthlySpend
        FROM dbo.FactInvoice f
        INNER JOIN dbo.DimExpenseArea ea
            ON f.ExpenseAreaKey = ea.ExpenseAreaKey
        INNER JOIN dbo.DimDate d
            ON f.DateKey = d.DateKey
        GROUP BY
            d.Year,
            d.Month,
            d.MonthName,
            ea.ExpenseArea
    ),

    -------------------------------------------------------
    -- STEP 2: Rank Expense Areas per Month
    -------------------------------------------------------
    RankedSpend AS (
        SELECT
            Year,
            Month,
            MonthName,
            ExpenseArea,
            MonthlySpend,
            RANK() OVER (
                PARTITION BY Year, Month
                ORDER BY MonthlySpend DESC
            ) AS RankPosition
        FROM MonthlySpend
    ),

    -------------------------------------------------------
    -- STEP 3: Track Movement from Previous Month
    -------------------------------------------------------
    MovementCalc AS (
        SELECT
            Year,
            Month,
            MonthName,
            ExpenseArea,
            MonthlySpend,
            RankPosition,
            LAG(RankPosition) OVER (
                PARTITION BY ExpenseArea
                ORDER BY Year, Month
            ) AS PreviousRank
        FROM RankedSpend
        WHERE RankPosition <= 10
    )

    -------------------------------------------------------
    -- STEP 4: Final Output
    -------------------------------------------------------
    SELECT
        Year,
        MonthName,
        ExpenseArea,
        MonthlySpend,
        RankPosition,
        PreviousRank,
        CASE
            WHEN PreviousRank IS NULL THEN 'New'
            WHEN RankPosition < PreviousRank THEN 'Up'
            WHEN RankPosition > PreviousRank THEN 'Down'
            ELSE 'No Change'
        END AS Movement,
        CASE
            WHEN PreviousRank IS NULL THEN NULL
            ELSE ABS(RankPosition - PreviousRank)
        END AS PositionsMoved
    FROM MovementCalc
    ORDER BY
        Year,
        Month,
        RankPosition ASC;

END;
GO

EXEC dbo.usp_Monthly_Top10_ExpenseArea_Ranking;


-- TASK 4(d) — Complex Time-Hierarchy Supplier Analysis + CSV Export

CREATE OR ALTER PROCEDURE dbo.usp_Supplier_TimeHierarchy_Analysis
AS
BEGIN
    SET NOCOUNT ON;

    -------------------------------------------------------
    -- STEP 1: Supplier Spend by Year & Month
    -------------------------------------------------------
    WITH SupplierMonthlySpend AS (
        SELECT
            d.Year,
            d.Month,
            d.MonthName,
            s.SupplierName,
            SUM(f.Amount) AS MonthlySpend
        FROM dbo.FactInvoice f
        INNER JOIN dbo.DimSupplier s
            ON f.SupplierKey = s.SupplierKey
        INNER JOIN dbo.DimDate d
            ON f.DateKey = d.DateKey
        GROUP BY
            d.Year,
            d.Month,
            d.MonthName,
            s.SupplierName
    ),

    -------------------------------------------------------
    -- STEP 2: Calculate Cumulative Spend per Supplier
    -------------------------------------------------------
    CumulativeSpend AS (
        SELECT
            Year,
            Month,
            MonthName,
            SupplierName,
            MonthlySpend,
            SUM(MonthlySpend) OVER (
                PARTITION BY SupplierName
                ORDER BY Year, Month
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS CumulativeSpend
        FROM SupplierMonthlySpend
    ),

    -------------------------------------------------------
    -- STEP 3: Rank Suppliers per Year
    -------------------------------------------------------
    RankedSuppliers AS (
        SELECT
            Year,
            MonthName,
            SupplierName,
            MonthlySpend,
            CumulativeSpend,
            RANK() OVER (
                PARTITION BY Year
                ORDER BY CumulativeSpend DESC
            ) AS SupplierRank
        FROM CumulativeSpend
    )

    -------------------------------------------------------
    -- STEP 4: Final Output (CSV Ready)
    -------------------------------------------------------
    SELECT
        Year,
        MonthName,
        SupplierName,
        MonthlySpend,
        CumulativeSpend,
        SupplierRank
    FROM RankedSuppliers
    ORDER BY
        Year,
        SupplierRank,
        SupplierName;

END;
GO
EXEC dbo.usp_Supplier_TimeHierarchy_Analysis;


















