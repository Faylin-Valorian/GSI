USE GSI;
GO

-- =============================================
-- 1. CLEANUP (Drop FKs and Tables)
-- =============================================

-- Remove FK from Users if it exists (so we can drop IndexingCounties)
IF OBJECT_ID('FK_Users_IndexingCounties', 'F') IS NOT NULL
ALTER TABLE dbo.Users DROP CONSTRAINT FK_Users_IndexingCounties;
GO

-- Drop CountyImages if exists
IF OBJECT_ID('dbo.CountyImages', 'U') IS NOT NULL
DROP TABLE dbo.CountyImages;
GO

-- Drop Users if exists
IF OBJECT_ID('dbo.Users', 'U') IS NOT NULL
DROP TABLE dbo.Users;
GO

-- Drop IndexingStates if exists
IF OBJECT_ID('dbo.IndexingStates', 'U') IS NOT NULL
DROP TABLE dbo.IndexingStates;
GO

-- Drop IndexingCounties if exists
IF OBJECT_ID('dbo.IndexingCounties', 'U') IS NOT NULL
DROP TABLE dbo.IndexingCounties;
GO

-- =============================================
-- 2. CREATE TABLES
-- =============================================

-- TABLE: Users
CREATE TABLE Users (
    id INT IDENTITY(1,1) PRIMARY KEY,
    username NVARCHAR(50) NOT NULL UNIQUE,
    email NVARCHAR(100) NOT NULL UNIQUE,
    password_hash NVARCHAR(255) NOT NULL,
    
    -- Verification
    is_verified BIT DEFAULT 0,
    verification_code NVARCHAR(6) NULL,

    -- Account Management
    is_locked BIT DEFAULT 0,          
    role NVARCHAR(20) DEFAULT 'user', 
    is_temporary_password BIT DEFAULT 0,

    -- Workflow (New)
    current_working_county_id INT NULL 
    -- Note: Foreign Key added at the end of script
);
GO

-- TABLE: IndexingStates
CREATE TABLE IndexingStates (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    StateName NVARCHAR(100) NOT NULL,
    FipsCode NVARCHAR(10) NOT NULL, 
    IsEnabled BIT DEFAULT 1,
    IsLocked BIT DEFAULT 0
);
GO

-- TABLE: IndexingCounties
CREATE TABLE IndexingCounties (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    CountyName NVARCHAR(100) NOT NULL,
    GeoId NVARCHAR(100) NOT NULL,  
    StateFips NVARCHAR(10) NOT NULL,
    
    -- Status Flags
    IsActive BIT DEFAULT 0, -- 0=Inactive (Red), 1=Active (Green)
    IsLocked BIT DEFAULT 0,
    IsEnabled BIT DEFAULT 0, -- Visibility on Map

    -- Data (New)
    notes NVARCHAR(MAX) NULL
);
GO

-- TABLE: CountyImages (New)
CREATE TABLE CountyImages (
    id INT IDENTITY(1,1) PRIMARY KEY,
    county_id INT NOT NULL,
    image_path NVARCHAR(255) NOT NULL,

    CONSTRAINT FK_CountyImages_IndexingCounties 
    FOREIGN KEY (county_id) REFERENCES dbo.IndexingCounties(Id)
    ON DELETE CASCADE
);
GO

IF OBJECT_ID('dbo.UnindexedImages', 'U') IS NOT NULL
    DROP TABLE dbo.UnindexedImages;
GO

CREATE TABLE dbo.UnindexedImages (
    id INT IDENTITY(1,1) PRIMARY KEY,
    county_id INT NOT NULL,
    full_path NVARCHAR(MAX) NOT NULL,
    book_name NVARCHAR(255),
    page_name NVARCHAR(255),
    require_indexing BIT DEFAULT 0,
    scanned_at DATETIME DEFAULT GETDATE(),
    CONSTRAINT FK_UnindexedImages_Counties FOREIGN KEY (county_id) 
        REFERENCES dbo.IndexingCounties(Id) 
        ON DELETE CASCADE
);
GO

-- =============================================
-- 3. APPLY CONSTRAINTS
-- =============================================

-- Add FK to Users (Now that IndexingCounties exists)
ALTER TABLE dbo.Users WITH CHECK 
ADD CONSTRAINT FK_Users_IndexingCounties FOREIGN KEY(current_working_county_id)
REFERENCES dbo.IndexingCounties (Id);
GO