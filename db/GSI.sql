-- =============================================
-- GSI DATABASE INITIALIZATION SCRIPT
-- Matches definitions in models.py
-- =============================================

USE GSI;
GO

-- =============================================
-- 1. CLEANUP (Drop FKs and Tables)
-- =============================================

-- Remove FK from Users if it exists (so we can drop IndexingCounties)
IF OBJECT_ID('FK_Users_Counties', 'F') IS NOT NULL
ALTER TABLE dbo.Users DROP CONSTRAINT FK_Users_Counties;
GO

-- Drop CountyImages if exists
IF OBJECT_ID('dbo.County_Images', 'U') IS NOT NULL
DROP TABLE dbo.County_Images;
GO

-- Drop Users if exists
IF OBJECT_ID('dbo.Users', 'U') IS NOT NULL
DROP TABLE dbo.Users;
GO

-- Drop IndexingStates if exists
IF OBJECT_ID('dbo.Indexing_States', 'U') IS NOT NULL
DROP TABLE dbo.Indexing_States;
GO

-- Drop IndexingCounties if exists
IF OBJECT_ID('dbo.Indexing_Counties', 'U') IS NOT NULL
DROP TABLE dbo.Indexing_Counties;
GO

-- Drop unindexed_images if exists
IF OBJECT_ID('dbo.unindexed_images', 'U') IS NOT NULL
DROP TABLE dbo.unindexed_images;
GO

-- =============================================
-- 2. SETUP TABLES
-- =============================================

-- 1. Indexing States
IF OBJECT_ID('dbo.indexing_states', 'U') IS NOT NULL DROP TABLE dbo.indexing_states;
CREATE TABLE indexing_states (
    id INT IDENTITY(1,1) PRIMARY KEY,
    state_name NVARCHAR(100),
    fips_code NVARCHAR(10),
    is_enabled BIT DEFAULT 0,
    is_locked BIT DEFAULT 0
);

-- 2. Indexing Counties
IF OBJECT_ID('dbo.indexing_counties', 'U') IS NOT NULL DROP TABLE dbo.indexing_counties;
CREATE TABLE indexing_counties (
    id INT IDENTITY(1,1) PRIMARY KEY,
    county_name NVARCHAR(100),
    geo_id NVARCHAR(50),
    state_fips NVARCHAR(10),
    is_active BIT DEFAULT 0,
    is_enabled BIT DEFAULT 0,
    is_locked BIT DEFAULT 0,
    notes NVARCHAR(MAX)
);

-- 3. Users
-- Depends on indexing_counties for the current_working_county_id foreign key
IF OBJECT_ID('dbo.users', 'U') IS NOT NULL DROP TABLE dbo.users;
CREATE TABLE users (
    id INT IDENTITY(1,1) PRIMARY KEY,
    username NVARCHAR(150) NOT NULL UNIQUE,
    email NVARCHAR(150) NOT NULL UNIQUE,
    password_hash NVARCHAR(256),
    role NVARCHAR(50) DEFAULT 'user',
    is_verified BIT DEFAULT 0,
    verification_code NVARCHAR(6),
    is_locked BIT DEFAULT 0,
    is_temporary_password BIT DEFAULT 0,
    current_working_county_id INT NULL,
    CONSTRAINT FK_Users_Counties FOREIGN KEY (current_working_county_id) 
        REFERENCES indexing_counties(id)
);

-- 4. County Images
IF OBJECT_ID('dbo.county_images', 'U') IS NOT NULL DROP TABLE dbo.county_images;
CREATE TABLE county_images (
    id INT IDENTITY(1,1) PRIMARY KEY,
    county_id INT,
    image_path NVARCHAR(255),
    CONSTRAINT FK_CountyImages_Counties FOREIGN KEY (county_id) 
        REFERENCES indexing_counties(id) ON DELETE CASCADE
);

-- 5. Unindexed Images
IF OBJECT_ID('dbo.unindexed_images', 'U') IS NOT NULL DROP TABLE dbo.unindexed_images;
CREATE TABLE unindexed_images (
    id INT IDENTITY(1,1) PRIMARY KEY,
    county_id INT NOT NULL,
    full_path NVARCHAR(4000) NOT NULL,
    book_name NVARCHAR(255),
    page_name NVARCHAR(255),
    require_indexing BIT DEFAULT 0,
    scanned_at DATETIME DEFAULT GETDATE(),
    CONSTRAINT FK_UnindexedImages_Counties FOREIGN KEY (county_id) 
        REFERENCES indexing_counties(id) ON DELETE CASCADE
);