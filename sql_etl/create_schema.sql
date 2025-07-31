-- =========================================
-- Dimension: Date
-- =========================================
CREATE TABLE [dbo].[DimDate] (
    [DateKey] INT NOT NULL PRIMARY KEY, -- Format: YYYYMMDD
    [FullDate] DATE NOT NULL,
    [year] INT NOT NULL,
    [month] SMALLINT NOT NULL,
    [day] SMALLINT NOT NULL,
    CONSTRAINT CK_DimDate_DateKey CHECK ([DateKey] BETWEEN 19000101 AND 21001231)
);

-- =========================================
-- Dimension: Country
-- =========================================

CREATE TABLE dbo.DimCountry (
    CountryKey INT NOT NULL PRIMARY KEY IDENTITY(1,1),
    GTDCountryCode INT NOT NULL,
    CountryName NVARCHAR(100) NOT NULL
);
GO




-- =========================================
-- Dimension: Region
-- =========================================
CREATE TABLE [dbo].[DimRegion] (
    RegionKey INT NOT NULL PRIMARY KEY IDENTITY(1,1),
    region INT NOT NULL,
    RegionName NVARCHAR(100) NOT NULL
);

-- =========================================
-- Dimension: Attack Type
-- =========================================
CREATE TABLE [dbo].[DimAttackType] (
    AttackTypeKey INT NOT NULL PRIMARY KEY IDENTITY(1,1),
    attacktype1 INT NOT NULL,
    AttackTypeLabel NVARCHAR(100) NOT NULL
);

-- =========================================
-- Dimension: Target Type
-- =========================================
CREATE TABLE [dbo].[DimTargetType] (
    TargetTypeKey INT NOT NULL PRIMARY KEY IDENTITY(1,1),
    targtype1 INT NOT NULL,
    TargetTypeLabel NVARCHAR(100) NOT NULL
);

-- =========================================
-- Dimension: Weapon Type
-- =========================================
CREATE TABLE [dbo].[DimWeaponType] (
    WeaponTypeKey INT NOT NULL PRIMARY KEY IDENTITY(1,1),
    weaptype1 INT NOT NULL,
    WeaponTypeLabel NVARCHAR(100) NOT NULL
);

-- =========================================
-- Dimension: Perpetrator Group
-- =========================================
CREATE TABLE [dbo].[DimPerpetratorGroup] (
    PerpetratorGroupKey INT NOT NULL PRIMARY KEY IDENTITY(1,1),
    GroupName NVARCHAR(255) NOT NULL
);

-- =========================================
-- Dimension: Success
-- =========================================
CREATE TABLE [dbo].[DimSuccess] (
    SuccessKey INT NOT NULL PRIMARY KEY IDENTITY(1,1),
    IsSuccessful BIT NOT NULL,
    Description NVARCHAR(50) NOT NULL
);

-- =========================================
-- Dimension: Suicide
-- =========================================
CREATE TABLE [dbo].[DimSuicide] (
    SuicideKey INT NOT NULL PRIMARY KEY IDENTITY(1,1),
    IsSuicide BIT NOT NULL,
    Description NVARCHAR(50) NOT NULL
);

-- =========================================
-- Dimension: Property Damage
-- =========================================
CREATE TABLE dbo.DimPropertyDamage (
    PropertyDamageKey INT NOT NULL PRIMARY KEY IDENTITY(1,1),
    PropertyDamageCode SMALLINT NOT NULL,
    Description NVARCHAR(50) NOT NULL
);

-- =========================================
-- Dimension: Hostage Situation
-- =========================================
CREATE TABLE dbo.DimHostageSituation (
    HostageSituationKey INT NOT NULL PRIMARY KEY IDENTITY(1,1),
    HostageSituationCode SMALLINT NULL,
    Description NVARCHAR(50) NOT NULL
);

-- =========================================
-- Dimension: Doubt Terrorism Proper
-- =========================================
CREATE TABLE dbo.DimDoubtTerrorism (
    DoubtTerrorismKey INT NOT NULL PRIMARY KEY IDENTITY(1,1),
    DoubtTerrorismCode SMALLINT NOT NULL,
    Description NVARCHAR(50) NOT NULL
);


-- =========================================
-- Dimension: Event Type
-- =========================================
CREATE TABLE [dbo].[DimEventType] (
    EventTypeKey INT NOT NULL PRIMARY KEY IDENTITY(1,1),
    IsMultiple BIT NOT NULL,
    Description NVARCHAR(50) NOT NULL
);

-- =========================================
-- Dimension: Extended Incident
-- =========================================
CREATE TABLE [dbo].[DimExtendedIncident] (
    ExtendedIncidentKey INT NOT NULL PRIMARY KEY IDENTITY(1,1),
    IsExtended BIT NOT NULL,
    Description NVARCHAR(50) NOT NULL
);

-- =========================================
-- Dimension: Weapon Subtype
-- =========================================
CREATE TABLE [dbo].[DimWeaponSubType] (
    WeaponSubTypeKey INT NOT NULL PRIMARY KEY IDENTITY(1,1),
    weapsubtype1 INT NOT NULL,
    WeaponSubTypeLabel NVARCHAR(100) NOT NULL
);

-- =========================================
-- Dimension: Target Subtype
-- =========================================
CREATE TABLE [dbo].[DimTargetSubType] (
    TargetSubTypeKey INT NOT NULL PRIMARY KEY IDENTITY(1,1),
    targsubtype1 INT NOT NULL,
    TargetSubTypeLabel NVARCHAR(100) NOT NULL
);

-- =========================================
-- Dimension: Attack Success Type
-- =========================================
CREATE TABLE [dbo].[DimAttackSuccessType] (
    AttackSuccessTypeKey INT NOT NULL PRIMARY KEY IDENTITY(1,1),
    SuccessCode INT NOT NULL,
    SuccessLabel NVARCHAR(100) NOT NULL
);

-- =========================================
-- Dimension: Location
-- =========================================
CREATE TABLE [dbo].[DimLocation] (
    LocationKey INT NOT NULL PRIMARY KEY IDENTITY(1,1),
    country INT NOT NULL,
    region INT NOT NULL,
    City NVARCHAR(200) NOT NULL,
    Province NVARCHAR(200) NULL,
    Latitude FLOAT NULL,
    Longitude FLOAT NULL
);

-- =========================================
-- Fact Table: Terror Events
-- =========================================
CREATE TABLE [dbo].[Fact_Terror_Events] (
    EventID NVARCHAR(50) NOT NULL PRIMARY KEY,
    DateKey INT NOT NULL,
    LocationKey INT NOT NULL,
    CountryKey INT NULL, -- ✅ Added CountryKey
    AttackTypeKey INT NULL,
    TargetTypeKey INT NULL,
    TargetSubTypeKey INT NULL,
    WeaponTypeKey INT NULL,
    WeaponSubTypeKey INT NULL,
    PerpetratorGroupKey INT NULL,
    SuccessKey INT NULL,
    SuicideKey INT NULL,
    PropertyDamageKey INT NULL,
    HostageSituationKey INT NULL,
    DoubtTerrorismKey INT NULL,
    AlternativeDesignationKey INT NULL,
    EventTypeKey INT NULL,
    ExtendedIncidentKey INT NULL,
    AttackSuccessTypeKey INT NULL,
    NumKilled INT NULL,
    NumWounded INT NULL,
    NumUSKilled INT NULL,
    NumUSWounded INT NULL,
    NumTerroristsKilled INT NULL,
    NumTerroristsWounded INT NULL,

    CONSTRAINT FK_Fact_Date FOREIGN KEY (DateKey) REFERENCES dbo.DimDate(DateKey),
    CONSTRAINT FK_Fact_Location FOREIGN KEY (LocationKey) REFERENCES dbo.DimLocation(LocationKey),
    CONSTRAINT FK_Fact_Country FOREIGN KEY (CountryKey) REFERENCES dbo.DimCountry(CountryKey), -- ✅ New FK
    CONSTRAINT FK_Fact_AttackType FOREIGN KEY (AttackTypeKey) REFERENCES dbo.DimAttackType(AttackTypeKey),
    CONSTRAINT FK_Fact_TargetType FOREIGN KEY (TargetTypeKey) REFERENCES dbo.DimTargetType(TargetTypeKey),
    CONSTRAINT FK_Fact_TargetSubType FOREIGN KEY (TargetSubTypeKey) REFERENCES dbo.DimTargetSubType(TargetSubTypeKey),
    CONSTRAINT FK_Fact_WeaponType FOREIGN KEY (WeaponTypeKey) REFERENCES dbo.DimWeaponType(WeaponTypeKey),
    CONSTRAINT FK_Fact_WeaponSubType FOREIGN KEY (WeaponSubTypeKey) REFERENCES dbo.DimWeaponSubType(WeaponSubTypeKey),
    CONSTRAINT FK_Fact_Perpetrator FOREIGN KEY (PerpetratorGroupKey) REFERENCES dbo.DimPerpetratorGroup(PerpetratorGroupKey),
    CONSTRAINT FK_Fact_Success FOREIGN KEY (SuccessKey) REFERENCES dbo.DimSuccess(SuccessKey),
    CONSTRAINT FK_Fact_Suicide FOREIGN KEY (SuicideKey) REFERENCES dbo.DimSuicide(SuicideKey),
    CONSTRAINT FK_Fact_PropertyDamage FOREIGN KEY (PropertyDamageKey) REFERENCES dbo.DimPropertyDamage(PropertyDamageKey),
    CONSTRAINT FK_Fact_HostageSituation FOREIGN KEY (HostageSituationKey) REFERENCES dbo.DimHostageSituation(HostageSituationKey),
    CONSTRAINT FK_Fact_DoubtTerrorism FOREIGN KEY (DoubtTerrorismKey) REFERENCES dbo.DimDoubtTerrorism(DoubtTerrorismKey),
    CONSTRAINT FK_Fact_EventType FOREIGN KEY (EventTypeKey) REFERENCES dbo.DimEventType(EventTypeKey),
    CONSTRAINT FK_Fact_ExtendedIncident FOREIGN KEY (ExtendedIncidentKey) REFERENCES dbo.DimExtendedIncident(ExtendedIncidentKey),
    CONSTRAINT FK_Fact_AttackSuccessType FOREIGN KEY (AttackSuccessTypeKey) REFERENCES dbo.DimAttackSuccessType(AttackSuccessTypeKey)
);

