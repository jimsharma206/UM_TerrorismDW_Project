--*************************************************************************--
-- Title: UM_Terrorism_ETL_Log_Setup
-- Author: James Sharma
-- Desc: Logging setup for ETL procedures 
--*************************************************************************--

USE UM_Terrorism_DW; 
GO
SET NOCOUNT ON;

--********************************************************************--
-- Create ETLLog Table
--********************************************************************--
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'ETLLog')
CREATE TABLE [dbo].[ETLLog] (
    [ETLLogID] INT IDENTITY PRIMARY KEY,
    [ETLDateAndTime] DATETIME DEFAULT GETDATE(),
    [ETLAction] VARCHAR(100),
    [ETLLogMessage] VARCHAR(2000)
);
GO

--********************************************************************--
-- Create vETLLog View
--********************************************************************--
CREATE OR ALTER VIEW [dbo].[vETLLog] AS
SELECT
    ETLLogID,
    ETLDate = FORMAT(ETLDateAndTime, 'D', 'en-us'),
    ETLTime = FORMAT(CAST(ETLDateAndTime AS datetime2), 'HH:mm', 'en-us'),
    ETLAction,
    ETLLogMessage
FROM [dbo].[ETLLog];
GO

--********************************************************************--
-- Create pInsETLLog Procedure
--********************************************************************--
CREATE OR ALTER PROCEDURE [dbo].[pInsETLLog]
    @ETLAction VARCHAR(100),
    @ETLLogMessage VARCHAR(2000)
AS
BEGIN
    BEGIN TRY
        INSERT INTO [dbo].[ETLLog] (ETLAction, ETLLogMessage)
        VALUES (@ETLAction, @ETLLogMessage);
    END TRY
    BEGIN CATCH
        -- No rollback or transaction control here
        -- You can optionally PRINT or log errors elsewhere
    END CATCH
END
GO


--####################################################################--
--## Procedure: pETLDropForeignKeys
--## Desc: Safely removes all FKs from Fact_Terror_Events
--## Improvements:
--##   - Includes IF EXISTS checks
--##   - Uses table-driven loop for maintainability
--##   - Logs result and returns 1 (success) / -1 (failure)
--## Change Log:
--##   2025-07-24, JSharma, Hardened and refactored
--####################################################################--
CREATE OR ALTER PROCEDURE pETLDropForeignKeys
AS
BEGIN
    DECLARE @RC INT = 0;
    DECLARE @SQL NVARCHAR(MAX);

    BEGIN TRY
        BEGIN TRAN;

        -- Dynamically drop all foreign keys on Fact_Terror_Events
        SELECT @SQL = STRING_AGG('ALTER TABLE dbo.Fact_Terror_Events DROP CONSTRAINT [' + fk.name + ']', '; ')
        FROM sys.foreign_keys fk
        WHERE fk.parent_object_id = OBJECT_ID('dbo.Fact_Terror_Events');

        IF @SQL IS NOT NULL
            EXEC sp_executesql @SQL;

        EXEC pInsETLLog
            @ETLAction = 'pETLDropForeignKeys',
            @ETLLogMessage = 'Foreign keys dropped from Fact_Terror_Events';

        COMMIT TRAN;
        SET @RC = 1;
    END TRY

    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;

        DECLARE @ErrorMessage NVARCHAR(1000) = ERROR_MESSAGE();
        EXEC pInsETLLog 
            @ETLAction = 'pETLDropForeignKeys',
            @ETLLogMessage = @ErrorMessage;

        SET @RC = -1;
    END CATCH

    RETURN @RC;
END
GO

--********************************************************************--
--####################################################################--
--## Procedure: pETLTruncateTables
--## Desc: Truncates fact and dimension tables for Terrorism DW
--## Returns: 1 = success, -1 = failure
--## Change Log:
--##   2025-07-24, JSharma, Added return codes
--####################################################################--
CREATE OR ALTER PROCEDURE [dbo].[pETLTruncateTables]
AS
BEGIN
    DECLARE @RC INT = 0;

    BEGIN TRY
        -- Truncate fact table first (due to FK dependencies)
        TRUNCATE TABLE [dbo].[Fact_Terror_Events];

        -- Truncate dimension tables
        TRUNCATE TABLE [dbo].[DimDate];
        TRUNCATE TABLE [dbo].[DimCountry];
        TRUNCATE TABLE [dbo].[DimRegion];
        TRUNCATE TABLE [dbo].[DimAttackType];
        TRUNCATE TABLE [dbo].[DimTargetType];
        TRUNCATE TABLE [dbo].[DimTargetSubType];
        TRUNCATE TABLE [dbo].[DimWeaponType];
        TRUNCATE TABLE [dbo].[DimWeaponSubType];
        TRUNCATE TABLE [dbo].[DimPerpetratorGroup];
        TRUNCATE TABLE [dbo].[DimSuccess];
        TRUNCATE TABLE [dbo].[DimSuicide];
        TRUNCATE TABLE [dbo].[DimPropertyDamage];
        TRUNCATE TABLE [dbo].[DimHostageSituation];
        TRUNCATE TABLE [dbo].[DimDoubtTerrorism];
        TRUNCATE TABLE [dbo].[DimEventType];
        TRUNCATE TABLE [dbo].[DimExtendedIncident];
        TRUNCATE TABLE [dbo].[DimAttackSuccessType];
        TRUNCATE TABLE [dbo].[DimLocation];

        EXEC [dbo].[pInsETLLog]
            @ETLAction = 'pETLTruncateTables',
            @ETLLogMessage = 'Terrorism DW dimension and fact tables truncated';

        SET @RC = 1;
    END TRY
    BEGIN CATCH
        DECLARE @ErrMsg NVARCHAR(1000) = ERROR_MESSAGE();
        EXEC [dbo].[pInsETLLog]
            @ETLAction = 'pETLTruncateTables',
            @ETLLogMessage = @ErrMsg;

        SET @RC = -1;
    END CATCH

    RETURN @RC;
END
GO




CREATE OR ALTER VIEW vETLDimDate
/* Desc: Extracts valid dates from FactTerrorEvents for DimDate
** Change Log: When,Who,What
** 2025-07-20,JSharma,Fixed conversion error.
*/
AS
SELECT DISTINCT
    CAST(FORMAT(DATEFROMPARTS(year, month, day), 'yyyyMMdd') AS INT) AS DateKey,
    DATEFROMPARTS(year, month, day) AS FullDate,
    year AS [Year],
    month AS [Month],
    day AS [Day]
FROM dbo.FactTerrorEvents
WHERE ISDATE(CONCAT(year, '-', month, '-', day)) = 1
  AND year BETWEEN 1900 AND 2100
  AND month BETWEEN 1 AND 12
  AND day BETWEEN 1 AND 31;
GO



-- Procedure Creation
-- Procedure: pETLFillDimDate
-- Desc: Inserts distinct dates into DimDate
/* Desc: Inserts unique dates into DimDate from vETLDimDate
** Change Log: When,Who,What
** 2025-07-20,JSharma,Added NOT EXISTS filter to prevent PK violation
*/
CREATE OR ALTER PROCEDURE [dbo].[pETLFillDimDate]
/* Author: JSharma
** Desc: Inserts sequential rows into DimDate using loop-based generator
** Change Log: 2025-07-26, JSharma, Mimicked RRoot-style looping ETL
*/
AS
BEGIN
    DECLARE @RC INT = 0;

    BEGIN TRY
        -- Configuration
        DECLARE @StartDate DATE = '1900-01-01';
        DECLARE @EndDate DATE = '2100-12-31';
        DECLARE @DateInProcess DATE = @StartDate;

        -- Begin inserting
        BEGIN TRAN;
        WHILE @DateInProcess <= @EndDate
        BEGIN
            INSERT INTO dbo.DimDate (
                DateKey,
                FullDate,
                [year],
                [month],
                [day]
            )
            VALUES (
                CAST(CONVERT(NVARCHAR(8), @DateInProcess, 112) AS INT), -- DateKey (YYYYMMDD)
                @DateInProcess,                                          -- FullDate
                YEAR(@DateInProcess),                                    -- year
                MONTH(@DateInProcess),                                   -- month
                DAY(@DateInProcess)                                      -- day
            );

            SET @DateInProcess = DATEADD(DAY, 1, @DateInProcess);
        END
        COMMIT TRAN;

        EXEC pInsETLLog
            @ETLAction = 'pETLFillDimDate',
            @ETLLogMessage = 'DimDate filled (loop-based insert)';
        SET @RC = 1;
    END TRY

    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRAN;
        DECLARE @ErrorMessage NVARCHAR(1000) = ERROR_MESSAGE();
        EXEC pInsETLLog 
            @ETLAction = 'pETLFillDimDate',
            @ETLLogMessage = @ErrorMessage;
        SET @RC = -1;
    END CATCH

    RETURN @RC;
END
GO


/* Desc: Extracts and transforms country data for DimCountry
** Change Log: 2025-07-25, JSharma, Corrected alias names
*/

CREATE OR ALTER VIEW vETLDimCountry
/* Desc: Extracts distinct GTD country codes and names from FactTerrorEvents
** Change Log: 2025-07-25, JSharma, Aligned with GTDCountryCode in DimCountry
*/
AS
SELECT DISTINCT
    CAST(country AS INT) AS GTDCountryCode,
    CAST(country_txt AS NVARCHAR(100)) AS CountryName
FROM dbo.FactTerrorEvents
WHERE country IS NOT NULL AND country_txt IS NOT NULL;
GO







/* Testing Code:
   Select * From vETLDimCountry;
*/



CREATE OR ALTER PROCEDURE pETLFillDimCountry
/* Desc: Inserts unique GTD country codes and names into DimCountry
** Change Log: 2025-07-25, JSharma, Final version with error logging and deduplication
*/
AS
BEGIN
    DECLARE @RC INT = 0;

    BEGIN TRY
        BEGIN TRAN;

        INSERT INTO dbo.DimCountry (
            GTDCountryCode,
            CountryName
        )
        SELECT
            v.GTDCountryCode,
            v.CountryName
        FROM vETLDimCountry v
        WHERE NOT EXISTS (
            SELECT 1
            FROM dbo.DimCountry d
            WHERE d.GTDCountryCode = v.GTDCountryCode
        );

        EXEC pInsETLLog
            @ETLAction = 'pETLFillDimCountry',
            @ETLLogMessage = 'DimCountry filled (duplicates skipped)';

        COMMIT TRAN;
        SET @RC = 1;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;

        DECLARE @ErrorMessage NVARCHAR(1000) = ERROR_MESSAGE();
        EXEC pInsETLLog 
            @ETLAction = 'pETLFillDimCountry',
            @ETLLogMessage = @ErrorMessage;

        SET @RC = -1;
    END CATCH

    RETURN @RC;
END
GO


/* Desc: Extracts distinct GTD region codes and names for DimRegion
** Change Log: 2025-07-25, JSharma
*/
CREATE OR ALTER VIEW vETLDimRegion
AS
SELECT DISTINCT
    CAST(region AS INT) AS region,
    CAST(region_txt AS NVARCHAR(100)) AS RegionName
FROM dbo.FactTerrorEvents
WHERE region IS NOT NULL AND region_txt IS NOT NULL;
GO


/* Testing Code:
   Select * From vETLDimRegion;
*/


/* Desc: Inserts distinct GTD region codes and names into DimRegion
** Change Log: 2025-07-25, JSharma
*/
CREATE OR ALTER PROCEDURE pETLFillDimRegion
AS
BEGIN
    DECLARE @RC INT = 0;

    BEGIN TRY
        BEGIN TRAN;

        INSERT INTO dbo.DimRegion (
            region,
            RegionName
        )
        SELECT
            r.region,
            r.RegionName
        FROM vETLDimRegion r
        WHERE NOT EXISTS (
            SELECT 1
            FROM dbo.DimRegion d
            WHERE d.region = r.region
        );

        EXEC pInsETLLog
            @ETLAction = 'pETLFillDimRegion',
            @ETLLogMessage = 'DimRegion filled (duplicates skipped)';

        COMMIT TRAN;
        SET @RC = 1;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;

        DECLARE @ErrorMessage NVARCHAR(1000) = ERROR_MESSAGE();
        EXEC pInsETLLog 
            @ETLAction = 'pETLFillDimRegion',
            @ETLLogMessage = @ErrorMessage;

        SET @RC = -1;
    END CATCH

    RETURN @RC;
END
GO


-- ###############################################################
-- View: vETLDimAttackType
-- Desc: Extracts distinct attack type codes and labels from FactTerrorEvents
-- Notes: Excludes rows where attacktype1_txt is NULL
-- ###############################################################
CREATE OR ALTER VIEW vETLDimAttackType
AS
SELECT DISTINCT
    CAST(attacktype1 AS INT) AS attacktype1,
    CAST(ISNULL(attacktype1_txt, 'Attack Type Not Given') AS NVARCHAR(100)) AS AttackTypeLabel
FROM dbo.FactTerrorEvents
WHERE attacktype1 IS NOT NULL;
GO



-- ###############################################################
-- Procedure: pETLFillDimAttackType
-- Desc: Inserts unique attack types from vETLDimAttackType into DimAttackType
-- Notes: Skips duplicates based on attacktype1 code
-- ###############################################################
CREATE OR ALTER PROCEDURE pETLFillDimAttackType
AS
BEGIN
    DECLARE @RC INT = 0;

    BEGIN TRY
        BEGIN TRAN;

        INSERT INTO dbo.DimAttackType (
            attacktype1,
            AttackTypeLabel
        )
        SELECT
            a.attacktype1,
            a.AttackTypeLabel
        FROM vETLDimAttackType a
        WHERE NOT EXISTS (
            SELECT 1
            FROM dbo.DimAttackType d
            WHERE d.attacktype1 = a.attacktype1
        );

        EXEC pInsETLLog
            @ETLAction = 'pETLFillDimAttackType',
            @ETLLogMessage = 'DimAttackType filled (duplicates skipped)';

        COMMIT TRAN;
        SET @RC = 1;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;

        DECLARE @ErrorMessage NVARCHAR(1000) = ERROR_MESSAGE();
        EXEC pInsETLLog 
            @ETLAction = 'pETLFillDimAttackType',
            @ETLLogMessage = @ErrorMessage;

        SET @RC = -1;
    END CATCH

    RETURN @RC;
END
GO


-- ###############################################################
-- View: vETLDimTargetType
-- Desc: Extracts distinct target type codes and labels from FactTerrorEvents
-- Notes: Excludes NULL targtype1_txt values
-- ###############################################################
CREATE OR ALTER VIEW vETLDimTargetType
AS
SELECT DISTINCT
    CAST(targtype1 AS INT) AS targtype1,
    CAST(ISNULL(targtype1_txt, 'Target Type Not Given') AS NVARCHAR(100)) AS TargetTypeLabel
FROM dbo.FactTerrorEvents
WHERE targtype1 IS NOT NULL;
GO

-- ###############################################################
-- Procedure: pETLFillDimTargetType
-- Desc: Inserts unique target types into DimTargetType from vETLDimTargetType
-- Notes: Skips duplicates based on targtype1 code
-- ###############################################################
CREATE OR ALTER PROCEDURE pETLFillDimTargetType
AS
BEGIN
    DECLARE @RC INT = 0;

    BEGIN TRY
        BEGIN TRAN;

        INSERT INTO dbo.DimTargetType (
            targtype1,
            TargetTypeLabel
        )
        SELECT
            t.targtype1,
            t.TargetTypeLabel
        FROM vETLDimTargetType t
        WHERE NOT EXISTS (
            SELECT 1
            FROM dbo.DimTargetType d
            WHERE d.targtype1 = t.targtype1
        );

        EXEC pInsETLLog
            @ETLAction = 'pETLFillDimTargetType',
            @ETLLogMessage = 'DimTargetType filled (duplicates skipped)';

        COMMIT TRAN;
        SET @RC = 1;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;

        DECLARE @ErrorMessage NVARCHAR(1000) = ERROR_MESSAGE();
        EXEC pInsETLLog 
            @ETLAction = 'pETLFillDimTargetType',
            @ETLLogMessage = @ErrorMessage;

        SET @RC = -1;
    END CATCH

    RETURN @RC;
END
GO



-- ###############################################################
-- View: vETLDimWeaponType
-- Desc: Extracts distinct weapon type codes and labels from FactTerrorEvents
-- Notes: Excludes NULL weaptype1_txt values
-- ###############################################################
CREATE OR ALTER VIEW vETLDimWeaponType
AS
SELECT DISTINCT
    CAST(weaptype1 AS INT) AS weaptype1,
    CAST(ISNULL(weaptype1_txt, 'Weapon Type Not Given') AS NVARCHAR(100)) AS WeaponTypeLabel
FROM dbo.FactTerrorEvents
WHERE weaptype1 IS NOT NULL;
GO



-- ###############################################################
-- Procedure: pETLFillDimWeaponType
-- Desc: Inserts unique weapon types from vETLDimWeaponType into DimWeaponType
-- Notes: Skips duplicates based on weaptype1 code
-- ###############################################################
CREATE OR ALTER PROCEDURE pETLFillDimWeaponType
AS
BEGIN
    DECLARE @RC INT = 0;

    BEGIN TRY
        BEGIN TRAN;

        INSERT INTO dbo.DimWeaponType (
            weaptype1,
            WeaponTypeLabel
        )
        SELECT
            w.weaptype1,
            w.WeaponTypeLabel
        FROM vETLDimWeaponType w
        WHERE NOT EXISTS (
            SELECT 1
            FROM dbo.DimWeaponType d
            WHERE d.weaptype1 = w.weaptype1
        );

        EXEC pInsETLLog
            @ETLAction = 'pETLFillDimWeaponType',
            @ETLLogMessage = 'DimWeaponType filled (duplicates skipped)';

        COMMIT TRAN;
        SET @RC = 1;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;

        DECLARE @ErrorMessage NVARCHAR(1000) = ERROR_MESSAGE();
        EXEC pInsETLLog 
            @ETLAction = 'pETLFillDimWeaponType',
            @ETLLogMessage = @ErrorMessage;

        SET @RC = -1;
    END CATCH

    RETURN @RC;
END
GO


--############################################################--
--## View: vETLDimPerpetratorGroup
--## Desc: Extracts distinct non-null perpetrator group names
--## Source: FactTerrorEvents.gname
--## Target: DimPerpetratorGroup
--## Change Log:
--##   2025-07-21, JSharma, Created View
--############################################################--
CREATE OR ALTER VIEW vETLDimPerpetratorGroupWithKey AS
SELECT DISTINCT
    dpg.PerpetratorGroupKey,
    dpg.GroupName
FROM dbo.FactTerrorEvents fte
JOIN dbo.DimPerpetratorGroup dpg
    ON dpg.GroupName = ISNULL(fte.gname, 'Perp Group Not Given');
GO




--############################################################--
--## Stored Procedure: pETLFillDimPerpetratorGroup
--## Desc: Inserts unique perpetrator group names from view
--## Target: DimPerpetratorGroup
--## Change Log:
--##   2025-07-21, JSharma, Created Procedure
--############################################################--
CREATE OR ALTER PROCEDURE [dbo].[pETLFillDimPerpetratorGroup]
AS
BEGIN
    DECLARE @RC INT = 0;

    BEGIN TRY
        IF ((SELECT COUNT(*) FROM dbo.DimPerpetratorGroup) = 0)
        BEGIN
            BEGIN TRAN;

            INSERT INTO dbo.DimPerpetratorGroup (GroupName)
            SELECT GroupName
            FROM dbo.vETLDimPerpetratorGroup;

            COMMIT TRAN;

            EXEC pInsETLLog
                @ETLAction = 'pETLFillDimPerpetratorGroup',
                @ETLLogMessage = 'DimPerpetratorGroup filled';
            SET @RC = +1;
        END
    END TRY

    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRAN;

        DECLARE @ErrorMessage NVARCHAR(1000) = ERROR_MESSAGE();
        EXEC pInsETLLog 
            @ETLAction = 'pETLFillDimPerpetratorGroup',
            @ETLLogMessage = @ErrorMessage;
        SET @RC = -1;
    END CATCH

    RETURN @RC;
END
GO


-- ###############################################################
-- View: vETLDimSuccess
-- Desc: Extracts distinct success flags with readable labels
-- ###############################################################
CREATE OR ALTER VIEW vETLDimSuccess
AS
SELECT DISTINCT
    CAST(success AS INT) AS IsSuccessful,
    CASE 
        WHEN success = 1 THEN 'Successful'
        WHEN success = 0 THEN 'Unsuccessful'
    END AS Description
FROM dbo.FactTerrorEvents;
GO


-- ###############################################################
-- Procedure: pETLFillDimSuccess
-- Desc: Loads DimSuccess with unique success flags and descriptions
-- ###############################################################
CREATE OR ALTER PROCEDURE pETLFillDimSuccess
AS
BEGIN
    DECLARE @RC INT = 0;

    BEGIN TRY
        BEGIN TRAN;

        INSERT INTO dbo.DimSuccess (
            IsSuccessful,
            Description
        )
        SELECT
            s.IsSuccessful,
            s.Description
        FROM vETLDimSuccess s
        WHERE NOT EXISTS (
            SELECT 1
            FROM dbo.DimSuccess d
            WHERE d.IsSuccessful = s.IsSuccessful
        );

        EXEC pInsETLLog
            @ETLAction = 'pETLFillDimSuccess',
            @ETLLogMessage = 'DimSuccess filled (duplicates skipped)';

        COMMIT TRAN;
        SET @RC = 1;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;

        DECLARE @ErrorMessage NVARCHAR(1000) = ERROR_MESSAGE();
        EXEC pInsETLLog 
            @ETLAction = 'pETLFillDimSuccess',
            @ETLLogMessage = @ErrorMessage;

        SET @RC = -1;
    END CATCH

    RETURN @RC;
END
GO

--############################################################--
--## View: vETLDimSuicide
--## Desc: Extracts distinct values for suicide attacks
--## Source: FactTerrorEvents.suicide
--## Target: DimSuicide
--## Notes: Only includes valid suicide values (0 or 1)
--## Change Log:
--##   2025-07-23, JSharma, Rewritten to exclude invalid values
--############################################################--
-- ###############################################################
-- View: vETLDimSuicide
-- Desc: Extracts distinct suicide flags with readable labels
-- ###############################################################
CREATE OR ALTER VIEW vETLDimSuicide AS
SELECT DISTINCT
    CAST(suicide AS INT) AS IsSuicide,
    CASE 
        WHEN suicide = 1 THEN 'Suicide Attack'
        WHEN suicide = 0 THEN 'Not a Suicide Attack'
        ELSE 'Unknown'
    END AS Description
FROM dbo.FactTerrorEvents
WHERE suicide IN (0, 1);
GO



-- ###############################################################
-- Procedure: pETLFillDimSuicide
-- Desc: Loads DimSuicide with suicide flags and descriptions
-- ###############################################################
CREATE OR ALTER PROCEDURE pETLFillDimSuicide
AS
BEGIN
    DECLARE @RC INT = 0;

    BEGIN TRY
        BEGIN TRAN;

        INSERT INTO dbo.DimSuicide (
            IsSuicide,
            Description
        )
        SELECT
            s.IsSuicide,
            s.Description
        FROM vETLDimSuicide s
        WHERE NOT EXISTS (
            SELECT 1
            FROM dbo.DimSuicide d
            WHERE d.IsSuicide = s.IsSuicide
        );

        EXEC pInsETLLog
            @ETLAction = 'pETLFillDimSuicide',
            @ETLLogMessage = 'DimSuicide filled (duplicates skipped)';

        COMMIT TRAN;
        SET @RC = 1;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;

        DECLARE @ErrorMessage NVARCHAR(1000) = ERROR_MESSAGE();
        EXEC pInsETLLog 
            @ETLAction = 'pETLFillDimSuicide',
            @ETLLogMessage = @ErrorMessage;

        SET @RC = -1;
    END CATCH

    RETURN @RC;
END
GO

-- ###############################################################
-- View: vETLDimPropertyDamage
-- Desc: Extracts distinct property damage codes with human-readable descriptions
-- ###############################################################
CREATE OR ALTER VIEW vETLDimPropertyDamage
AS
SELECT DISTINCT
    CAST(property AS SMALLINT) AS PropertyDamageCode,
    CASE 
        WHEN property = 1 THEN 'Property Damaged'
        WHEN property = 0 THEN 'No Property Damage'
        WHEN property = -9 THEN 'No Data'
    END AS Description
FROM dbo.FactTerrorEvents;
GO


-- ###############################################################
-- Procedure: pETLFillDimPropertyDamage
-- Desc: Loads DimPropertyDamage from vETLDimPropertyDamage
-- ###############################################################
CREATE OR ALTER PROCEDURE pETLFillDimPropertyDamage
AS
BEGIN
    DECLARE @RC INT = 0;

    BEGIN TRY
        BEGIN TRAN;

        INSERT INTO dbo.DimPropertyDamage (
            PropertyDamageCode,
            Description
        )
        SELECT
            p.PropertyDamageCode,
            p.Description
        FROM vETLDimPropertyDamage p
        WHERE NOT EXISTS (
            SELECT 1
            FROM dbo.DimPropertyDamage d
            WHERE d.PropertyDamageCode = p.PropertyDamageCode
        );

        EXEC pInsETLLog
            @ETLAction = 'pETLFillDimPropertyDamage',
            @ETLLogMessage = 'DimPropertyDamage filled (duplicates skipped)';

        COMMIT TRAN;
        SET @RC = 1;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;

        DECLARE @ErrorMessage NVARCHAR(1000) = ERROR_MESSAGE();
        EXEC pInsETLLog 
            @ETLAction = 'pETLFillDimPropertyDamage',
            @ETLLogMessage = @ErrorMessage;

        SET @RC = -1;
    END CATCH

    RETURN @RC;
END
GO



--####################################################################--
--## View: vETLDimHostageSituation
--## Desc: Extracts distinct hostage situation codes from FactTerrorEvents
--## Maps values: 1 = Hostage, 0 = No Hostage, -9 = Unknown, others = Other
--## Change Log:
--##   2025-07-23, JSharma, Recreated after table schema update
--####################################################################--
CREATE OR ALTER VIEW vETLDimHostageSituation
AS
SELECT DISTINCT
    CAST(
        CASE 
            WHEN ishostkid IS NULL THEN 2
            ELSE ishostkid
        END AS SMALLINT
    ) AS HostageSituationCode,
    CASE 
        WHEN ishostkid = 1 THEN 'Hostage Situation'
        WHEN ishostkid = 0 THEN 'No Hostage Situation'
        WHEN ishostkid = -9 THEN 'Unknown'
        WHEN ishostkid IS NULL THEN 'No Data Given'
        ELSE 'Invalid Code'
    END AS Description
FROM dbo.FactTerrorEvents;
GO


--####################################################################--
--## Procedure: pETLFillDimHostageSituation
--## Desc: Loads DimHostageSituation from vETLDimHostageSituation
--## Notes:
--##   - Inserts only if table is empty
--##   - Uses SMALLINT to support full value range
--##   - Logs success or error to ETL log
--## Change Log:
--##   2025-07-23, JSharma, Rewritten to match updated schema
--####################################################################--
CREATE OR ALTER PROCEDURE pETLFillDimHostageSituation
AS
BEGIN
    DECLARE @RC INT = 0;

    BEGIN TRY
        BEGIN TRAN;

        INSERT INTO dbo.DimHostageSituation (
            HostageSituationCode,
            Description
        )
        SELECT
            h.HostageSituationCode,
            h.Description
        FROM vETLDimHostageSituation h
        WHERE NOT EXISTS (
            SELECT 1
            FROM dbo.DimHostageSituation d
            WHERE d.HostageSituationCode = h.HostageSituationCode
        );

        EXEC pInsETLLog
            @ETLAction = 'pETLFillDimHostageSituation',
            @ETLLogMessage = 'DimHostageSituation filled (duplicates skipped)';

        COMMIT TRAN;
        SET @RC = 1;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;

        DECLARE @ErrorMessage NVARCHAR(1000) = ERROR_MESSAGE();
        EXEC pInsETLLog 
            @ETLAction = 'pETLFillDimHostageSituation',
            @ETLLogMessage = @ErrorMessage;

        SET @RC = -1;
    END CATCH

    RETURN @RC;
END
GO

-- ###############################################################
-- View: vETLDimDoubtTerrorism
-- Desc: Extracts unique doubtterr codes and maps -9 to 'Unknown'
-- ###############################################################
CREATE OR ALTER VIEW vETLDimDoubtTerrorism
AS
SELECT DISTINCT
    CAST(doubtterr AS SMALLINT) AS DoubtTerrorismCode,
    CASE 
        WHEN doubtterr = 1 THEN 'Doubt Terrorism'
        WHEN doubtterr = 0 THEN 'No Doubt'
        WHEN doubtterr = -9 THEN 'Unknown'
    END AS Description
FROM dbo.FactTerrorEvents;
GO


-- ###############################################################
-- Procedure: pETLFillDimDoubtTerrorism
-- Desc: Loads DimDoubtTerrorism from vETLDimDoubtTerrorism
-- ###############################################################
CREATE OR ALTER PROCEDURE pETLFillDimDoubtTerrorism
AS
BEGIN
    DECLARE @RC INT = 0;

    BEGIN TRY
        BEGIN TRAN;

        INSERT INTO dbo.DimDoubtTerrorism (
            DoubtTerrorismCode,
            Description
        )
        SELECT
            d.DoubtTerrorismCode,
            d.Description
        FROM vETLDimDoubtTerrorism d
        WHERE NOT EXISTS (
            SELECT 1
            FROM dbo.DimDoubtTerrorism x
            WHERE x.DoubtTerrorismCode = d.DoubtTerrorismCode
        );

        EXEC pInsETLLog
            @ETLAction = 'pETLFillDimDoubtTerrorism',
            @ETLLogMessage = 'DimDoubtTerrorism filled (duplicates skipped)';

        COMMIT TRAN;
        SET @RC = 1;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;

        DECLARE @ErrorMessage NVARCHAR(1000) = ERROR_MESSAGE();
        EXEC pInsETLLog 
            @ETLAction = 'pETLFillDimDoubtTerrorism',
            @ETLLogMessage = @ErrorMessage;

        SET @RC = -1;
    END CATCH

    RETURN @RC;
END
GO



-- ###############################################################
-- View: vETLDimEventType
-- Desc: Maps `multiple` flag to readable event type description
-- ###############################################################
CREATE OR ALTER VIEW vETLDimEventType AS
SELECT DISTINCT
    CAST(multiple AS INT) AS IsMultiple,
    CASE 
        WHEN multiple = 1 THEN 'Multiple Incident'
        WHEN multiple = 0 THEN 'Single Incident'
        WHEN multiple = -1 THEN 'Unknown'
    END AS Description
FROM dbo.FactTerrorEvents;
GO




--####################################################################--
--## Procedure: pETLFillDimEventType
--## Desc: Loads DimEventType from vETLDimEventType
--## Notes:
--##   - Inserts only if table is empty
--##   - Uses BIT for binary classification
--##   - Logs results to ETL log
--## Change Log:
--##   2025-07-23, JSharma, Created
--####################################################################--
-- ###############################################################
-- Procedure: pETLFillDimEventType
-- Desc: Inserts distinct event type flags and labels into DimEventType
-- ###############################################################
CREATE OR ALTER PROCEDURE pETLFillDimEventType
AS
BEGIN
    DECLARE @RC INT = 0;

    BEGIN TRY
        BEGIN TRAN;

        INSERT INTO dbo.DimEventType (
            IsMultiple,
            Description
        )
        SELECT
            e.IsMultiple,
            e.Description
        FROM vETLDimEventType e
        WHERE NOT EXISTS (
            SELECT 1
            FROM dbo.DimEventType d
            WHERE d.IsMultiple = e.IsMultiple
        );

        EXEC pInsETLLog
            @ETLAction = 'pETLFillDimEventType',
            @ETLLogMessage = 'DimEventType filled (duplicates skipped)';

        COMMIT TRAN;
        SET @RC = 1;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;

        DECLARE @ErrorMessage NVARCHAR(1000) = ERROR_MESSAGE();
        EXEC pInsETLLog 
            @ETLAction = 'pETLFillDimEventType',
            @ETLLogMessage = @ErrorMessage;

        SET @RC = -1;
    END CATCH

    RETURN @RC;
END
GO

--- ###############################################################
-- View: vETLDimExtendedIncident
-- Desc: Maps 'extended' flag to human-readable description
-- ###############################################################
CREATE OR ALTER VIEW vETLDimExtendedIncident
AS
SELECT DISTINCT
    CAST(extended AS int) AS IsExtended,
    CASE 
        WHEN extended = 1 THEN 'Extended Incident'
        WHEN extended = 0 THEN 'Not Extended'
    END AS Description
FROM dbo.FactTerrorEvents;
GO

-- ###############################################################
-- Procedure: pETLFillDimExtendedIncident
-- Desc: Inserts extended incident flags into DimExtendedIncident
-- ###############################################################
CREATE OR ALTER PROCEDURE pETLFillDimExtendedIncident
AS
BEGIN
    DECLARE @RC INT = 0;

    BEGIN TRY
        BEGIN TRAN;

        INSERT INTO dbo.DimExtendedIncident (
            IsExtended,
            Description
        )
        SELECT
            e.IsExtended,
            e.Description
        FROM vETLDimExtendedIncident e
        WHERE NOT EXISTS (
            SELECT 1
            FROM dbo.DimExtendedIncident d
            WHERE d.IsExtended = e.IsExtended
        );

        EXEC pInsETLLog
            @ETLAction = 'pETLFillDimExtendedIncident',
            @ETLLogMessage = 'DimExtendedIncident filled (duplicates skipped)';

        COMMIT TRAN;
        SET @RC = 1;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;

        DECLARE @ErrorMessage NVARCHAR(1000) = ERROR_MESSAGE();
        EXEC pInsETLLog 
            @ETLAction = 'pETLFillDimExtendedIncident',
            @ETLLogMessage = @ErrorMessage;

        SET @RC = -1;
    END CATCH

    RETURN @RC;
END
GO



-- ###############################################################
-- View: vETLDimWeaponSubType
-- Desc: Maps weapsubtype1 to readable labels, assigns 0 = No Data
-- ###############################################################
CREATE OR ALTER VIEW vETLDimWeaponSubType
AS
SELECT DISTINCT
    CAST(ISNULL(weapsubtype1, 0) AS INT) AS weapsubtype1,
    CAST(ISNULL(weapsubtype1_txt, 'No Data for Weapon Subtype') AS NVARCHAR(100)) AS WeaponSubTypeLabel
FROM dbo.FactTerrorEvents;
GO


-- ###############################################################
-- Procedure: pETLFillDimWeaponSubType
-- Desc: Inserts weapon subtypes into DimWeaponSubType
-- ###############################################################
CREATE OR ALTER PROCEDURE pETLFillDimWeaponSubType
AS
BEGIN
    DECLARE @RC INT = 0;

    BEGIN TRY
        BEGIN TRAN;

        INSERT INTO dbo.DimWeaponSubType (
            weapsubtype1,
            WeaponSubTypeLabel
        )
        SELECT
            w.weapsubtype1,
            w.WeaponSubTypeLabel
        FROM vETLDimWeaponSubType w
        WHERE NOT EXISTS (
            SELECT 1
            FROM dbo.DimWeaponSubType d
            WHERE d.weapsubtype1 = w.weapsubtype1
        );

        EXEC pInsETLLog
            @ETLAction = 'pETLFillDimWeaponSubType',
            @ETLLogMessage = 'DimWeaponSubType filled (duplicates skipped)';

        COMMIT TRAN;
        SET @RC = 1;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;

        DECLARE @ErrorMessage NVARCHAR(1000) = ERROR_MESSAGE();
        EXEC pInsETLLog 
            @ETLAction = 'pETLFillDimWeaponSubType',
            @ETLLogMessage = @ErrorMessage;

        SET @RC = -1;
    END CATCH

    RETURN @RC;
END
GO


-- ###############################################################
-- View: vETLDimTargetSubType
-- Desc: Maps targsubtype1 to readable labels, assigns 0 = No Data
-- ###############################################################
CREATE OR ALTER VIEW vETLDimTargetSubType
AS
SELECT DISTINCT
    CAST(ISNULL(targsubtype1, 0) AS INT) AS targsubtype1,
    CAST(ISNULL(targsubtype1_txt, 'No Data for Target Subtype') AS NVARCHAR(100)) AS TargetSubTypeLabel
FROM dbo.FactTerrorEvents;
GO



-- ###############################################################
-- Procedure: pETLFillDimTargetSubType
-- Desc: Inserts target subtypes into DimTargetSubType
-- ###############################################################
CREATE OR ALTER PROCEDURE pETLFillDimTargetSubType
AS
BEGIN
    DECLARE @RC INT = 0;

    BEGIN TRY
        BEGIN TRAN;

        INSERT INTO dbo.DimTargetSubType (
            targsubtype1,
            TargetSubTypeLabel
        )
        SELECT
            t.targsubtype1,
            t.TargetSubTypeLabel
        FROM vETLDimTargetSubType t
        WHERE NOT EXISTS (
            SELECT 1
            FROM dbo.DimTargetSubType d
            WHERE d.targsubtype1 = t.targsubtype1
        );

        EXEC pInsETLLog
            @ETLAction = 'pETLFillDimTargetSubType',
            @ETLLogMessage = 'DimTargetSubType filled (duplicates skipped)';

        COMMIT TRAN;
        SET @RC = 1;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;

        DECLARE @ErrorMessage NVARCHAR(1000) = ERROR_MESSAGE();
        EXEC pInsETLLog 
            @ETLAction = 'pETLFillDimTargetSubType',
            @ETLLogMessage = @ErrorMessage;

        SET @RC = -1;
    END CATCH

    RETURN @RC;
END
GO


--=========================================
-- View: vETLDimAttackSuccessType
-- Desc: Extracts unique success codes and labels
CREATE OR ALTER VIEW [dbo].[vETLDimAttackSuccessType] AS
SELECT DISTINCT
    CAST(success AS INT) AS SuccessCode,
    CASE success
        WHEN 1 THEN 'Attack Successful'
        WHEN 0 THEN 'Attack Failed'
        ELSE 'Unknown'
    END AS SuccessLabel
FROM dbo.FactTerrorEvents
WHERE success IN (0, 1);
GO

--=========================================
-- Procedure: pETLFillDimAttackSuccessType
-- Desc: Fills DimAttackSuccessType from view
--=========================================
CREATE OR ALTER PROCEDURE [dbo].[pETLFillDimAttackSuccessType]
AS
BEGIN
    DECLARE @RC INT = 0;

    BEGIN TRY
        IF ((SELECT COUNT(*) FROM dbo.DimAttackSuccessType) = 0)
        BEGIN
            BEGIN TRAN;

            INSERT INTO dbo.DimAttackSuccessType (SuccessCode, SuccessLabel)
            SELECT SuccessCode, SuccessLabel
            FROM dbo.vETLDimAttackSuccessType;

            COMMIT TRAN;

            EXEC pInsETLLog
                @ETLAction = 'pETLFillDimAttackSuccessType',
                @ETLLogMessage = 'DimAttackSuccessType filled';
            SET @RC = 1;
        END
    END TRY

    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRAN;

        DECLARE @ErrorMessage NVARCHAR(1000) = ERROR_MESSAGE();
        EXEC pInsETLLog 
            @ETLAction = 'pETLFillDimAttackSuccessType',
            @ETLLogMessage = @ErrorMessage;
        SET @RC = -1;
    END CATCH

    RETURN @RC;
END
GO




--####################################################################--
--## View: vETLDimLocation
--## Desc: Extracts distinct location combinations from FactTerrorEvents
--## Includes country, region, city, province, lat/long
--## Excludes rows missing required location fields
--## Change Log:
--##   2025-07-24, JSharma, Created
--####################################################################--
CREATE OR ALTER VIEW vETLDimLocationWithKey AS
SELECT DISTINCT
    dloc.LocationKey,
    dloc.country,
    dloc.region,
    dloc.City,
    dloc.Province,
    dloc.Latitude,
    dloc.Longitude
FROM dbo.FactTerrorEvents fte
JOIN dbo.DimLocation dloc
    ON dloc.country = fte.country
   AND dloc.region = fte.region
   AND dloc.City = fte.city
   AND dloc.Province = fte.provstate
   AND ISNULL(dloc.Latitude, 0) = ISNULL(fte.latitude, 0)
   AND ISNULL(dloc.Longitude, 0) = ISNULL(fte.longitude, 0);
GO


--####################################################################--
--## Procedure: pETLFillDimLocation
--## Desc: Loads DimLocation from vETLDimLocation
--## Notes:
--##   - Inserts only if table is empty
--##   - Latitude and Longitude are optional
--##   - Logs success/failure to ETL log
--## Change Log:
--##   2025-07-24, JSharma, Created
--####################################################################--
CREATE OR ALTER PROCEDURE [dbo].[pETLFillDimLocation]
AS
BEGIN
    DECLARE @RC INT = 0;

    BEGIN TRY
        IF ((SELECT COUNT(*) FROM dbo.DimLocation) = 0)
        BEGIN TRAN

            INSERT INTO dbo.DimLocation (country, region, City, Province, Latitude, Longitude)
            SELECT country, region, City, Province, Latitude, Longitude
            FROM dbo.vETLDimLocation;

        COMMIT TRAN;

        EXEC pInsETLLog
            @ETLAction = 'pETLFillDimLocation',
            @ETLLogMessage = 'DimLocation loaded';
        SET @RC = 1;
    END TRY

    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRAN;

        DECLARE @ErrorMessage NVARCHAR(1000) = ERROR_MESSAGE();
        EXEC pInsETLLog 
            @ETLAction = 'pETLFillDimLocation',
            @ETLLogMessage = @ErrorMessage;
        SET @RC = -1;
    END CATCH

    RETURN @RC;
END
GO




--####################################################################--
--## View: vETLFactTerrorEvents
--## Desc: Joins Fact_Terror_Events with all dimension tables using correct column names
--## Change Log:
--##   2025-07-24, JSharma, Final version using table definitions
--####################################################################--
CREATE OR ALTER VIEW vETLFactTerrorEvents AS
WITH RankedEvents AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY eventid ORDER BY eventid) AS rn
    FROM dbo.FactTerrorEvents
),
UniqueLocation AS (
    SELECT country, region, City, MIN(LocationKey) AS LocationKey
    FROM dbo.DimLocation
    GROUP BY country, region, City
),
UniqueEventType AS (
    SELECT IsMultiple, MIN(EventTypeKey) AS EventTypeKey
    FROM dbo.DimEventType
    GROUP BY IsMultiple
)
SELECT
    fte.eventid,
    dd.DateKey,
    loc.LocationKey,
    c.CountryKey,
	c.CountryName AS CountryName,
	fte.country AS GTDCountryCode,
    atk.AttackTypeKey,
    tgt.TargetTypeKey,
    sub.TargetSubTypeKey,
    weap.WeaponTypeKey,
    weapsub.WeaponSubTypeKey,
    perp.PerpetratorGroupKey,
    s.SuccessKey,
    sui.SuicideKey,
    prop.PropertyDamageKey,
    h.HostageSituationKey,
    dtr.DoubtTerrorismKey,
    et.EventTypeKey,
    ext.ExtendedIncidentKey,
    axt.AttackSuccessTypeKey,
    fte.num_killed,
    fte.num_wounded,
    fte.num_us_killed,
    fte.num_us_wounded,
    fte.num_terrorists_killed,
    fte.num_terrorists_wounded
FROM RankedEvents fte
LEFT JOIN dbo.DimDate dd 
    ON FORMAT(DATEFROMPARTS(fte.year, fte.month, fte.day), 'yyyyMMdd') = dd.DateKey
JOIN UniqueLocation loc 
    ON loc.country = fte.country AND loc.region = fte.region AND loc.City = fte.city
LEFT JOIN dbo.DimCountry c 
    ON c.GTDCountryCode = fte.country
LEFT JOIN dbo.DimAttackType atk 
    ON atk.attacktype1 = fte.attacktype1
LEFT JOIN dbo.DimTargetType tgt 
    ON tgt.targtype1 = fte.targtype1
LEFT JOIN dbo.DimTargetSubType sub 
    ON sub.targsubtype1 = ISNULL(fte.targsubtype1, 0)
LEFT JOIN dbo.DimWeaponType weap 
    ON weap.weaptype1 = fte.weaptype1
LEFT JOIN dbo.DimWeaponSubType weapsub 
    ON weapsub.weapsubtype1 = ISNULL(fte.weapsubtype1, 0)
LEFT JOIN dbo.DimPerpetratorGroup perp 
    ON perp.GroupName = ISNULL(fte.gname, 'Perp Group Not Given')
LEFT JOIN dbo.DimSuccess s 
    ON s.IsSuccessful = fte.success
LEFT JOIN dbo.DimSuicide sui 
    ON sui.IsSuicide = fte.suicide
LEFT JOIN dbo.DimPropertyDamage prop 
    ON prop.PropertyDamageCode = fte.property
LEFT JOIN dbo.DimHostageSituation h 
    ON h.HostageSituationCode = ISNULL(fte.ishostkid, 2)
LEFT JOIN dbo.DimDoubtTerrorism dtr 
    ON dtr.DoubtTerrorismCode = fte.doubtterr
LEFT JOIN UniqueEventType et 
    ON et.IsMultiple = fte.multiple
LEFT JOIN dbo.DimExtendedIncident ext 
    ON ext.IsExtended = fte.extended
LEFT JOIN dbo.DimAttackSuccessType axt 
    ON axt.SuccessCode = fte.success
WHERE fte.rn = 1
  AND ISDATE(CONCAT(fte.year, '-', fte.month, '-', fte.day)) = 1;
GO




--####################################################################--
--## Procedure: pETLLoadFactTerrorEvents
--## Desc: Loads mapped data from raw GTD table into Fact_Terror_Events
--## Assumes: All dimension tables are pre-populated with GTD code mappings
--## Change Log:
--##   2025-07-24, JSharma, Final insert with mapping joins
--####################################################################--
CREATE OR ALTER PROCEDURE pETLFillFactTerrorEvents AS
BEGIN
    DECLARE @RC INT = 0;

    BEGIN TRY
        BEGIN TRAN;

        INSERT INTO dbo.Fact_Terror_Events (
            EventID,
            DateKey,
            LocationKey,
            CountryKey,
            AttackTypeKey,
            TargetTypeKey,
            TargetSubTypeKey,
            WeaponTypeKey,
            WeaponSubTypeKey,
            PerpetratorGroupKey,
            SuccessKey,
            SuicideKey,
            PropertyDamageKey,
            HostageSituationKey,
            DoubtTerrorismKey,
            EventTypeKey,
            ExtendedIncidentKey,
            AttackSuccessTypeKey,
            NumKilled,
            NumWounded,
            NumUSKilled,
            NumUSWounded,
            NumTerroristsKilled,
            NumTerroristsWounded
        )
        SELECT 
            eventid,
            DateKey,
            LocationKey,
            CountryKey,
            AttackTypeKey,
            TargetTypeKey,
            TargetSubTypeKey,
            WeaponTypeKey,
            WeaponSubTypeKey,
            PerpetratorGroupKey,
            SuccessKey,
            SuicideKey,
            PropertyDamageKey,
            HostageSituationKey,
            DoubtTerrorismKey,
            EventTypeKey,
            ExtendedIncidentKey,
            AttackSuccessTypeKey,
            num_killed,
            num_wounded,
            num_us_killed,
            num_us_wounded,
            num_terrorists_killed,
            num_terrorists_wounded
        FROM vETLFactTerrorEvents v
        WHERE NOT EXISTS (
            SELECT 1 FROM dbo.Fact_Terror_Events f
            WHERE f.EventID = v.eventid
        );

        COMMIT TRAN;

        EXEC pInsETLLog
            @ETLAction = 'pETLFillFactTerrorEvents',
            @ETLLogMessage = 'Fact_Terror_Events inserted with FK mapping';
        SET @RC = 1;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;
        DECLARE @ErrorMessage NVARCHAR(1000) = ERROR_MESSAGE();
        EXEC pInsETLLog 
            @ETLAction = 'pETLFillFactTerrorEvents',
            @ETLLogMessage = @ErrorMessage;
        SET @RC = -1;
    END CATCH

    RETURN @RC;
END
GO



--####################################################################--
--## Procedure: pETLAddForeignKeys
--## Desc: Adds foreign key constraints back to Fact_Terror_Events
--## Notes:
--##   - Matches original table constraint names
--##   - Assumes dimension tables already exist
--##   - Logs result to ETL log
--## Change Log:
--##   2025-07-24, JSharma, Created
--####################################################################--
CREATE OR ALTER PROCEDURE [dbo].[pETLAddForeignKeys]
AS
BEGIN
    DECLARE @RC INT = 0;

    BEGIN TRY
        ALTER TABLE dbo.Fact_Terror_Events ADD CONSTRAINT FK_Fact_Date
            FOREIGN KEY (DateKey) REFERENCES dbo.DimDate(DateKey);

		ALTER TABLE dbo.Fact_Terror_Events ADD CONSTRAINT FK_Fact_Country
			FOREIGN KEY (CountryKey) REFERENCES dbo.DimCountry(CountryKey);

        ALTER TABLE dbo.Fact_Terror_Events ADD CONSTRAINT FK_Fact_Location
            FOREIGN KEY (LocationKey) REFERENCES dbo.DimLocation(LocationKey);

        ALTER TABLE dbo.Fact_Terror_Events ADD CONSTRAINT FK_Fact_AttackType
            FOREIGN KEY (AttackTypeKey) REFERENCES dbo.DimAttackType(AttackTypeKey);

        ALTER TABLE dbo.Fact_Terror_Events ADD CONSTRAINT FK_Fact_TargetType
            FOREIGN KEY (TargetTypeKey) REFERENCES dbo.DimTargetType(TargetTypeKey);

        ALTER TABLE dbo.Fact_Terror_Events ADD CONSTRAINT FK_Fact_TargetSubType
            FOREIGN KEY (TargetSubTypeKey) REFERENCES dbo.DimTargetSubType(TargetSubTypeKey);

        ALTER TABLE dbo.Fact_Terror_Events ADD CONSTRAINT FK_Fact_WeaponType
            FOREIGN KEY (WeaponTypeKey) REFERENCES dbo.DimWeaponType(WeaponTypeKey);

        ALTER TABLE dbo.Fact_Terror_Events ADD CONSTRAINT FK_Fact_WeaponSubType
            FOREIGN KEY (WeaponSubTypeKey) REFERENCES dbo.DimWeaponSubType(WeaponSubTypeKey);

        ALTER TABLE dbo.Fact_Terror_Events ADD CONSTRAINT FK_Fact_Perpetrator
            FOREIGN KEY (PerpetratorGroupKey) REFERENCES dbo.DimPerpetratorGroup(PerpetratorGroupKey);

        ALTER TABLE dbo.Fact_Terror_Events ADD CONSTRAINT FK_Fact_Success
            FOREIGN KEY (SuccessKey) REFERENCES dbo.DimSuccess(SuccessKey);

        ALTER TABLE dbo.Fact_Terror_Events ADD CONSTRAINT FK_Fact_Suicide
            FOREIGN KEY (SuicideKey) REFERENCES dbo.DimSuicide(SuicideKey);

        ALTER TABLE dbo.Fact_Terror_Events ADD CONSTRAINT FK_Fact_PropertyDamage
            FOREIGN KEY (PropertyDamageKey) REFERENCES dbo.DimPropertyDamage(PropertyDamageKey);

        ALTER TABLE dbo.Fact_Terror_Events ADD CONSTRAINT FK_Fact_HostageSituation
            FOREIGN KEY (HostageSituationKey) REFERENCES dbo.DimHostageSituation(HostageSituationKey);

        ALTER TABLE dbo.Fact_Terror_Events ADD CONSTRAINT FK_Fact_DoubtTerrorism
            FOREIGN KEY (DoubtTerrorismKey) REFERENCES dbo.DimDoubtTerrorism(DoubtTerrorismKey);

        ALTER TABLE dbo.Fact_Terror_Events ADD CONSTRAINT FK_Fact_EventType
            FOREIGN KEY (EventTypeKey) REFERENCES dbo.DimEventType(EventTypeKey);

        ALTER TABLE dbo.Fact_Terror_Events ADD CONSTRAINT FK_Fact_ExtendedIncident
            FOREIGN KEY (ExtendedIncidentKey) REFERENCES dbo.DimExtendedIncident(ExtendedIncidentKey);

        ALTER TABLE dbo.Fact_Terror_Events ADD CONSTRAINT FK_Fact_AttackSuccessType
            FOREIGN KEY (AttackSuccessTypeKey) REFERENCES dbo.DimAttackSuccessType(AttackSuccessTypeKey);

        EXEC pInsETLLog
            @ETLAction = 'pETLAddForeignKeys',
            @ETLLogMessage = 'Foreign keys successfully added to Fact_Terror_Events';
        SET @RC = 1;
    END TRY

    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;

        DECLARE @ErrorMessage NVARCHAR(1000) = ERROR_MESSAGE();
        EXEC pInsETLLog 
            @ETLAction = 'pETLAddForeignKeys',
            @ETLLogMessage = @ErrorMessage;
        SET @RC = -1;
    END CATCH

    RETURN @RC;
END
GO


DECLARE @Status INT;

EXEC @Status = [dbo].[pETLDropForeignKeyConstraints];
SELECT 'pETLDropForeignKeyConstraints' AS [Step], @Status AS [Status];

EXEC @Status = [dbo].[pETLTruncateTables];
SELECT 'pETLTruncateTables' AS [Step], @Status AS [Status];

EXEC @Status = [dbo].[pETLFillDimDate];
SELECT 'pETLFillDimDate' AS [Step], @Status AS [Status];

EXEC @Status = [dbo].[pETLFillDimCountry];
SELECT 'pETLFillDimCountry' AS [Step], @Status AS [Status];

EXEC @Status = [dbo].[pETLFillDimRegion];
SELECT 'pETLFillDimRegion' AS [Step], @Status AS [Status];

EXEC @Status = [dbo].[pETLFillDimAttackType];
SELECT 'pETLFillDimAttackType' AS [Step], @Status AS [Status];

EXEC @Status = [dbo].[pETLFillDimTargetType];
SELECT 'pETLFillDimTargetType' AS [Step], @Status AS [Status];

EXEC @Status = [dbo].[pETLFillDimWeaponType];
SELECT 'pETLFillDimWeaponType' AS [Step], @Status AS [Status];

EXEC @Status = [dbo].[pETLFillDimPerpetratorGroup];
SELECT 'pETLFillDimPerpetratorGroup' AS [Step], @Status AS [Status];

EXEC @Status = [dbo].[pETLFillDimSuccess];
SELECT 'pETLFillDimSuccess' AS [Step], @Status AS [Status];

EXEC @Status = [dbo].[pETLFillDimSuicide];
SELECT 'pETLFillDimSuicide' AS [Step], @Status AS [Status];

EXEC @Status = [dbo].[pETLFillDimPropertyDamage];
SELECT 'pETLFillDimPropertyDamage' AS [Step], @Status AS [Status];

EXEC @Status = [dbo].[pETLFillDimHostageSituation];
SELECT 'pETLFillDimHostageSituation' AS [Step], @Status AS [Status];

EXEC @Status = [dbo].[pETLFillDimDoubtTerrorism];
SELECT 'pETLFillDimDoubtTerrorism' AS [Step], @Status AS [Status];

EXEC @Status = [dbo].[pETLFillDimEventType];
SELECT 'pETLFillDimEventType' AS [Step], @Status AS [Status];

EXEC @Status = [dbo].[pETLFillDimExtendedIncident];
SELECT 'pETLFillDimExtendedIncident' AS [Step], @Status AS [Status];

EXEC @Status = [dbo].[pETLFillDimWeaponSubType];
SELECT 'pETLFillDimWeaponSubType' AS [Step], @Status AS [Status];

EXEC @Status = [dbo].[pETLFillDimTargetSubType];
SELECT 'pETLFillDimTargetSubType' AS [Step], @Status AS [Status];

EXEC @Status = [dbo].[pETLFillDimAttackSuccessType];
SELECT 'pETLFillDimAttackSuccessType' AS [Step], @Status AS [Status];

EXEC @Status = [dbo].[pETLFillDimLocation];
SELECT 'pETLFillDimLocation' AS [Step], @Status AS [Status];


EXEC @Status = [dbo].[pETLFillFactTerrorEvents];
SELECT '[pETLFillFactTerrorEvents]' AS [Step], @Status AS [Status];

EXEC @Status = [dbo].[pETLAddForeignKeys];
SELECT 'pETLAddForeignKeys' AS [Step], @Status AS [Status];



SELECT * FROM [dbo].[vETLLog];


-- SELECT TOP 10 * FROM dbo.vETLDimDate;
-- SELECT TOP 10 * FROM dbo.vETLDimCountry;
-- SELECT TOP 10 * FROM dbo.vETLDimRegion;
-- SELECT TOP 10 * FROM dbo.vETLDimAttackType;
-- SELECT TOP 10 * FROM dbo.vETLDimTargetType;
-- SELECT TOP 10 * FROM dbo.vETLDimWeaponType;
-- Select Top 10 * From dbo.vETLDimPerpetratorGroup;
-- Select Top 10 * From dbo.vETLDimSuccess;
-- Select Top 10 * From dbo.vETLDimSuicide;
-- Select Top 10 * From dbo.vETLDimPropertyDamage;
-- Select Top 10 * From dbo.vETLDimHostageSituation;
-- Select Top 10 * From dbo.vETLDimDoubtTerrorism;
-- Select Top 10 * From dbo.vETLDimEventType;
-- Select Top 10 * From dbo.vETLDimExtendedIncident;
-- Select Top 10 * From dbo.vETLDimWeaponSubType;
-- Select Top 10 * From dbo.vETLDimTargetSubType;
-- Select Top 10 * From dbo.vETLDimAttackSuccessType;
-- Select Top 10 * From dbo.vETLDimLocation;
-- Select Top 10 * From dbo.vETLFactTerrorEvents;

