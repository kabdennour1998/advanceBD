USE Ontario511DB;
GO

DECLARE @RootPath nvarchar(400) = N'C:\Ontario511Data';
DECLARE @File_Evt nvarchar(400) = @RootPath + N'\evenements_ontario_511.csv';

TRUNCATE TABLE dbo.Staging_Evenements;

DECLARE @sql nvarchar(max) =
N'BULK INSERT dbo.Staging_Evenements
   FROM N''' + REPLACE(@File_Evt,'''','''''') + N'''
   WITH (
     FORMAT = ''CSV'',
     FIRSTROW = 2,
     FIELDQUOTE = ''"'',
     ROWTERMINATOR = ''0x0d0a'',   -- if 0 rows, try 0x0a
     CODEPAGE = ''65001'',
     TABLOCK
   );';
EXEC (@sql);

SELECT COUNT(*) AS Evenements_StagingCount FROM dbo.Staging_Evenements;

EXEC dbo.Import_Evenements;
SELECT COUNT(*) AS Evenements_TargetCount FROM dbo.Evenements;
SELECT TOP 20 * FROM dbo.Evenements ORDER BY EvenementId DESC;

USE Ontario511DB;
GO
ALTER TABLE dbo.Evenements
ALTER COLUMN Description NVARCHAR(MAX) NULL;
GO

-- re-run the import
EXEC dbo.Import_Evenements;


USE Ontario511DB;
GO
ALTER TABLE dbo.HistoriqueEvenements
ALTER COLUMN Description NVARCHAR(MAX) NULL;
GO

-- re-run the import that failed
EXEC dbo.Import_Evenements;



USE Ontario511DB;
GO

DECLARE @RootPath nvarchar(400) = N'C:\Ontario511Data';
DECLARE @File_Rc nvarchar(400) = @RootPath + N'\roadconditions_ontario_511.csv';

TRUNCATE TABLE dbo.Staging_RoadConditions;

DECLARE @sql nvarchar(max) =
N'BULK INSERT dbo.Staging_RoadConditions
   FROM N''' + REPLACE(@File_Rc,'''','''''') + N'''
   WITH (
     FIRSTROW = 2,
     FIELDTERMINATOR = '','',
     ROWTERMINATOR = ''0x0d0a'',   -- if 0 rows, try 0x0a
     CODEPAGE = ''65001'',
     TABLOCK
   );';
EXEC (@sql);

SELECT COUNT(*) AS RoadConditions_StagingCount FROM dbo.Staging_RoadConditions;

EXEC dbo.Import_RoadConditions;
SELECT COUNT(*) AS RoadConditions_TargetCount FROM dbo.RoadConditions;
SELECT TOP 20 * FROM dbo.RoadConditions ORDER BY ConditionId DESC;





USE Ontario511DB;
GO

DECLARE @RootPath nvarchar(400) = N'C:\Ontario511Data';
DECLARE @File_Evt nvarchar(400) = @RootPath + N'\evenements_ontario_511.csv';

TRUNCATE TABLE dbo.Staging_Evenements;

DECLARE @sql nvarchar(max) =
N'BULK INSERT dbo.Staging_Evenements
   FROM N''' + REPLACE(@File_Evt,'''','''''') + N'''
   WITH (
     FORMAT = ''CSV'',
     FIRSTROW = 2,
     FIELDQUOTE = ''"'',
     ROWTERMINATOR = ''0x0d0a'',      -- if 0 rows, try 0x0a
     CODEPAGE = ''65001'',
     MAXERRORS = 100000,
     ERRORFILE = ''C:\Ontario511Data\evenements_err''
   );';
EXEC (@sql);

SELECT COUNT(*) AS StagingRows FROM dbo.Staging_Evenements;



EXEC dbo.Import_Evenements;
SELECT COUNT(*) FROM dbo.Evenements;




USE Ontario511DB;
GO
-- Make sure you have the correct table name
SELECT name FROM sys.tables WHERE name LIKE 'Road%';

-- Counts
SELECT COUNT(*) AS StagingCount FROM dbo.Staging_RoadConditions;
SELECT COUNT(*) AS TargetCount  FROM dbo.RoadConditions;

-- Peek at staging
SELECT TOP 5 * FROM dbo.Staging_RoadConditions;

-- Are LastUpdated values parsable?
SELECT 
  SUM(CASE WHEN TRY_CONVERT(DATETIMEOFFSET, LastUpdated) IS NOT NULL THEN 1 ELSE 0 END) AS Parsable_TZO,
  SUM(CASE WHEN TRY_CONVERT(DATETIME2(0),  LastUpdated) IS NOT NULL THEN 1 ELSE 0 END) AS Parsable_DT2
FROM dbo.Staging_RoadConditions;




ALTER PROCEDURE dbo.Import_RoadConditions AS
BEGIN
  SET NOCOUNT ON;

  MERGE dbo.RoadConditions AS T
  USING (
    SELECT
      LocationDescription,
      [Condition],             -- bracket to be safe
      [Visibility],
      [Drifting],
      Region,
      RoadwayName,
      EncodedPolyline,
      COALESCE( CAST(TRY_CONVERT(DATETIMEOFFSET, LastUpdated) AS DATETIME2(0)),
                TRY_CONVERT(DATETIME2(0), LastUpdated) ) AS LastUpdated
    FROM dbo.Staging_RoadConditions
  ) AS S
  ON ( T.RoadwayName = S.RoadwayName
       AND ISNULL(T.LocationDescription,N'') = ISNULL(S.LocationDescription,N'')
       AND T.LastUpdated = S.LastUpdated )
  WHEN MATCHED THEN
    UPDATE SET
      T.[Condition]      = S.[Condition],
      T.[Visibility]     = S.[Visibility],
      T.[Drifting]       = S.[Drifting],
      T.Region           = S.Region,
      T.EncodedPolyline  = S.EncodedPolyline,
      T.DateModification = SYSUTCDATETIME()
  WHEN NOT MATCHED BY TARGET THEN
    INSERT (LocationDescription, [Condition], [Visibility], [Drifting], Region, RoadwayName, EncodedPolyline, LastUpdated)
    VALUES (S.LocationDescription, S.[Condition], S.[Visibility], S.[Drifting], S.Region, S.RoadwayName, S.EncodedPolyline, S.LastUpdated);
END
GO



DECLARE @RootPath nvarchar(400) = N'C:\Ontario511Data';
DECLARE @File_Rc   nvarchar(400) = @RootPath + N'\roadconditions_ontario_511.csv';

TRUNCATE TABLE dbo.Staging_RoadConditions;

DECLARE @sql nvarchar(max) =
N'BULK INSERT dbo.Staging_RoadConditions
   FROM N''' + REPLACE(@File_Rc,'''','''''') + N'''
   WITH (
     FORMAT = ''CSV'',
     FIRSTROW = 2,
     FIELDQUOTE = ''"'',
     ROWTERMINATOR = ''0x0d0a'',   -- if count=0, try 0x0a
     CODEPAGE = ''65001'',
     MAXERRORS = 100000,
     ERRORFILE = ''C:\Ontario511Data\roadconditions_err''
   );';
EXEC (@sql);

SELECT COUNT(*) AS StagingCount FROM dbo.Staging_RoadConditions;
SELECT TOP 3 * FROM dbo.Staging_RoadConditions;



__________
EXEC dbo.Import_RoadConditions;

SELECT COUNT(*) AS TargetCount FROM dbo.RoadConditions;
SELECT TOP 10 * FROM dbo.RoadConditions ORDER BY ConditionId DESC;




____________________________
USE Ontario511DB;
GO

-- Optional: ensure modern compat (helps with TRY/CATCH, THROW, etc.)
-- ALTER DATABASE Ontario511DB SET COMPATIBILITY_LEVEL = 150;
-- GO

DECLARE @RootPath nvarchar(400) = N'C:\Ontario511Data';
DECLARE @rt       nvarchar(10)  = N'0x0d0a';  -- use N'0x0a' if needed
DECLARE @sql nvarchar(max), @file nvarchar(400), @cnt int;

-------------------------------------------------------------------------------
-- Ensure logger exists
IF OBJECT_ID('dbo.Log_Exec','P') IS NULL
  EXEC('CREATE PROCEDURE dbo.Log_Exec AS RETURN 0');
GO
ALTER PROCEDURE dbo.Log_Exec
  @TypeDonnees nvarchar(50),
  @Fichier nvarchar(400),
  @Nb int = NULL,
  @Statut nvarchar(20),
  @MessageErreur nvarchar(2000) = NULL
AS
BEGIN
  SET NOCOUNT ON;
  INSERT dbo.LogsExecution(DateHeure,TypeDonnees,Fichier,NbEnregistrements,Statut,MessageErreur)
  VALUES (SYSUTCDATETIME(), @TypeDonnees, @Fichier, @Nb, @Statut, LEFT(@MessageErreur,2000));
END
GO

-------------------------------------------------------------------------------
-- EVENEMENTS
BEGIN TRY
  SET @file = @RootPath + N'\evenements_ontario_511.csv';
  TRUNCATE TABLE dbo.Staging_Evenements;

  SET @sql = N'BULK INSERT dbo.Staging_Evenements FROM N''' + REPLACE(@file,'''','''''') +
             N''' WITH (FIRSTROW=2, FIELDTERMINATOR='','', ROWTERMINATOR=''' + @rt +
             N''', CODEPAGE=''65001'', MAXERRORS=100000, TABLOCK);';
  EXEC (@sql);

  SELECT @cnt = COUNT(*) FROM dbo.Staging_Evenements;
  EXEC dbo.Log_Exec N'Evenements', @file, @cnt, N'SUCCES', NULL;

  EXEC dbo.Import_Evenements;
  SELECT @cnt = COUNT(*) FROM dbo.Evenements;
  EXEC dbo.Log_Exec N'Evenements', @file, @cnt, N'SUCCES', N'Import terminé';
END TRY
BEGIN CATCH
  EXEC dbo.Log_Exec N'Evenements', @file, NULL, N'ECHEC', ERROR_MESSAGE();
  THROW;
END CATCH;

-------------------------------------------------------------------------------
-- CONSTRUCTIONS
BEGIN TRY
  SET @file = @RootPath + N'\constructions_ontario_511.csv';
  TRUNCATE TABLE dbo.Staging_Constructions;

  SET @sql = N'BULK INSERT dbo.Staging_Constructions FROM N''' + REPLACE(@file,'''','''''') +
             N''' WITH (FIRSTROW=2, FIELDTERMINATOR='','', ROWTERMINATOR=''' + @rt +
             N''', CODEPAGE=''65001'', MAXERRORS=100000, TABLOCK);';
  EXEC (@sql);

  SELECT @cnt = COUNT(*) FROM dbo.Staging_Constructions;
  EXEC dbo.Log_Exec N'Constructions', @file, @cnt, N'SUCCES', NULL;

  EXEC dbo.Import_Constructions;
  SELECT @cnt = COUNT(*) FROM dbo.Constructions;
  EXEC dbo.Log_Exec N'Constructions', @file, @cnt, N'SUCCES', N'Import terminé';
END TRY
BEGIN CATCH
  EXEC dbo.Log_Exec N'Constructions', @file, NULL, N'ECHEC', ERROR_MESSAGE();
  THROW;
END CATCH;

-------------------------------------------------------------------------------
-- CAMERAS
BEGIN TRY
  SET @file = @RootPath + N'\cameras_ontario_511.csv';
  TRUNCATE TABLE dbo.Staging_Cameras;

  SET @sql = N'BULK INSERT dbo.Staging_Cameras FROM N''' + REPLACE(@file,'''','''''') +
             N''' WITH (FIRSTROW=2, FIELDTERMINATOR='','', ROWTERMINATOR=''' + @rt +
             N''', CODEPAGE=''65001'', MAXERRORS=100000, TABLOCK);';
  EXEC (@sql);

  SELECT @cnt = COUNT(*) FROM dbo.Staging_Cameras;
  EXEC dbo.Log_Exec N'Cameras', @file, @cnt, N'SUCCES', NULL;

  EXEC dbo.Import_Cameras;
  SELECT @cnt = COUNT(*) FROM dbo.Cameras;
  EXEC dbo.Log_Exec N'Cameras', @file, @cnt, N'SUCCES', N'Import terminé';
END TRY
BEGIN CATCH
  EXEC dbo.Log_Exec N'Cameras', @file, NULL, N'ECHEC', ERROR_MESSAGE();
  THROW;
END CATCH;

-------------------------------------------------------------------------------
-- ROADCONDITIONS
BEGIN TRY
  SET @file = @RootPath + N'\roadconditions_ontario_511.csv';
  TRUNCATE TABLE dbo.Staging_RoadConditions;

  SET @sql = N'BULK INSERT dbo.Staging_RoadConditions FROM N''' + REPLACE(@file,'''','''''') +
             N''' WITH (FIRSTROW=2, FIELDTERMINATOR='','', ROWTERMINATOR=''' + @rt +
             N''', CODEPAGE=''65001'', MAXERRORS=100000, TABLOCK);';
  EXEC (@sql);

  SELECT @cnt = COUNT(*) FROM dbo.Staging_RoadConditions;
  EXEC dbo.Log_Exec N'RoadConditions', @file, @cnt, N'SUCCES', NULL;

  EXEC dbo.Import_RoadConditions;
  SELECT @cnt = COUNT(*) FROM dbo.RoadConditions;
  EXEC dbo.Log_Exec N'RoadConditions', @file, @cnt, N'SUCCES', N'Import terminé';
END TRY
BEGIN CATCH
  EXEC dbo.Log_Exec N'RoadConditions', @file, NULL, N'ECHEC', ERROR_MESSAGE();
  THROW;
END CATCH;

-------------------------------------------------------------------------------
-- Verify logs
SELECT TOP 50 * FROM dbo.LogsExecution ORDER BY LogId DESC;



_________________________
USE Ontario511DB;
GO

DECLARE @RootPath nvarchar(400) = N'C:\Ontario511Data';
DECLARE @File_Cam nvarchar(400) = @RootPath + N'\cameras_ontario_511.csv';

TRUNCATE TABLE dbo.Staging_Cameras;

DECLARE @sql nvarchar(max) =
N'BULK INSERT dbo.Staging_Cameras
   FROM N''' + REPLACE(@File_Cam,'''','''''') + N'''
   WITH (
     FORMAT = ''CSV'',
     FIRSTROW = 2,
     FIELDQUOTE = ''"'',
     ROWTERMINATOR = ''0x0d0a'',  -- if 0 rows, try 0x0a
     CODEPAGE = ''65001'',
     TABLOCK
   );';
EXEC (@sql);


USE Ontario511DB;
GO

DECLARE @RootPath nvarchar(400) = N'C:\Ontario511Data';
DECLARE @File_Con nvarchar(400) = @RootPath + N'\constructions_ontario_511.csv';

TRUNCATE TABLE dbo.Staging_Constructions;

DECLARE @sql nvarchar(max) =
N'BULK INSERT dbo.Staging_Constructions
   FROM N''' + REPLACE(@File_Con,'''','''''') + N'''
   WITH (
     FORMAT = ''CSV'',
     FIRSTROW = 2,
     FIELDQUOTE = ''"'',
     ROWTERMINATOR = ''0x0d0a'',  -- if 0 rows, try 0x0a
     CODEPAGE = ''65001'',
     TABLOCK
   );';
EXEC (@sql);



