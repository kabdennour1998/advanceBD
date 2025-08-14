
/*
================================================================================
Ontario 511 — Script SQL (basé sur les CSV fournis)
Base cible : Ontario511DB
Contenu : Schéma adapté aux en-têtes réels, staging + MERGE, BULK INSERT modèles,
          contraintes, indexes, triggers, et quelques procédures utiles.
================================================================================
NOTE: Adapter @RootPath pour pointer vers le dossier où se trouvent vos CSV.
      Les fichiers attendus sont exactement ceux uploadés :
        - evenements_ontario_511.csv
        - constructions_ontario_511.csv
        - cameras_ontario_511.csv
        - roadconditions_ontario_511.csv
================================================================================
*/

/*==========================================================================
  0) Base de données
==========================================================================*/
IF DB_ID(N'Ontario511DB') IS NULL
BEGIN
    CREATE DATABASE Ontario511DB;
END
GO

USE Ontario511DB;
GO

/*==========================================================================
  1) Tables cibles (définitives) adaptées aux CSV
==========================================================================*/

/*=========================
  Evenements
=========================*/
IF OBJECT_ID('dbo.Evenements') IS NULL
BEGIN
    CREATE TABLE dbo.Evenements
    (
        EvenementId      BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_Evenements PRIMARY KEY,
        ID               BIGINT NOT NULL,
        SourceId         NVARCHAR(100) NOT NULL,
        Organization     NVARCHAR(200) NULL,
        RoadwayName      NVARCHAR(200) NULL,
        Direction        NVARCHAR(100) NULL,
        Description      NVARCHAR(4000) NULL,
        Reported         DATETIME2(0) NULL,
        LastUpdated      DATETIME2(0) NULL,
        StartDate        DATETIME2(0) NULL,
        PlannedEndDate   DATETIME2(0) NULL,
        EventType        NVARCHAR(100) NULL,
        Latitude         DECIMAL(9,6) NULL,
        Longitude        DECIMAL(9,6) NULL,
        DateCreation     DATETIME2(0) NOT NULL CONSTRAINT DF_Evenements_DateCreation DEFAULT (SYSUTCDATETIME()),
        DateModification DATETIME2(0) NOT NULL CONSTRAINT DF_Evenements_DateModification DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT UQ_Evenements UNIQUE (SourceId, ID),
        CONSTRAINT CK_Evenements_Dates CHECK (PlannedEndDate IS NULL OR PlannedEndDate >= StartDate)
    );
    CREATE INDEX IX_Evenements_Roadway ON dbo.Evenements(RoadwayName);
    CREATE INDEX IX_Evenements_LastUpdated ON dbo.Evenements(LastUpdated);
END
GO

/*=========================
  Constructions
=========================*/
IF OBJECT_ID('dbo.Constructions') IS NULL
BEGIN
    CREATE TABLE dbo.Constructions
    (
        ConstructionId   BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_Constructions PRIMARY KEY,
        ID               BIGINT NOT NULL,
        SourceId         NVARCHAR(100) NOT NULL,
        Organization     NVARCHAR(200) NULL,
        RoadwayName      NVARCHAR(200) NULL,
        DirectionOfTravel NVARCHAR(100) NULL,
        Description      NVARCHAR(4000) NULL,
        Reported         DATETIME2(0) NULL,
        LastUpdated      DATETIME2(0) NULL,
        StartDate        DATETIME2(0) NULL,
        PlannedEndDate   DATETIME2(0) NULL,
        LanesAffected    NVARCHAR(200) NULL,
        Latitude         DECIMAL(9,6) NULL,
        Longitude        DECIMAL(9,6) NULL,
        LatitudeSecondary DECIMAL(9,6) NULL,
        LongitudeSecondary DECIMAL(9,6) NULL,
        EventType        NVARCHAR(100) NULL,
        IsFullClosure    BIT NULL,
        Comment          NVARCHAR(2000) NULL,
        EncodedPolyline  NVARCHAR(MAX) NULL,
        Recurrence       NVARCHAR(400) NULL,
        RecurrenceSchedules NVARCHAR(MAX) NULL,
        LinkId           BIGINT NULL,
        DateCreation     DATETIME2(0) NOT NULL CONSTRAINT DF_Constructions_DateCreation DEFAULT (SYSUTCDATETIME()),
        DateModification DATETIME2(0) NOT NULL CONSTRAINT DF_Constructions_DateModification DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT UQ_Constructions UNIQUE (SourceId, ID),
        CONSTRAINT CK_Constructions_Dates CHECK (PlannedEndDate IS NULL OR PlannedEndDate >= StartDate)
    );
    CREATE INDEX IX_Constructions_Roadway ON dbo.Constructions(RoadwayName);
    CREATE INDEX IX_Constructions_LastUpdated ON dbo.Constructions(LastUpdated);
END
GO

/*=========================
  Cameras
=========================*/
IF OBJECT_ID('dbo.Cameras') IS NULL
BEGIN
    CREATE TABLE dbo.Cameras
    (
        CameraId         BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_Cameras PRIMARY KEY,
        BaseId           NVARCHAR(64) NOT NULL,
        Source           NVARCHAR(100) NULL,
        SourceId         NVARCHAR(100) NULL,
        Roadway          NVARCHAR(200) NULL,
        Direction        NVARCHAR(100) NULL,
        Location         NVARCHAR(400) NULL,
        Latitude         DECIMAL(9,6) NULL,
        Longitude        DECIMAL(9,6) NULL,
        ViewId           NVARCHAR(64) NOT NULL,
        Url              NVARCHAR(1000) NULL,
        Status           NVARCHAR(50) NULL,
        Description      NVARCHAR(2000) NULL,
        DateCreation     DATETIME2(0) NOT NULL CONSTRAINT DF_Cameras_DateCreation DEFAULT (SYSUTCDATETIME()),
        DateModification DATETIME2(0) NOT NULL CONSTRAINT DF_Cameras_DateModification DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT UQ_Cameras UNIQUE (BaseId, ViewId)
    );
    CREATE INDEX IX_Cameras_Roadway ON dbo.Cameras(Roadway);
END
GO

/*=========================
  RoadConditions
=========================*/
IF OBJECT_ID('dbo.RoadConditions') IS NULL
BEGIN
    CREATE TABLE dbo.RoadConditions
    (
        ConditionId      BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_RoadConditions PRIMARY KEY,
        LocationDescription NVARCHAR(400) NULL,
        Condition        NVARCHAR(100) NULL,
        Visibility       NVARCHAR(100) NULL,
        Drifting         NVARCHAR(20) NULL,
        Region           NVARCHAR(100) NULL,
        RoadwayName      NVARCHAR(200) NULL,
        EncodedPolyline  NVARCHAR(MAX) NULL,
        LastUpdated      DATETIME2(0) NULL,
        DateCreation     DATETIME2(0) NOT NULL CONSTRAINT DF_RoadConditions_DateCreation DEFAULT (SYSUTCDATETIME()),
        DateModification DATETIME2(0) NOT NULL CONSTRAINT DF_RoadConditions_DateModification DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT UQ_RoadConditions UNIQUE (RoadwayName, LastUpdated, LocationDescription)
    );
    CREATE INDEX IX_RoadConditions_LastUpdated ON dbo.RoadConditions(LastUpdated);
END
GO

/*=========================
  Tables de log & historique
=========================*/
IF OBJECT_ID('dbo.LogsExecution') IS NULL
BEGIN
    CREATE TABLE dbo.LogsExecution
    (
        LogId            BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_LogsExecution PRIMARY KEY,
        DateHeure        DATETIME2(0) NOT NULL CONSTRAINT DF_LogsExecution_DateHeure DEFAULT (SYSUTCDATETIME()),
        TypeDonnees      NVARCHAR(50) NOT NULL,
        Fichier          NVARCHAR(400) NULL,
        NbEnregistrements INT NULL,
        Statut           NVARCHAR(20) NOT NULL,
        MessageErreur    NVARCHAR(2000) NULL
    );
END
GO

IF OBJECT_ID('dbo.HistoriqueEvenements') IS NULL
BEGIN
    CREATE TABLE dbo.HistoriqueEvenements
    (
        HistoriqueId     BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_HistoriqueEvenements PRIMARY KEY,
        EvenementId      BIGINT NOT NULL,
        ID               BIGINT NOT NULL,
        SourceId         NVARCHAR(100) NOT NULL,
        Organization     NVARCHAR(200) NULL,
        RoadwayName      NVARCHAR(200) NULL,
        Direction        NVARCHAR(100) NULL,
        Description      NVARCHAR(2000) NULL,
        Reported         DATETIME2(0) NULL,
        LastUpdated      DATETIME2(0) NULL,
        StartDate        DATETIME2(0) NULL,
        PlannedEndDate   DATETIME2(0) NULL,
        EventType        NVARCHAR(100) NULL,
        Latitude         DECIMAL(9,6) NULL,
        Longitude        DECIMAL(9,6) NULL,
        DateModificationOrig DATETIME2(0) NOT NULL,
        DateHistorisationUtc DATETIME2(0) NOT NULL CONSTRAINT DF_HistEvt_Date DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT FK_HistoriqueEvenements_Evenements FOREIGN KEY (EvenementId) REFERENCES dbo.Evenements(EvenementId)
    );
END
GO

/*==========================================================================
  2) Tables STAGING (import brut en NVARCHAR, conversion en MERGE)
==========================================================================*/

IF OBJECT_ID('dbo.Staging_Evenements') IS NULL
BEGIN
    CREATE TABLE dbo.Staging_Evenements
    (
        ID               NVARCHAR(100) NULL,
        SourceId         NVARCHAR(100) NULL,
        Organization     NVARCHAR(200) NULL,
        RoadwayName      NVARCHAR(200) NULL,
        Direction        NVARCHAR(100) NULL,
        Description      NVARCHAR(4000) NULL,
        Reported         NVARCHAR(100) NULL,
        LastUpdated      NVARCHAR(100) NULL,
        StartDate        NVARCHAR(100) NULL,
        PlannedEndDate   NVARCHAR(100) NULL,
        EventType        NVARCHAR(100) NULL,
        Latitude         NVARCHAR(100) NULL,
        Longitude        NVARCHAR(100) NULL
    );
END
GO

IF OBJECT_ID('dbo.Staging_Constructions') IS NULL
BEGIN
    CREATE TABLE dbo.Staging_Constructions
    (
        ID               NVARCHAR(100) NULL,
        SourceId         NVARCHAR(100) NULL,
        Organization     NVARCHAR(200) NULL,
        RoadwayName      NVARCHAR(200) NULL,
        DirectionOfTravel NVARCHAR(100) NULL,
        Description      NVARCHAR(MAX) NULL,
        Reported         NVARCHAR(100) NULL,
        LastUpdated      NVARCHAR(100) NULL,
        StartDate        NVARCHAR(100) NULL,
        PlannedEndDate   NVARCHAR(100) NULL,
        LanesAffected    NVARCHAR(200) NULL,
        Latitude         NVARCHAR(100) NULL,
        Longitude        NVARCHAR(100) NULL,
        LatitudeSecondary NVARCHAR(100) NULL,
        LongitudeSecondary NVARCHAR(100) NULL,
        EventType        NVARCHAR(100) NULL,
        IsFullClosure    NVARCHAR(50) NULL,
        Comment          NVARCHAR(2000) NULL,
        EncodedPolyline  NVARCHAR(MAX) NULL,
        Recurrence       NVARCHAR(400) NULL,
        RecurrenceSchedules NVARCHAR(MAX) NULL,
        LinkId           NVARCHAR(100) NULL
    );
END
GO

IF OBJECT_ID('dbo.Staging_Cameras') IS NULL
BEGIN
    CREATE TABLE dbo.Staging_Cameras
    (
        BaseId           NVARCHAR(64) NULL,
        Source           NVARCHAR(100) NULL,
        SourceId         NVARCHAR(100) NULL,
        Roadway          NVARCHAR(200) NULL,
        Direction        NVARCHAR(100) NULL,
        Location         NVARCHAR(400) NULL,
        Latitude         NVARCHAR(100) NULL,
        Longitude        NVARCHAR(100) NULL,
        ViewId           NVARCHAR(64) NULL,
        Url              NVARCHAR(1000) NULL,
        Status           NVARCHAR(50) NULL,
        Description      NVARCHAR(2000) NULL
    );
END
GO

IF OBJECT_ID('dbo.Staging_RoadConditions') IS NULL
BEGIN
    CREATE TABLE dbo.Staging_RoadConditions
    (
        LocationDescription NVARCHAR(400) NULL,
        Condition        NVARCHAR(100) NULL,
        Visibility       NVARCHAR(100) NULL,
        Drifting         NVARCHAR(20) NULL,
        Region           NVARCHAR(100) NULL,
        RoadwayName      NVARCHAR(200) NULL,
        EncodedPolyline  NVARCHAR(MAX) NULL,
        LastUpdated      NVARCHAR(100) NULL
    );
END
GO

/*==========================================================================
  3) BULK INSERT (chemins à adapter)
==========================================================================*/
DECLARE @RootPath NVARCHAR(400) = N'C:\\Ontario511Data\\drop'; -- A D A P T E R

DECLARE @File_Evt NVARCHAR(400) = @RootPath + N'C:\\Ontario511Data\\evenements_ontario_511.csv';
DECLARE @File_Con NVARCHAR(400) = @RootPath + N'C:\\Ontario511Data\\constructions_ontario_511.csv';
DECLARE @File_Cam NVARCHAR(400) = @RootPath + N'C:\\Ontario511Data\\cameras_ontario_511.csv';
DECLARE @File_Rc  NVARCHAR(400) = @RootPath + N'C:\\Ontario511Data\\roadconditions_ontario_511.csv';

/* Modèles (décommenter pour exécuter) */
/*
TRUNCATE TABLE dbo.Staging_Evenements;
BULK INSERT dbo.Staging_Evenements FROM @File_Evt
WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='0x0a', CODEPAGE='65001', TABLOCK);

TRUNCATE TABLE dbo.Staging_Constructions;
BULK INSERT dbo.Staging_Constructions FROM @File_Con
WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='0x0a', CODEPAGE='65001', TABLOCK);

TRUNCATE TABLE dbo.Staging_Cameras;
BULK INSERT dbo.Staging_Cameras FROM @File_Cam
WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='0x0a', CODEPAGE='65001', TABLOCK);

TRUNCATE TABLE dbo.Staging_RoadConditions;
BULK INSERT dbo.Staging_RoadConditions FROM @File_Rc
WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='0x0a', CODEPAGE='65001', TABLOCK);
*/

/*==========================================================================
  4) Procédures: MERGE depuis STAGING vers cibles (avec conversions robustes)
==========================================================================*/

IF OBJECT_ID('dbo.Import_Evenements') IS NOT NULL DROP PROCEDURE dbo.Import_Evenements;
GO
CREATE PROCEDURE dbo.Import_Evenements AS
BEGIN
    SET NOCOUNT ON;
    MERGE dbo.Evenements AS T
    USING (
        SELECT
            TRY_CONVERT(BIGINT, ID) AS ID,
            SourceId,
            Organization,
            RoadwayName,
            Direction,
            Description,
            TRY_CONVERT(DATETIME2(0), Reported) AS Reported,
            TRY_CONVERT(DATETIME2(0), LastUpdated) AS LastUpdated,
            TRY_CONVERT(DATETIME2(0), StartDate) AS StartDate,
            TRY_CONVERT(DATETIME2(0), PlannedEndDate) AS PlannedEndDate,
            EventType,
            TRY_CONVERT(DECIMAL(9,6), Latitude) AS Latitude,
            TRY_CONVERT(DECIMAL(9,6), Longitude) AS Longitude
        FROM dbo.Staging_Evenements
    ) AS S
    ON (T.SourceId = S.SourceId AND T.ID = S.ID)
    WHEN MATCHED THEN UPDATE SET
        T.Organization = S.Organization,
        T.RoadwayName = S.RoadwayName,
        T.Direction = S.Direction,
        T.Description = S.Description,
        T.Reported = S.Reported,
        T.LastUpdated = S.LastUpdated,
        T.StartDate = S.StartDate,
        T.PlannedEndDate = S.PlannedEndDate,
        T.EventType = S.EventType,
        T.Latitude = S.Latitude,
        T.Longitude = S.Longitude,
        T.DateModification = SYSUTCDATETIME()
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (ID, SourceId, Organization, RoadwayName, Direction, Description, Reported, LastUpdated, StartDate, PlannedEndDate, EventType, Latitude, Longitude)
        VALUES (S.ID, S.SourceId, S.Organization, S.RoadwayName, S.Direction, S.Description, S.Reported, S.LastUpdated, S.StartDate, S.PlannedEndDate, S.EventType, S.Latitude, S.Longitude);
END
GO

IF OBJECT_ID('dbo.Import_Constructions') IS NOT NULL DROP PROCEDURE dbo.Import_Constructions;
GO
CREATE PROCEDURE dbo.Import_Constructions AS
BEGIN
    SET NOCOUNT ON;
    MERGE dbo.Constructions AS T
    USING (
        SELECT
            TRY_CONVERT(BIGINT, ID) AS ID,
            SourceId,
            Organization,
            RoadwayName,
            DirectionOfTravel,
            Description,
            TRY_CONVERT(DATETIME2(0), Reported) AS Reported,
            TRY_CONVERT(DATETIME2(0), LastUpdated) AS LastUpdated,
            TRY_CONVERT(DATETIME2(0), StartDate) AS StartDate,
            TRY_CONVERT(DATETIME2(0), PlannedEndDate) AS PlannedEndDate,
            LanesAffected,
            TRY_CONVERT(DECIMAL(9,6), Latitude) AS Latitude,
            TRY_CONVERT(DECIMAL(9,6), Longitude) AS Longitude,
            TRY_CONVERT(DECIMAL(9,6), LatitudeSecondary) AS LatitudeSecondary,
            TRY_CONVERT(DECIMAL(9,6), LongitudeSecondary) AS LongitudeSecondary,
            EventType,
            CASE WHEN ISNULL(LTRIM(RTRIM(IsFullClosure)),'') IN (N'1',N'true',N'True',N'TRUE',N'yes',N'Yes',N'Oui',N'oui') THEN 1 ELSE 0 END AS IsFullClosure,
            Comment,
            EncodedPolyline,
            Recurrence,
            RecurrenceSchedules,
            TRY_CONVERT(BIGINT, LinkId) AS LinkId
        FROM dbo.Staging_Constructions
    ) AS S
    ON (T.SourceId = S.SourceId AND T.ID = S.ID)
    WHEN MATCHED THEN UPDATE SET
        T.Organization = S.Organization,
        T.RoadwayName = S.RoadwayName,
        T.DirectionOfTravel = S.DirectionOfTravel,
        T.Description = S.Description,
        T.Reported = S.Reported,
        T.LastUpdated = S.LastUpdated,
        T.StartDate = S.StartDate,
        T.PlannedEndDate = S.PlannedEndDate,
        T.LanesAffected = S.LanesAffected,
        T.Latitude = S.Latitude,
        T.Longitude = S.Longitude,
        T.LatitudeSecondary = S.LatitudeSecondary,
        T.LongitudeSecondary = S.LongitudeSecondary,
        T.EventType = S.EventType,
        T.IsFullClosure = S.IsFullClosure,
        T.Comment = S.Comment,
        T.EncodedPolyline = S.EncodedPolyline,
        T.Recurrence = S.Recurrence,
        T.RecurrenceSchedules = S.RecurrenceSchedules,
        T.LinkId = S.LinkId,
        T.DateModification = SYSUTCDATETIME()
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (ID, SourceId, Organization, RoadwayName, DirectionOfTravel, Description, Reported, LastUpdated, StartDate, PlannedEndDate, LanesAffected, Latitude, Longitude, LatitudeSecondary, LongitudeSecondary, EventType, IsFullClosure, Comment, EncodedPolyline, Recurrence, RecurrenceSchedules, LinkId)
        VALUES (S.ID, S.SourceId, S.Organization, S.RoadwayName, S.DirectionOfTravel, S.Description, S.Reported, S.LastUpdated, S.StartDate, S.PlannedEndDate, S.LanesAffected, S.Latitude, S.Longitude, S.LatitudeSecondary, S.LongitudeSecondary, S.EventType, S.IsFullClosure, S.Comment, S.EncodedPolyline, S.Recurrence, S.RecurrenceSchedules, S.LinkId);
END
GO

IF OBJECT_ID('dbo.Import_Cameras') IS NOT NULL DROP PROCEDURE dbo.Import_Cameras;
GO
CREATE PROCEDURE dbo.Import_Cameras AS
BEGIN
    SET NOCOUNT ON;
    MERGE dbo.Cameras AS T
    USING (
        SELECT
            BaseId,
            Source,
            SourceId,
            Roadway,
            Direction,
            Location,
            TRY_CONVERT(DECIMAL(9,6), Latitude) AS Latitude,
            TRY_CONVERT(DECIMAL(9,6), Longitude) AS Longitude,
            ViewId,
            Url,
            Status,
            Description
        FROM dbo.Staging_Cameras
    ) AS S
    ON (T.BaseId = S.BaseId AND T.ViewId = S.ViewId)
    WHEN MATCHED THEN UPDATE SET
        T.Source = S.Source,
        T.SourceId = S.SourceId,
        T.Roadway = S.Roadway,
        T.Direction = S.Direction,
        T.Location = S.Location,
        T.Latitude = S.Latitude,
        T.Longitude = S.Longitude,
        T.Url = S.Url,
        T.Status = S.Status,
        T.Description = S.Description,
        T.DateModification = SYSUTCDATETIME()
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (BaseId, Source, SourceId, Roadway, Direction, Location, Latitude, Longitude, ViewId, Url, Status, Description)
        VALUES (S.BaseId, S.Source, S.SourceId, S.Roadway, S.Direction, S.Location, S.Latitude, S.Longitude, S.ViewId, S.Url, S.Status, S.Description);
END
GO

IF OBJECT_ID('dbo.Import_RoadConditions') IS NOT NULL DROP PROCEDURE dbo.Import_RoadConditions;
GO
CREATE PROCEDURE dbo.Import_RoadConditions AS
BEGIN
    SET NOCOUNT ON;
    MERGE dbo.RoadConditions AS T
    USING (
        SELECT
            LocationDescription,
            Condition,
            Visibility,
            Drifting,
            Region,
            RoadwayName,
            EncodedPolyline,
            TRY_CONVERT(DATETIME2(0), LastUpdated) AS LastUpdated
        FROM dbo.Staging_RoadConditions
    ) AS S
    ON (T.RoadwayName = S.RoadwayName AND T.LastUpdated = S.LastUpdated AND ISNULL(T.LocationDescription,N'') = ISNULL(S.LocationDescription,N''))
    WHEN MATCHED THEN UPDATE SET
        T.Condition = S.Condition,
        T.Visibility = S.Visibility,
        T.Drifting = S.Drifting,
        T.Region = S.Region,
        T.EncodedPolyline = S.EncodedPolyline,
        T.DateModification = SYSUTCDATETIME()
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (LocationDescription, Condition, Visibility, Drifting, Region, RoadwayName, EncodedPolyline, LastUpdated)
        VALUES (S.LocationDescription, S.Condition, S.Visibility, S.Drifting, S.Region, S.RoadwayName, S.EncodedPolyline, S.LastUpdated);
END
GO

/*==========================================================================
  5) Triggers (mise à jour timestamp + historisation Evenements)
==========================================================================*/

IF OBJECT_ID('dbo.TR_TS_Evenements', 'TR') IS NOT NULL DROP TRIGGER dbo.TR_TS_Evenements;
GO
CREATE TRIGGER dbo.TR_TS_Evenements ON dbo.Evenements
AFTER INSERT, UPDATE AS
BEGIN
    SET NOCOUNT ON;
    UPDATE E
      SET DateModification = SYSUTCDATETIME()
    FROM dbo.Evenements E
    INNER JOIN inserted i ON i.EvenementId = E.EvenementId;
END
GO

IF OBJECT_ID('dbo.TR_TS_Constructions', 'TR') IS NOT NULL DROP TRIGGER dbo.TR_TS_Constructions;
GO
CREATE TRIGGER dbo.TR_TS_Constructions ON dbo.Constructions
AFTER INSERT, UPDATE AS
BEGIN
    SET NOCOUNT ON;
    UPDATE C
      SET DateModification = SYSUTCDATETIME()
    FROM dbo.Constructions C
    INNER JOIN inserted i ON i.ConstructionId = C.ConstructionId;
END
GO

IF OBJECT_ID('dbo.TR_TS_Cameras', 'TR') IS NOT NULL DROP TRIGGER dbo.TR_TS_Cameras;
GO
CREATE TRIGGER dbo.TR_TS_Cameras ON dbo.Cameras
AFTER INSERT, UPDATE AS
BEGIN
    SET NOCOUNT ON;
    UPDATE C
      SET DateModification = SYSUTCDATETIME()
    FROM dbo.Cameras C
    INNER JOIN inserted i ON i.CameraId = C.CameraId;
END
GO

IF OBJECT_ID('dbo.TR_TS_RoadConditions', 'TR') IS NOT NULL DROP TRIGGER dbo.TR_TS_RoadConditions;
GO
CREATE TRIGGER dbo.TR_TS_RoadConditions ON dbo.RoadConditions
AFTER INSERT, UPDATE AS
BEGIN
    SET NOCOUNT ON;
    UPDATE R
      SET DateModification = SYSUTCDATETIME()
    FROM dbo.RoadConditions R
    INNER JOIN inserted i ON i.ConditionId = R.ConditionId;
END
GO

IF OBJECT_ID('dbo.TR_Historiser_Modifs_Evenements', 'TR') IS NOT NULL DROP TRIGGER dbo.TR_Historiser_Modifs_Evenements;
GO
CREATE TRIGGER dbo.TR_Historiser_Modifs_Evenements ON dbo.Evenements
AFTER UPDATE AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO dbo.HistoriqueEvenements
    (
        EvenementId, ID, SourceId, Organization, RoadwayName, Direction, Description,
        Reported, LastUpdated, StartDate, PlannedEndDate, EventType, Latitude, Longitude,
        DateModificationOrig
    )
    SELECT
        d.EvenementId, d.ID, d.SourceId, d.Organization, d.RoadwayName, d.Direction, d.Description,
        d.Reported, d.LastUpdated, d.StartDate, d.PlannedEndDate, d.EventType, d.Latitude, d.Longitude,
        d.DateModification
    FROM deleted d;
END
GO

/*==========================================================================
  6) Rapports rapides (procédures utilitaires)
==========================================================================*/

IF OBJECT_ID('dbo.Report_TopRoad_Cameras') IS NOT NULL DROP PROCEDURE dbo.Report_TopRoad_Cameras;
GO
CREATE PROCEDURE dbo.Report_TopRoad_Cameras AS
BEGIN
    SET NOCOUNT ON;
    SELECT TOP (1) Roadway, COUNT(*) AS NbCameras
    FROM dbo.Cameras
    WHERE Roadway IS NOT NULL AND Roadway <> N''
    GROUP BY Roadway
    ORDER BY COUNT(*) DESC, Roadway;
END
GO

IF OBJECT_ID('dbo.Report_TopRoad_Constructions') IS NOT NULL DROP PROCEDURE dbo.Report_TopRoad_Constructions;
GO
CREATE PROCEDURE dbo.Report_TopRoad_Constructions AS
BEGIN
    SET NOCOUNT ON;
    SELECT TOP (1) RoadwayName, COUNT(*) AS NbConstructions
    FROM dbo.Constructions
    WHERE (PlannedEndDate IS NULL OR PlannedEndDate >= SYSUTCDATETIME())
      AND RoadwayName IS NOT NULL AND RoadwayName <> N''
    GROUP BY RoadwayName
    ORDER BY COUNT(*) DESC, RoadwayName;
END
GO

IF OBJECT_ID('dbo.Search_Evenements') IS NOT NULL DROP PROCEDURE dbo.Search_Evenements;
GO
CREATE PROCEDURE dbo.Search_Evenements
    @Road NVARCHAR(200) = NULL,
    @Type NVARCHAR(100) = NULL,
    @Date DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SELECT *
    FROM dbo.Evenements
    WHERE (@Road IS NULL OR RoadwayName = @Road)
      AND (@Type IS NULL OR EventType = @Type)
      AND (@Date IS NULL OR CONVERT(date, COALESCE(StartDate, Reported, LastUpdated)) = @Date)
    ORDER BY COALESCE(LastUpdated, StartDate, Reported) DESC;
END
GO

/*==========================================================================
  7) Exemple d'orchestration manuelle (sans SQL Agent)
==========================================================================*/
/*
-- 1) Charger staging (décommenter la section BULK INSERT ci-dessus)
-- 2) Importer vers cibles
EXEC dbo.Import_Evenements;
EXEC dbo.Import_Constructions;
EXEC dbo.Import_Cameras;
EXEC dbo.Import_RoadConditions;

-- 3) Rapports
EXEC dbo.Report_TopRoad_Cameras;
EXEC dbo.Report_TopRoad_Constructions;
EXEC dbo.Search_Evenements @Road = N'QEW', @Type = N'Construction', @Date = '2025-08-01';
*/

PRINT 'Script prêt. Adaptez @RootPath puis exécutez BULK INSERT + procs d''import.';
