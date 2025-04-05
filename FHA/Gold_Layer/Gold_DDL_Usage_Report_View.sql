-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.text("catalog_name", "","")
-- MAGIC catalog_name = dbutils.widgets.get("catalog_name")
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"use catalog {catalog_name}");

-- COMMAND ----------

USE DWH;

-- COMMAND ----------

CREATE or replace VIEW PORTFOLIIOS_VW AS
     SELECT
  ROW_NUMBER() OVER (
    ORDER BY
      U.Email,
      S.Name
  ) AS Id,
  S.Name AS Servicer,
  U.Email AS User,
  cast(fus.logintimestamp AS DATE) AS LoginDate, --source table change
  COUNT(DISTINCT FUS.User_Session_Id) AS Logins, --source table change
  COUNT(DISTINCT SSF.Servicer_Source_File_Id) AS PortfoliosUploaded,
  SUM(
    CASE
      WHEN SSF.StatusId != 1 THEN 1
      ELSE 0
    END
  ) AS PortfolioUploadsFailed,
  SUM(
    CASE
      WHEN SSF.StatusId = 1 THEN 1
      ELSE 0
    END
  ) AS PortfolioUploadsValidated
FROM
  DWH.USER_SESSIONS_SERVICER_FACT FUSS --table change
  INNER JOIN DWH.USER_SESSION_FACT FUS ON FUSS.SessionId= FUS.User_Session_Id
  INNER JOIN DWH.USER_DIM U ON U.User_Id = FUS.USERID --join table change
  INNER JOIN DWH.SERVICER_DIM S ON S.Servicer_Id = FUSS.SERVICERID --added
  LEFT JOIN DWH.SERVICER_UPLOAD_FACT SSF ON SSF.src_XMD_INSERTUSERID = FUS.USERID --userid source table change
  AND SSF.src_XMD_INSERTDATETIMESTAMP BETWEEN FUSS.StartTimestamp --start time field change
  AND ifnull(ifnull(FUSS.EndTimestamp,FUS.LastActivityTimestamp),'2099-01-01') --end time field and logic change
  --LEFT JOIN DWH.SERVICER_DIM S ON S.SERVICER_Id = SSF.SERVICERID --DELETED
  --where cast(SSF.src_XMD_INSERTDATETIMESTAMP AS DATE) >= '2024-11-25'
GROUP BY
  U.Email,
  S.Name,
  cast(fus.logintimestamp AS DATE) --source table change


-- COMMAND ----------

select * from PORTFOLIIOS_VW

-- COMMAND ----------

-- CREATE or replace VIEW ASSET_DISPOSITION_VW AS
-- SELECT
--   ROW_NUMBER() OVER (
--     ORDER BY
--       U.Email,
--       S.Name
--   ) AS Id,
--   S.Name AS Servicer,
--   U.Email AS User,
--   cast(fus.logintimestamp AS DATE) AS LoginDate,
--   COUNT(SAD.Servicer_Asset_Id) AS AssetsUploaded,
--   COUNT(DISTINCT SAD.ServicerAssetKey) AS UniqueAssetsUploaded,
--   COUNT(
--     CASE
--       WHEN SAD.Disposition_StatusId = 2 THEN SAD.Servicer_Asset_Id
--     END
--   ) AS AssetsSaved,
--   COUNT(
--     DISTINCT CASE
--       WHEN SAD.Servicer_Asset_Id IS NOT NULL
--       AND SAD.ServicerAssetKey IS NOT NULL
--       AND SAD.Disposition_StatusId = 2 THEN SAD.ServicerAssetKey
--     END
--   ) AS UniqueAssetsSaved
-- FROM
--   DWH.USER_SESSIONS_SERVICER_FACT FUSS --table change
--   INNER JOIN DWH.USER_SESSION_FACT FUS ON FUSS.SessionId = FUS.User_Session_Id
--   INNER JOIN DWH.USER_DIM U ON U.User_Id = FUS.USERID
--   LEFT JOIN DWH.SERVICER_UPLOAD_FACT SSF ON SSF.src_XMD_INSERTUSERID = FUS.USERID --userid source table change
--   AND SSF.src_XMD_INSERTDATETIMESTAMP BETWEEN FUSS.StartTimestamp --start time field change
--   AND ifnull(
--     ifnull(FUSS.EndTimestamp, FUS.LastActivityTimestamp),
--     '2099-01-01'
--   )
--   LEFT JOIN DWH.SERVICER_DIM S ON S.SERVICER_Id = SSF.SERVICERID
--   LEFT JOIN DWH.SERVICER_ASSET_DISPOSITION_FACT SAD ON SAD.SourceFileId = SSF.Servicer_Source_File_Id --LEFT JOIN DWH.SERVICER_ASSET_DISPOSITION_FACT SAD ON SAD.Servicer_Asset_Id = SA.Servicer_Asset_Id
--   --where fus.UserId='4bdad07d-a4bd-411f-9ab9-f91258c9d100'
--   where cast(SSF.src_XMD_INSERTDATETIMESTAMP AS DATE) >= '2024-11-25'
-- GROUP BY
--   U.Email,
--   S.Name,
--   cast(fus.logintimestamp AS DATE)

-- COMMAND ----------

CREATE OR REPLACE VIEW DWH.ASSET_UPLOAD_VW AS
SELECT
  ROW_NUMBER() OVER (
    ORDER BY
      U.Email,
      S.Name
  ) AS Id,
  S.Name AS Servicer,
  U.Email AS User,
  cast(fus.logintimestamp AS DATE) AS LoginDate,
  count(distinct
    CASE
      WHEN SSF.StatusId = 1 THEN AST.Id
    END
  ) AS AssetsUploaded,
  count(distinct
    CASE
      WHEN SSF.StatusId = 1 THEN AST.ServicerAssetKey
    END
  ) AS UniqueAssetsUploaded
FROM
  DWH.USER_SESSIONS_SERVICER_FACT FUSS
  INNER JOIN DWH.USER_SESSION_FACT FUS ON FUSS.SessionId = FUS.User_Session_Id
  INNER JOIN DWH.USER_DIM U ON U.User_Id = FUS.USERID
  INNER JOIN DWH.SERVICER_DIM S ON S.Servicer_Id = FUSS.SERVICERID
  LEFT JOIN DWH.SERVICER_UPLOAD_FACT SSF ON SSF.src_XMD_INSERTUSERID = FUS.USERID
  AND SSF.src_XMD_INSERTDATETIMESTAMP BETWEEN FUSS.StartTimestamp
  AND ifnull(ifnull(FUSS.EndTimestamp, FUS.LastActivityTimestamp),'2099-01-01')
  left join DWH.ASSET_DIM AST on SSF.Servicer_Source_File_Id = AST.SourceFileID
GROUP BY
  U.Email,
  S.Name,
  cast(fus.logintimestamp AS DATE)

-- COMMAND ----------

select * from DWH.ASSET_UPLOAD_VW

-- COMMAND ----------

CREATE OR REPLACE VIEW DWH.ASSET_DISPOSITION_VW AS
SELECT
  ROW_NUMBER() OVER (
    ORDER BY
      U.Email,
      S.Name
  ) AS Id,
  S.Name AS Servicer,
  U.Email AS User,
  cast(fus.logintimestamp AS DATE) AS LoginDate,
  count(distinct
    CASE
      WHEN SAD.Disposition_StatusId = 2 THEN SAD.Servicer_Asset_Disposition_Id
    END
  ) AS AssetsSaved,
  count(distinct
    CASE
      WHEN SAD.Disposition_StatusId = 2 THEN SAD.ServicerAssetKey
    END
  ) AS UniqueAssetsSaved
FROM
  DWH.USER_SESSIONS_SERVICER_FACT FUSS
  INNER JOIN DWH.USER_SESSION_FACT FUS ON FUSS.SessionId = FUS.User_Session_Id
  INNER JOIN DWH.USER_DIM U ON U.User_Id = FUS.USERID
  INNER JOIN DWH.SERVICER_DIM S ON S.Servicer_Id = FUSS.SERVICERID
  LEFT JOIN DWH.SERVICER_ASSET_DISPOSITION_FACT SAD ON SAD.src_XMD_INSERTUSERID = FUS.USERID
  AND SAD.src_XMD_INSERTDATETIMESTAMP BETWEEN FUSS.StartTimestamp
  AND ifnull(ifnull(FUSS.EndTimestamp, FUS.LastActivityTimestamp),'2099-01-01')
  --left join CBA14.SERVICER_ASSET SA on S.Servicer_Id = SA.ServicerId and SAD.Servicer_Asset_Id = SA.Id
GROUP BY
  U.Email,
  S.Name,
  cast(fus.logintimestamp AS DATE)

-- COMMAND ----------

select * from DWH.ASSET_DISPOSITION_VW

-- COMMAND ----------

CREATE or replace VIEW USEREVENT_VW AS

SELECT
  ROW_NUMBER() OVER (
    ORDER BY
      U.Email,
      S.Name
  ) AS Id,
  S.Name AS Servicer,
  U.Email AS User,
  cast(fus.logintimestamp AS DATE) AS LoginDate,
  COUNT(FUE.User_Event_ID) AS AssetsViewed
FROM
  DWH.USER_SESSIONS_SERVICER_FACT FUSS --table change
  INNER JOIN DWH.USER_SESSION_FACT FUS ON FUSS.SessionId = FUS.User_Session_Id
  INNER JOIN DWH.USER_DIM U ON U.User_Id = FUS.USERID
  INNER JOIN DWH.SERVICER_DIM S ON S.Servicer_Id = FUSS.SERVICERID
  LEFT JOIN DWH.USER_EVENT_FACT FUE ON FUE.src_xmd_InsertUserId = U.User_Id
    AND FUE.src_XMD_INSERTDATETIMESTAMP BETWEEN FUSS.StartTimestamp 
	AND ifnull(ifnull(FUSS.EndTimestamp, FUS.LastActivityTimestamp),'2099-01-01')
GROUP BY
  U.Email,
  S.Name,
  cast(fus.logintimestamp AS DATE)

-- COMMAND ----------

select * from USEREVENT_VW

-- COMMAND ----------

-- CREATE or replace VIEW USAGE_REPORT_VW AS
-- SELECT  
--     PORTFOLIOS.Servicer,
--     PORTFOLIOS.User,
--     PORTFOLIOS.LoginDate,
--     PORTFOLIOS.Logins,
--     PORTFOLIOS.PortfoliosUploaded ,
--     PORTFOLIOS.PortfolioUploadsFailed ,
--     PORTFOLIOS.PortfolioUploadsValidated,
--     ASSET_DISPOSITION.AssetsUploaded,
--     ASSET_DISPOSITION.UniqueAssetsUploaded ,
--     USEREVENT.AssetsViewed ,
--     ASSET_DISPOSITION.AssetsSaved,
--     ASSET_DISPOSITION.UniqueAssetsSaved
-- FROM dwh.PORTFOLIIOS_VW PORTFOLIOS
-- left JOIN dwh.ASSET_DISPOSITION_VW ASSET_DISPOSITION ON (PORTFOLIOS.Servicer = ASSET_DISPOSITION.Servicer and PORTFOLIOS.User = ASSET_DISPOSITION.User and PORTFOLIOS.LoginDate = ASSET_DISPOSITION.LoginDate)
-- left JOIN dwh.USEREVENT_VW USEREVENT ON (USEREVENT.Servicer = PORTFOLIOS.Servicer and USEREVENT.User = PORTFOLIOS.User and USEREVENT.LoginDate = PORTFOLIOS.LoginDate)
--  order by PORTFOLIOS.Servicer;

-- COMMAND ----------

CREATE or replace VIEW USAGE_REPORT_VW AS
SELECT  
    PORTFOLIOS.Id, 
    PORTFOLIOS.Servicer,
    PORTFOLIOS.User,
    PORTFOLIOS.LoginDate, 
    PORTFOLIOS.Logins,

    PORTFOLIOS.PortfoliosUploaded,
    PORTFOLIOS.PortfolioUploadsFailed ,
    PORTFOLIOS.PortfolioUploadsValidated ,
    ASSET_UPLOAD.AssetsUploaded ,
    ASSET_UPLOAD.UniqueAssetsUploaded ,
    USEREVENT.AssetsViewed ,
    ASSET_DISPOSITION.AssetsSaved,
    ASSET_DISPOSITION.UniqueAssetsSaved 
FROM dwh.PORTFOLIIOS_VW PORTFOLIOS
left JOIN dwh.ASSET_DISPOSITION_VW ASSET_DISPOSITION ON (PORTFOLIOS.Servicer = ASSET_DISPOSITION.Servicer and PORTFOLIOS.User = ASSET_DISPOSITION.User and PORTFOLIOS.LoginDate = ASSET_DISPOSITION.LoginDate)
left JOIN dwh.ASSET_UPLOAD_VW ASSET_UPLOAD ON (PORTFOLIOS.Servicer = ASSET_UPLOAD.Servicer and PORTFOLIOS.User = ASSET_UPLOAD.User and PORTFOLIOS.LoginDate = ASSET_UPLOAD.LoginDate)
left JOIN dwh.USEREVENT_VW USEREVENT ON (USEREVENT.Servicer = PORTFOLIOS.Servicer and USEREVENT.User = PORTFOLIOS.User and USEREVENT.LoginDate = PORTFOLIOS.LoginDate)
 order by PORTFOLIOS.Servicer;

-- COMMAND ----------

select * from USAGE_REPORT_VW;