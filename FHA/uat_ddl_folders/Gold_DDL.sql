-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.text("Catalog_name", "", "")
-- MAGIC Catalog_name = dbutils.widgets.get("Catalog_name")
-- MAGIC
-- MAGIC

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS DWH
MANAGED LOCATION 'abfss://gold@adlsdiuatcus.dfs.core.windows.net/fha/managed/dwh';				
USE DWH;

-- COMMAND ----------

CREATE or REPLACE TABLE SERVICER_DIM			
(
SERVICER_DIM_KEY    BIGINT  GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1), 
src_xmd_InsertDateTimestamp	TIMESTAMP			,
src_xmd_InsertUserId	STRING			,
src_xmd_UpdateDateTimestamp	TIMESTAMP			,
src_xmd_UpdateUserId	STRING			,
src_xmd_DeleteDateTimestamp	TIMESTAMP			,
src_xmd_DeleteUserId	STRING			,
src_xmd_ChangeOperationCode	STRING			,
xmd_InsertDateTimestamp	TIMESTAMP		NOT NULL	,
xmd_InsertUserId	STRING		NOT NULL	,
xmd_UpdateDateTimestamp	TIMESTAMP			,
xmd_UpdateUserId	STRING			,
xmd_DeleteDateTimestamp	TIMESTAMP			,
xmd_DeleteUserId	STRING			,
xmd_ChangeOperationCode	STRING			,
DeleteInd	BOOLEAN			,
SERVICER_Id	STRING		NOT NULL	,
Name	STRING		,
ShortName STRING,
EffectiveDate	DATE,
ExpiryDate	DATE,
IsCurrent	BOOLEAN 
) USING DELTA
location 'abfss://gold@adlsdiuatcus.dfs.core.windows.net/fha/external/dwh/SERVICER_DIM';

-- COMMAND ----------

CREATE OR REPLACE TABLE USER_DIM			
(
USER_DIM_KEY    BIGINT  GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1), 
src_xmd_InsertDateTimestamp	TIMESTAMP			,
src_xmd_InsertUserId	STRING			,
src_xmd_UpdateDateTimestamp	TIMESTAMP			,
src_xmd_UpdateUserId	STRING			,
src_xmd_DeleteDateTimestamp	TIMESTAMP			,
src_xmd_DeleteUserId	STRING			,
src_xmd_ChangeOperationCode	STRING			,
xmd_InsertDateTimestamp	TIMESTAMP		NOT NULL	,
xmd_InsertUserId	STRING		NOT NULL	,
xmd_UpdateDateTimestamp	TIMESTAMP			,
xmd_UpdateUserId	STRING			,
xmd_DeleteDateTimestamp	TIMESTAMP			,
xmd_DeleteUserId	STRING			,
xmd_ChangeOperationCode	STRING			,
DeleteInd	BOOLEAN			,
User_Id	STRING		NOT NULL	,
Email	VARCHAR(255)		NOT NULL	,
FirstName	VARCHAR(255)		NOT NULL	,
LastName	VARCHAR(255)		NOT NULL	,
StatusId	INT		NOT NULL	,
LastLoginDateTimestamp	TIMESTAMP			,
B2CUserId	STRING		NOT NULL	,
EffectiveDate	DATE,
ExpiryDate	DATE,
IsCurrent	BOOLEAN 
) USING DELTA
location 'abfss://gold@adlsdiuatcus.dfs.core.windows.net/fha/external/dwh/USER_DIM';

-- COMMAND ----------

CREATE or REPLACE TABLE ASSET_DIM
(
ID STRING,
ASSET_DIM_KEY  BIGINT  GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1), 
src_xmd_InsertDateTimestamp	TIMESTAMP			,
src_xmd_InsertUserId	STRING			,
src_xmd_UpdateDateTimestamp	TIMESTAMP			,
src_xmd_UpdateUserId	STRING			,
src_xmd_DeleteDateTimestamp	TIMESTAMP			,
src_xmd_DeleteUserId	STRING			,
src_xmd_ChangeOperationCode	STRING			,
xmd_InsertDateTimestamp	TIMESTAMP		NOT NULL	,
xmd_InsertUserId	STRING		NOT NULL	,
xmd_UpdateDateTimestamp	TIMESTAMP			,
xmd_UpdateUserId	STRING			,
xmd_DeleteDateTimestamp	TIMESTAMP			,
xmd_DeleteUserId	STRING			,
xmd_ChangeOperationCode	STRING			,
DeleteInd	BOOLEAN			,
Asset_Key	STRING,
ServicerAssetKey STRING,
XPK_Key	STRING,
SourceFileID STRING,
StreetAddress	STRING,
CityName	STRING,
StateCode	STRING,
ZipCode	STRING,
EffectiveDate	DATE,
ExpiryDate	DATE,
IsCurrent	BOOLEAN 
) USING DELTA
location 'abfss://gold@adlsdiuatcus.dfs.core.windows.net/fha/external/dwh/ASSET_DIM';


-- COMMAND ----------

CREATE OR REPLACE TABLE DATE_DIM (
    DATE_DIM_KEY BIGINT  GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1), 
    CALENDAR_DATE DATE ,
    DAY INT  ,
    MONTH INT  ,
    YEAR INT ,
    QUARTER INT,
    WEEK_OF_YEAR INT ,
    DAY_OF_WEEK INT ,
    DAY_NAME STRING ,
    MONTH_NAME STRING ,
    WEEKEND_IND BOOLEAN ,
    YEAR_MONTH STRING,
    HOLIDAY_IND BOOLEAN 
)
USING DELTA
PARTITIONED BY (YEAR)
location  'abfss://gold@adlsdiuatcus.dfs.core.windows.net/fha/external/dwh/DATE_DIM'


-- COMMAND ----------

CREATE OR REPLACE TABLE USER_SESSION_FACT (
USER_DIM_KEY BIGINT,
DATE_DIM_KEY BIGINT,
src_xmd_InsertDateTimestamp	TIMESTAMP		NOT NULL	,
src_xmd_InsertUserId	STRING		NOT NULL	,
src_xmd_UpdateDateTimestamp	TIMESTAMP			,
src_xmd_UpdateUserId	STRING			,
src_xmd_DeleteDateTimestamp	TIMESTAMP			,
src_xmd_DeleteUserId	STRING			,
src_xmd_ChangeOperationCode	STRING	,
xmd_InsertDateTimestamp	TIMESTAMP		NOT NULL	,
xmd_InsertUserId	STRING		NOT NULL	,
xmd_UpdateDateTimestamp	TIMESTAMP			,
xmd_UpdateUserId	STRING			,
xmd_DeleteDateTimestamp	TIMESTAMP			,
xmd_DeleteUserId	STRING			,
xmd_ChangeOperationCode	STRING			,
DeleteInd	BOOLEAN	,  
User_Session_Id	STRING	NOT NULL	,
UserId	STRING	NOT NULL	,
LoginTimestamp	TIMESTAMP		,
LogoutTimestamp	TIMESTAMP		,
LastActivityTimestamp	TIMESTAMP		,
ExpirationTimestamp	TIMESTAMP	NOT NULL	,
IPAddressCode	STRING		,
UserAgentCode	STRING		,
InteractionCount	INT	NOT NULL	,
ActiveInd	BOOLEAN	NOT NULL	
 ) USING DELTA
location 'abfss://gold@adlsdiuatcus.dfs.core.windows.net/fha/external/dwh/USER_SESSION_FACT';

-- COMMAND ----------

CREATE OR REPLACE TABLE SERVICER_UPLOAD_FACT(
Servicer_Dim_Key BIGINT,
User_Dim_Key  BIGINT,
Date_Dim_Key BIGINT,
src_xmd_InsertDateTimestamp	TIMESTAMP			,
src_xmd_InsertUserId	STRING			,
src_xmd_UpdateDateTimestamp	TIMESTAMP			,
src_xmd_UpdateUserId	STRING			,
src_xmd_DeleteDateTimestamp	TIMESTAMP			,
src_xmd_DeleteUserId	STRING			,
src_xmd_ChangeOperationCode	STRING			,
xmd_InsertDateTimestamp	TIMESTAMP		NOT NULL	,
xmd_InsertUserId	STRING		NOT NULL	,
xmd_UpdateDateTimestamp	TIMESTAMP			,
xmd_UpdateUserId	STRING			,
xmd_DeleteDateTimestamp	TIMESTAMP			,
xmd_DeleteUserId	STRING			,
xmd_ChangeOperationCode	STRING			,
DeleteInd	BOOLEAN			,
--SessionId STRING ,
Servicer_Source_File_Id	STRING		NOT NULL	,
ServicerId	STRING	 	NOT NULL	,
UserId	STRING		NOT NULL	,
FileName	STRING		NOT NULL	,
Path	STRING		NOT NULL	,
StatusId	INT		NOT NULL	,
Message	STRING
--,CONSTRAINT SERVICER_FK FOREIGN KEY(Id ) REFERENCES CBR4.servicer
) USING DELTA
location 'abfss://gold@adlsdiuatcus.dfs.core.windows.net/fha/external/dwh/SERVICER_UPLOAD_FACT';

-- COMMAND ----------

CREATE OR REPLACE TABLE USER_EVENT_FACT
(
xmd_InsertDateTimestamp	TIMESTAMP		NOT NULL	,
xmd_InsertUserId	STRING		NOT NULL	,
xmd_UpdateDateTimestamp	TIMESTAMP			,
xmd_UpdateUserId	STRING			,
xmd_DeleteDateTimestamp	TIMESTAMP			,
xmd_DeleteUserId	STRING			,
xmd_ChangeOperationCode	STRING			,
src_xmd_InsertDateTimestamp	TIMESTAMP		NOT NULL	,
src_xmd_InsertUserId	STRING		NOT NULL	,
src_xmd_UpdateDateTimestamp	TIMESTAMP			,
src_xmd_UpdateUserId	STRING			,
src_xmd_DeleteDateTimestamp	TIMESTAMP			,
src_xmd_DeleteUserId	STRING			,
src_xmd_ChangeOperationCode	STRING			,
User_Event_ID STRING,
DeleteInd BOOLEAN ,
EventTypeName STRING,
Action STRING ,
EventDetails STRING
)USING DELTA
location  'abfss://gold@adlsdiuatcus.dfs.core.windows.net/fha/external/dwh/USER_EVENT_FACT';

-- COMMAND ----------

CREATE OR REPLACE TABLE SERVICER_ASSET_DISPOSITION_FACT
(
ASSET_DIM_KEY BIGINT,
DATE_DIM_KEY BIGINT,
SERVICER_DIM_KEY BIGINT,

src_xmd_InsertDateTimestamp	TIMESTAMP			,
src_xmd_InsertUserId	STRING			,
src_xmd_UpdateDateTimestamp	TIMESTAMP			,
src_xmd_UpdateUserId	STRING			,
src_xmd_DeleteDateTimestamp	TIMESTAMP			,
src_xmd_DeleteUserId	STRING			,
src_xmd_ChangeOperationCode	STRING			,
xmd_InsertDateTimestamp	TIMESTAMP		NOT NULL	,
xmd_InsertUserId	STRING		NOT NULL	,
xmd_UpdateDateTimestamp	TIMESTAMP			,
xmd_UpdateUserId	STRING			,
xmd_DeleteDateTimestamp	TIMESTAMP			,
xmd_DeleteUserId	STRING			,
xmd_ChangeOperationCode	STRING			,
DeleteInd	BOOLEAN			,
Servicer_Asset_Disposition_Id STRING,
Servicer_Asset_Id	STRING		NOT NULL	,
XpkKey	STRING		NOT NULL	,
ServicerSystemExtractDate	DATE		NOT NULL	,
ServicerAssetKey	STRING		NOT NULL	,
ServicerId STRING NOT NULL,
SourceFileId	STRING		NOT NULL	,
StreetAddress	STRING		NOT NULL	,
CityName	STRING		NOT NULL	,
StateCode	STRING		NOT NULL	,
ZipCode	STRING		NOT NULL	,
PropertyLoanBalanceAmount	DECIMAL(18,2)		NOT NULL	,
PropertyLoanAccruedInterestAmount	DECIMAL(18,2)		NOT NULL	,
ServicerNonRecoverableAdvanceBalanceAmount	DECIMAL(18,2)		NOT NULL	,
ServicerRecoverableAdvanceBalanceAmount	DECIMAL(18,2)		NOT NULL	,
ServicerRecoverable3rdPartyAdvanceBalanceAmount	DECIMAL(18,2)		NOT NULL	,
ServicerEscrowAdvanceBalanceAmount	DECIMAL(18,2)		NOT NULL	,
ServicerUnappliedBorrowerFundAmount	DECIMAL(18,2)		NOT NULL	,
InsuranceLostDraftProceedAmount	DECIMAL(18,2)		NOT NULL	,
FhaProjectedPropertyValueAmount	DECIMAL(18,2)		NOT NULL	,
PropertyTypeCode	STRING			,
LoanFirstPaymentDueDate	DATE			,
LoanNextPaymentDueDate	DATE		NOT NULL	,
FhaServicerForeclosureChecklistCompleteDate	DATE			,
ForeclosureTargetStartDate	DATE		NOT NULL	,
ForeclosureExtendedStartDate	DATE		NOT NULL	,
ForeclosureStartDate	DATE		NOT NULL	,
ForeclosureScheduleSaleDate	DATE			,
ForeclosureSaleDate	DATE			,
FhaForeclosureDiligenceStartDate	DATE			,
StatutoryRightOfRedemptionStateInd	STRING			,
StatutoryRightOfRedemptionExpirationDate	DATE			,
StatutoryRightOfRedemptionCompletionDate	DATE			,
ForeclosureEvictionTargetStartDate	DATE			,
ForeclosureEvictionExtendedStartDate	DATE			,
ForeclosureEvictionStartDate	DATE			,
TargetHudConveyanceDate	DATE			,
ExtendedHudConveyanceDate	DATE			,
HudConveyanceDate	DATE			,
ForeclosureFirstVacancyDate	DATE			,
ForeclosureHoldDate	DATE			,
ForeclosureHoldDescriptionText	STRING			,
BorrowerOccupationalClassificationCode	STRING		NOT NULL	,
ForeclosureBidReserveAmount	DECIMAL(18,2)			,
ServicerLoanOnboardingDate	DATE			,
LoanInterestRate	DECIMAL(6,3)		NOT NULL	,
PostSaleClearTitleDate	DATE			,
FhaCurtailFirstLegalInd	STRING		NOT NULL	,
CurtailDueDiligenceInd	STRING		NOT NULL	,
DiligenceTargetStartDate	DATE			,
DiligenceExtendedStartDate	DATE			,
SoldTo3RdPartyInd	STRING			,
CwcotEligibilityInd	STRING		NOT NULL,
Disposition_StatusId	INT		,
Disposition_StatusName STRING,
RunDate	DATE	NOT NULL	,
FHA203kFundsAmount	DECIMAL(38,19)	NOT NULL	,
CurrentNetExposureTotalAmount	DECIMAL(38,19)	NOT NULL	,
DebentureRate	DECIMAL(38,19)	NOT NULL	,
DebentureInterestDate	DATE	NOT NULL	,
DefaultDate	DATE	NOT NULL	,
FCLApprovalDate	DATE		,
FirstLegalDueDate	DATE	NOT NULL	,
FirstLegalCurtailmentInd	BOOLEAN	NOT NULL	,
ForeclosureSaleDateActual	DATE	NOT NULL	,
DueDiligenceTargetDate	DATE	NOT NULL	,
DueDiligenceCurtailmentInd	BOOLEAN	NOT NULL	,
EvictionFirstLegalTargetDate	DATE		,
EvictionStartExtendedDate	DATE		,
FirstTimeVacancyDate	DATE		,
EvictionStartActualDate	DATE		,
EvictionStartCurtailmentInd	BOOLEAN	NOT NULL	,
ConveyanceTargetDateCalculated	DATE	NOT NULL	,
ConveyanceExtendedDate	DATE		,
ConveyanceActualDate	DATE		,
ConveyanceCurtailmentInd	BOOLEAN	NOT NULL	,
CurtailmentStatus	STRING	NOT NULL	,
Disposition_ForeclosureHoldDate	DATE		,
ForeclosureHoldReason	STRING		,
RRCFollowUpDate	DATE		,
RRCCompletionDate	DATE		,
TitleGrade	STRING		,
FHATitleIssueId	STRING	NOT NULL	,
TitleCureTime	INT	NOT NULL	,
AppraisalStatus	STRING	NOT NULL	,
AppraisalTypeNeeded	STRING	NOT NULL	

--CONSTRAINT SERVICER_SOURCE_FILE_FK FOREIGN KEY(Id ) REFERENCES CBA14.SERVICER_SOURCE_FILE
) USING DELTA
location 'abfss://gold@adlsdiuatcus.dfs.core.windows.net/fha/external/dwh/SERVICER_ASSET_DISPOSITION_FACT';

-- COMMAND ----------

USE dwh;
 
CREATE OR REPLACE TABLE USER_SESSIONS_SERVICER_FACT
(
ID STRING   ,
xmd_InsertDateTimestamp TIMESTAMP    ,
xmd_InsertUserId  STRING      ,
xmd_UpdateDateTimestamp TIMESTAMP     ,
xmd_UpdateUserId  STRING      ,
xmd_DeleteDateTimestamp TIMESTAMP     ,
xmd_DeleteUserId  STRING      ,
xmd_ChangeOperationCode STRING      ,
src_xmd_InsertDateTimestamp TIMESTAMP     ,
src_xmd_InsertUserId  STRING      ,
src_xmd_UpdateDateTimestamp TIMESTAMP     ,
src_xmd_UpdateUserId  STRING      ,
src_xmd_DeleteDateTimestamp TIMESTAMP     ,
src_xmd_DeleteUserId  STRING      ,
src_xmd_ChangeOperationCode STRING      ,
DeleteInd BOOLEAN  ,
SessionId STRING  ,
ServicerId STRING  ,
StartTimestamp TIMESTAMP  ,
EndTimestamp TIMESTAMP ,
ActiveInd BOOLEAN 
) USING DELTA
location 'abfss://gold@adlsdiuatcus.dfs.core.windows.net/fha/external/dwh/USER_SESSIONS_SERVICER_FACT'