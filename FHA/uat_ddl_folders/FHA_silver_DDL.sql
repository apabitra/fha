-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.text("Catalog_name", "", "")
-- MAGIC Catalog_name = dbutils.widgets.get("Catalog_name")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"use catalog {Catalog_name}")

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS CBR18 
MANAGED LOCATION 'abfss://silver@adlsdiuatcus.dfs.core.windows.net/fha/managed/cbr18';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS CBR18 
MANAGED LOCATION 'abfss://silver@adlsdiuatcus.dfs.core.windows.net/fha/managed/cbr18';

CREATE SCHEMA IF NOT EXISTS CBR16
MANAGED LOCATION 'abfss://silver@adlsdiuatcus.dfs.core.windows.net/fha/managed/cbr16';	

CREATE SCHEMA IF NOT EXISTS CBR17
MANAGED LOCATION 'abfss://silver@adlsdiuatcus.dfs.core.windows.net/fha/managed/cbr17';	

CREATE SCHEMA IF NOT EXISTS CBR20
MANAGED LOCATION 'abfss://silver@adlsdiuatcus.dfs.core.windows.net/fha/managed/cbr20';		

CREATE SCHEMA IF NOT EXISTS CBR4
MANAGED LOCATION 'abfss://silver@adlsdiuatcus.dfs.core.windows.net/fha/managed/cbr4';		

CREATE SCHEMA IF NOT EXISTS CBA14
MANAGED LOCATION 'abfss://silver@adlsdiuatcus.dfs.core.windows.net/fha/managed/cbr14';		

-- COMMAND ----------

						
USE CBR18;						
                       
CREATE OR REPLACE TABLE HUD_CAFMV (                        
Id	STRING	NOT NULL 		,
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
StartDate	DATE		NOT NULL	,
EndDate	DATE		NOT NULL	,
StateCode	STRING		NOT NULL	,
PropertyTypeCode	STRING		NOT NULL	,
ValueBeginAmount	DECIMAL(12,0)		NOT NULL	,
ValueEndAmount	DECIMAL(12,0)			,
ValueAdjPercent	DECIMAL(3,2)		NOT NULL	
   ) USING DELTA
location 'abfss://silver@adlsdiuatcus.dfs.core.windows.net/fha/external/cbr18/HUD_CAFMV';

-- COMMAND ----------

						
						
						
USE  CBR18;	
					
CREATE OR REPLACE TABLE HUD_DUE_DILIGENCE_TIMELINE(						
Id	STRING		NOT NULL	,
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
StartDate	DATE		NOT NULL	,
EndDate	DATE		NOT NULL	,
StateCode	STRING		NOT NULL	,
ReasonableDiligenceElapsedMonthCount	INT		NOT NULL	,
ReasonableDiligenceElapsedDayCount	INT		NOT NULL	
) USING DELTA
location 'abfss://silver@adlsdiuatcus.dfs.core.windows.net/fha/external/cbr18/HUD_DUE_DILIGENCE_TIMELINE';

-- COMMAND ----------

						
						
USE  CBR18;	
						
CREATE OR REPLACE TABLE HUD_FUTURE_EXPENSES (						
Id	STRING		NOT NULL	,
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
StartDate	DATE		NOT NULL	,
EndDate	DATE		NOT NULL	,
FutureExpenseName	STRING		NOT NULL	,
FutureExpenseMonthlyFrequency	DECIMAL(8,6)		NOT NULL	,
FutureExpenseCost	DECIMAL(12,2)							
) USING DELTA
location 'abfss://silver@adlsdiuatcus.dfs.core.windows.net/fha/external/cbr18/HUD_FUTURE_EXPENSES';

-- COMMAND ----------

		
USE CBR16;	
		
CREATE OR REPLACE TABLE XOME_CLOSING_COST (			
Id	STRING		NOT NULL	,
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
StartDate	DATE		NOT NULL	,
EndDate	DATE		NOT NULL	,
ClosingVariableCode	STRING		NOT NULL	,
ClosingCoeffAmount	DECIMAL(38,18)		NOT NULL		
) USING DELTA
location 'abfss://silver@adlsdiuatcus.dfs.core.windows.net/fha/external/cbr16/XOME_CLOSING_COST';

-- COMMAND ----------

			
USE CBR16;	
		
CREATE OR REPLACE TABLE XOME_ESCROW (			
Id	STRING		NOT NULL	,
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
StartDate	DATE		NOT NULL	,
EndDate	DATE		NOT NULL	,
EscrowCoeffCode	STRING		NOT NULL	,
EscrowCoeffValue	DECIMAL(38,18)		NOT NULL		
) USING DELTA
location 'abfss://silver@adlsdiuatcus.dfs.core.windows.net/fha/external/cbr16/XOME_ESCROW';

-- COMMAND ----------

				
USE CBR16;	
		
CREATE OR REPLACE TABLE XOME_TITLE_CURE			
 (			
Id	STRING		NOT NULL	,
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
StartDate	DATE		NOT NULL	,
EndDate	DATE		NOT NULL	,
TitleIssueDescription	STRING		NOT NULL	,
TitleGradeTypeCode	STRING			,
TitleIssueCureDayCount	INT		NOT NULL		
) USING DELTA 
location 'abfss://silver@adlsdiuatcus.dfs.core.windows.net/fha/external/cbr16/XOME_TITLE_CURE';	

-- COMMAND ----------

				
USE CBR16;
			
CREATE OR REPLACE TABLE XOME_FHA_MARKETING 			
(			
Id	STRING		NOT NULL	,
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
StartDate	DATE		NOT NULL	,
EndDate	DATE		NOT NULL	,
StateCode	STRING		NOT NULL	,
CwcotMarketingDayCount	INT		NOT NULL	,
ReoMarketingDayCount	INT		NOT NULL	
) USING DELTA 
location 'abfss://silver@adlsdiuatcus.dfs.core.windows.net/fha/external/cbr16/XOME_FHA_MARKETING';

-- COMMAND ----------

		
USE CBR17;	
		
CREATE OR REPLACE TABLE FED_DEBENTURE_RATE			
(			
Id	STRING		NOT NULL	,
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
StartDate	DATE		NOT NULL	,
EndDate	DATE		NOT NULL	,
DebentureRate	DECIMAL(10,6)		NOT NULL		
) USING DELTA
location 'abfss://silver@adlsdiuatcus.dfs.core.windows.net/fha/external/cbr17/FED_DEBENTURE_RATE';

-- COMMAND ----------

		
USE CBR20;	
		
CREATE OR REPLACE TABLE USFN_EVICTION			
(			
Id	STRING		NOT NULL	,
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
StartDate	DATE		NOT NULL	,
EndDate	DATE		NOT NULL	,
StateCode	STRING		NOT NULL	,
UsfnYearCode	INT		NOT NULL	,
UsfnElapsedDayCount	INT		NOT NULL	,
HudEvictionCost	DECIMAL(16,2)		NOT NULL		
) USING DELTA
location 'abfss://silver@adlsdiuatcus.dfs.core.windows.net/fha/external/cbr20/TABLE USFN_EVICTION';

-- COMMAND ----------


			
	
USE CBR4;

CREATE OR REPLACE TABLE SERVICER			
(
Id	STRING		NOT NULL	,
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
Name	STRING		,
ShortName STRING 
) USING DELTA
location 'abfss://silver@adlsdiuatcus.dfs.core.windows.net/fha/external/cbr4/SERVICER';


-- COMMAND ----------

		
USE CBA14;

CREATE OR REPLACE TABLE SERVICER_SOURCE_FILE(
Id	STRING		NOT NULL	,
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
ServicerId	STRING	 	NOT NULL	,
UserId	STRING		NOT NULL	,
FileName	STRING		NOT NULL	,
Path	STRING		NOT NULL	,
StatusId	INT		NOT NULL	,
Message	STRING
--,CONSTRAINT SERVICER_FK FOREIGN KEY(Id ) REFERENCES CBR4.servicer
) USING DELTA
location 'abfss://silver@adlsdiuatcus.dfs.core.windows.net/fha/external/cbr14/SERVICER_SOURCE_FILE';





-- COMMAND ----------


USE CBA14;


CREATE OR REPLACE TABLE SERVICER_ASSET
(
Id	STRING		NOT NULL	,
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
CwcotEligibilityInd	STRING		NOT NULL
--CurtailedForeclosureEvictionInd	STRING			,
--ProjectedDueDiligenceTargetDate	DATE			
--CONSTRAINT SERVICER_SOURCE_FILE_FK FOREIGN KEY(Id ) REFERENCES CBA14.SERVICER_SOURCE_FILE
) USING DELTA
location 'abfss://silver@adlsdiuatcus.dfs.core.windows.net/fha/external/cba14/SERVICER_ASSET';


-- COMMAND ----------


		
USE CBA14;

CREATE OR REPLACE TABLE FHA_USER
(
Id	STRING		NOT NULL	,
src_xmd_InsertDateTimestamp	TIMESTAMP		NOT NULL	,
src_xmd_InsertUserId	STRING			,
src_xmd_UpdateDateTimestamp	TIMESTAMP	,
src_xmd_UpdateUserId	STRING			,
src_xmd_DeleteDateTimestamp	TIMESTAMP			,
src_xmd_DeleteUserId	STRING			,
src_xmd_ChangeOperationCode	STRING			,
xmd_InsertDateTimestamp	TIMESTAMP		NOT NULL	,
xmd_InsertUserId	STRING			,
xmd_UpdateDateTimestamp	TIMESTAMP			,
xmd_UpdateUserId	STRING			,
xmd_DeleteDateTimestamp	TIMESTAMP			,
xmd_DeleteUserId	STRING			,
xmd_ChangeOperationCode	STRING			,
DeleteInd	BOOLEAN			,
Email	VARCHAR(255)		NOT NULL	,
FirstName	VARCHAR(255)		NOT NULL	,
LastName	VARCHAR(255)		NOT NULL	,
StatusId	INT		NOT NULL	,
LastLoginDateTimestamp	TIMESTAMP			,
B2CUserId	STRING		NOT NULL	
) USING DELTA
location 'abfss://silver@adlsdiuatcus.dfs.core.windows.net/fha/external/cba14/FHA_USER';


-- COMMAND ----------

	
USE CBA14;

CREATE OR REPLACE TABLE ROLE
(
Id	STRING		NOT NULL	,
src_xmd_InsertDateTimestamp	TIMESTAMP		NOT NULL	,
src_xmd_InsertUserId	STRING		NOT NULL	,
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
Name	VARCHAR(255)		NOT NULL	
) USING DELTA
location 'abfss://silver@adlsdiuatcus.dfs.core.windows.net/fha/external/cba14/ROLE';


-- COMMAND ----------

			
USE CBA14;

CREATE OR REPLACE TABLE USER_ROLE
(
UserId	STRING		NOT NULL	,
RoleId	STRING		NOT NULL	,
src_xmd_InsertDateTimestamp	TIMESTAMP		NOT NULL	,
src_xmd_InsertUserId	STRING		NOT NULL	,
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
DeleteInd	BOOLEAN			
) USING DELTA
location 'abfss://silver@adlsdiuatcus.dfs.core.windows.net/fha/external/cba14/USER_ROLE';




-- COMMAND ----------


		
USE CBA14;

CREATE OR REPLACE TABLE ROLE_PERMISSION
(
RoleId	STRING		NOT NULL	,
PermissionId	STRING		NOT NULL	,
src_xmd_InsertDateTimestamp	TIMESTAMP		NOT NULL	,
src_xmd_InsertUserId	STRING		NOT NULL	,
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
DeleteInd	BOOLEAN			
) USING DELTA
location 'abfss://silver@adlsdiuatcus.dfs.core.windows.net/fha/external/cba14/ROLE_PERMISSION';


-- COMMAND ----------

			
USE CBA14;

CREATE OR REPLACE TABLE PERMISSION
(
Id	STRING		NOT NULL	,
src_xmd_InsertDateTimestamp	TIMESTAMP		NOT NULL	,
src_xmd_InsertUserId	STRING		NOT NULL	,
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
Name	VARCHAR(255)		NOT NULL	,
ModuleId	STRING		NOT NULL	
) USING DELTA
location 'abfss://silver@adlsdiuatcus.dfs.core.windows.net/fha/external/cba14/PERMISSION';



-- COMMAND ----------

		
USE CBA14;

CREATE OR REPLACE TABLE MODULE
(
Id	STRING		NOT NULL	,
src_xmd_InsertDateTimestamp	TIMESTAMP		NOT NULL	,
src_xmd_InsertUserId	STRING		NOT NULL	,
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
Name	VARCHAR(255)		NOT NULL	,
Description	VARCHAR(1000)		 	
) USING DELTA
location 'abfss://silver@adlsdiuatcus.dfs.core.windows.net/fha/external/cba14/MODULE';




-- COMMAND ----------

		
USE CBA14;

CREATE OR REPLACE TABLE USER_SERVICER
(
UserId	STRING		NOT NULL	,
ServicerId	STRING		NOT NULL	,
src_xmd_InsertDateTimestamp	TIMESTAMP		NOT NULL	,
src_xmd_InsertUserId	STRING		NOT NULL	,
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
DeleteInd	BOOLEAN			
) USING DELTA
location 'abfss://silver@adlsdiuatcus.dfs.core.windows.net/fha/external/cba14/USER_SERVICER';


-- COMMAND ----------

			
USE CBA14;

CREATE OR REPLACE TABLE CBA14.FHA_USER_STATUS (
Id INT,
xmd_InsertDateTimestamp	TIMESTAMP		NOT NULL	,
xmd_InsertUserId	STRING		NOT NULL	,
xmd_UpdateDateTimestamp	TIMESTAMP			,
xmd_UpdateUserId	STRING			,
xmd_DeleteDateTimestamp	TIMESTAMP			,
xmd_DeleteUserId	STRING			,
xmd_ChangeOperationCode	STRING			,
Name STRING
) USING DELTA
location 'abfss://silver@adlsdiuatcus.dfs.core.windows.net/fha/external/cba14/FHA_USER_STATUS';





-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------

		
USE CBA14;


CREATE OR REPLACE TABLE CBA14.SERVICER_SOURCE_FILE_STATUS (
    Id INT,
xmd_InsertDateTimestamp	TIMESTAMP		NOT NULL	,
xmd_InsertUserId	STRING		NOT NULL	,
xmd_UpdateDateTimestamp	TIMESTAMP			,
xmd_UpdateUserId	STRING			,
xmd_DeleteDateTimestamp	TIMESTAMP			,
xmd_DeleteUserId	STRING			,
xmd_ChangeOperationCode	STRING			,
    Name STRING
) USING DELTA
location 'abfss://silver@adlsdiuatcus.dfs.core.windows.net/fha/external/cba14/SERVICER_SOURCE_FILE_STATUS';





-- COMMAND ----------

			
USE CBA14;

CREATE OR REPLACE TABLE CBA14.WORKER_SERVICE_PROCESS_INFO (
    Id STRING NOT NULL,
src_xmd_InsertDateTimestamp	TIMESTAMP		NOT NULL	,
src_xmd_InsertUserId	STRING			,
src_xmd_UpdateDateTimestamp	TIMESTAMP			,
src_xmd_UpdateUserId	STRING			,
src_xmd_DeleteDateTimestamp	TIMESTAMP			,
src_xmd_DeleteUserId	STRING			,
src_xmd_ChangeOperationCode	STRING	,
xmd_InsertDateTimestamp	TIMESTAMP		NOT NULL	,
xmd_InsertUserId	STRING			,
xmd_UpdateDateTimestamp	TIMESTAMP			,
xmd_UpdateUserId	STRING			,
xmd_DeleteDateTimestamp	TIMESTAMP			,
xmd_DeleteUserId	STRING			,
xmd_ChangeOperationCode	STRING			,
DeleteInd	BOOLEAN	,  
    Name STRING,
    IsActive BOOLEAN,  
    Interval INT,
    CronExpression STRING,  
    IsListener BOOLEAN
 ) USING DELTA
location 'abfss://silver@adlsdiuatcus.dfs.core.windows.net/fha/external/cba14/WORKER_SERVICE_PROCESS_INFO';



-- COMMAND ----------

				
USE CBA14;
CREATE OR REPLACE TABLE CBA14.DISPOSITION_PATH_TYPE(
	Id	INT	NOT NULL,
xmd_InsertDateTimestamp	TIMESTAMP		NOT NULL	,
xmd_InsertUserId	STRING		NOT NULL	,
xmd_UpdateDateTimestamp	TIMESTAMP			,
xmd_UpdateUserId	STRING			,
xmd_DeleteDateTimestamp	TIMESTAMP			,
xmd_DeleteUserId	STRING			,
xmd_ChangeOperationCode	STRING			,
	Name	STRING	NOT NULL
) USING DELTA
location 'abfss://silver@adlsdiuatcus.dfs.core.windows.net/fha/external/cba14/DISPOSITION_PATH_TYPE';


-- COMMAND ----------

		
USE CBA14;
CREATE OR REPLACE TABLE CBA14.DISPOSITION_STATUS_TYPE(
	Id	INT	NOT NULL,
xmd_InsertDateTimestamp	TIMESTAMP		NOT NULL	,
xmd_InsertUserId	STRING		NOT NULL	,
xmd_UpdateDateTimestamp	TIMESTAMP			,
xmd_UpdateUserId	STRING			,
xmd_DeleteDateTimestamp	TIMESTAMP			,
xmd_DeleteUserId	STRING			,
xmd_ChangeOperationCode	STRING			,
	Name	STRING	NOT NULL
) USING DELTA
location 'abfss://silver@adlsdiuatcus.dfs.core.windows.net/fha/external/cba14/DISPOSITION_STATUS_TYPE';


-- COMMAND ----------

	
USE CBA14;

CREATE OR REPLACE TABLE CBA14.FHA_USER_SESSIONS (
Id	STRING	NOT NULL	,
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
UserId	STRING	NOT NULL	,
LoginTimestamp	TIMESTAMP		,
LogoutTimestamp	TIMESTAMP		,
LastActivityTimestamp	TIMESTAMP		,
ExpirationTimestamp	TIMESTAMP	NOT NULL	,
IPAddress	STRING		,
UserAgent	STRING		,
InteractionCount	INT	NOT NULL	,
IsActive	BOOLEAN	NOT NULL	
 ) USING DELTA
location 'abfss://silver@adlsdiuatcus.dfs.core.windows.net/fha/external/cba14/FHA_USER_SESSIONS';

-- COMMAND ----------

				
USE CBA14;



CREATE OR REPLACE TABLE CBA14.SERVICER_ASSET_DISPOSITION (
Id STRING,
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
--SessionId	STRING		,
ServicerAssetId	STRING	NOT NULL	,
StatusId	INT	NOT NULL	,
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
ForeclosureHoldDate	DATE		,
ForeclosureHoldReason	STRING		,
RRCFollowUpDate	DATE		,
RRCCompletionDate	DATE		,
TitleGrade	STRING		,
FHATitleIssueId	STRING	NOT NULL	,
TitleCureTime	INT	NOT NULL	,
AppraisalStatus	STRING	NOT NULL	,
AppraisalTypeNeeded	STRING	NOT NULL	
    ) USING DELTA
location 'abfss://silver@adlsdiuatcus.dfs.core.windows.net/fha/external/cba14/SERVICER_ASSET_DISPOSITION';


-- COMMAND ----------

			
USE CBA14;

CREATE OR REPLACE TABLE CBA14.SERVICER_ASSET_DISPOSITION_PATH (
    Id STRING,
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
DispositionId	STRING	NOT NULL	,
DispositionPathTypeId	INT	NOT NULL	,
EventDate	DATE	NOT NULL	,
AdjustIfPastConveyanceTarget	INT	NOT NULL	,
FHAEvictionDays	INT	NOT NULL	,
FHARepairsDays	INT	NOT NULL	,
MarketingDays	INT	NOT NULL	,
TitleCurativeDays	INT	NOT NULL	,
FHADateAdjustmentDays	INT	NOT NULL	,
ProjectedDispositionDate	DATE	NOT NULL	,
CurrentPropertyValueAmount	DECIMAL(38,19)	NOT NULL	,
CurrentPropertyValueType	INT	NOT NULL	,
CAFMVHaircut	DECIMAL(38,19)	NOT NULL	,
CAFMV	DECIMAL(38,19)	NOT NULL	,
FHAValueAdjustment	DECIMAL(38,19)	NOT NULL	,
ProjectedSalesPrice	DECIMAL(38,19)	NOT NULL	,
ClosingCostAdjustmentFactor	DECIMAL(38,19)	NOT NULL	,
ProjectedNetCashfromClosing	DECIMAL(38,19)	NOT NULL	,
FHAEvictionCost	DECIMAL(38,19)	NOT NULL	,
FHACashForKeysCost	DECIMAL(38,19)	NOT NULL	,
TotalProjectedEvictionCosts	DECIMAL(38,19)	NOT NULL	,
LawnMaintenance	DECIMAL(38,19)	NOT NULL	,
FHAInitialSecure	DECIMAL(38,19)	NOT NULL	,
FHAWinterization	DECIMAL(38,19)	NOT NULL	,
FHALockChange	DECIMAL(38,19)	NOT NULL	,
FHADebrisRemoval	DECIMAL(38,19)	NOT NULL	,
FHAPoolSecuringInd	BOOLEAN	NOT NULL	,
FHAPoolSecuring	DECIMAL(38,19)	NOT NULL	,
FHASalesClean	DECIMAL(38,19)	NOT NULL	,
FHAOtherPropPreservationExpense	DECIMAL(38,19)	NOT NULL	,
TotalProjectedPropertyPreservationCosts	DECIMAL(38,19)	NOT NULL	,
FHAHOACondoDues	DECIMAL(38,19)	NOT NULL	,
TaxAndInsurance	DECIMAL(38,19)	NOT NULL	,
Inspections	DECIMAL(38,19)	NOT NULL	,
Utilities	DECIMAL(38,19)	NOT NULL	,
TotalProjectedOnGoingCosts	DECIMAL(38,19)	NOT NULL	,
FHARepairs	DECIMAL(38,19)	NOT NULL	,
FHALossDraftRepairOffset	DECIMAL(38,19)	NOT NULL	,
TotalProjectedSellingCosts	DECIMAL(38,19)	NOT NULL	,
FHAAppraisalCost	DECIMAL(38,19)	NOT NULL	,
ProjectedExpenses	DECIMAL(38,19)	NOT NULL	,
FHAOutstandingMaintenanceClaimable	DECIMAL(38,19)	NOT NULL	,
FHAOutstandingMaintenanceNonClaimable	DECIMAL(38,19)	NOT NULL	,
TotalOutstandingMaintenance	DECIMAL(38,19)	NOT NULL	,
OutstandingRepairs	DECIMAL(38,19)	NOT NULL	,
FHAOutstandingRepairsClaimable	DECIMAL(38,19)	NOT NULL	,
FHAOutstandingRepairsNonClaimable	DECIMAL(38,19)	NOT NULL	,
TotalOutstandingRepairs	DECIMAL(38,19)	NOT NULL	,
FutureAdvances	DECIMAL(38,19)	NOT NULL	,
FutureInterest	DECIMAL(38,19)	NOT NULL	,
TotalFutureExpenses	DECIMAL(38,19)	NOT NULL	,
TotalExposure	DECIMAL(38,19)	NOT NULL	,
CurrentBalance	DECIMAL(38,19)	NOT NULL	,
CurrentAdvances	DECIMAL(38,19)	NOT NULL	,
CurrentDebentureInterest	DECIMAL(38,19)	NOT NULL	,
ClaimFutureAdvances	DECIMAL(38,19)	NOT NULL	,
FutureDebentureInterest	DECIMAL(38,19)	NOT NULL	,
FHANonRecoverableExpenseAdjustment	DECIMAL(38,19)	NOT NULL	,
ContractSalesPrice	DECIMAL(38,19)	NOT NULL	,
ClosingCostReimbursement	DECIMAL(38,19)	NOT NULL	,
FinalClaimsProceeds	DECIMAL(38,19)	NOT NULL	,
SalesProceeds	DECIMAL(38,19)	NOT NULL	,
ClaimProceeds	DECIMAL(38,19)	NOT NULL	,
TotalNetGainLoss	DECIMAL(38,19)	NOT NULL
) USING DELTA
location 'abfss://silver@adlsdiuatcus.dfs.core.windows.net/fha/external/cba14/SERVICER_ASSET_DISPOSITION_PATH';



-- COMMAND ----------

				
USE CBA14;

CREATE OR REPLACE TABLE CBA14.SERVICER_ASSET_DISPOSITION_SUMMARY (
Id	STRING	NOT NULL	,
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
DeleteInd	BOOLEAN	NOT NULL	,
DispositionId	STRING	NOT NULL	,
EstimatedConveyanceLossOrGain	DECIMAL(38,19)	NOT NULL	,
EstimatedCWCOTLossOrGain	DECIMAL(38,19)	NOT NULL	,
ContributionAmount	DECIMAL(38,19)	NOT NULL	,
FHAContributionPercent	DECIMAL(38,19)	NOT NULL	,
EstimatedCWCOTOptimizedSavings	DECIMAL(38,19)	NOT NULL	,
MonthsToConveyance	INT	NOT NULL	,
DaysToConveyance	INT	NOT NULL	,
MonthsToCWCOTSale	INT	NOT NULL	,
DaysToCWCOTSale	INT	NOT NULL	


 ) USING DELTA
location 'abfss://silver@adlsdiuatcus.dfs.core.windows.net/fha/external/cba14/SERVICER_ASSET_DISPOSITION_SUMMARY';

-- COMMAND ----------

		
USE cba14;

CREATE OR REPLACE TABLE CBA14.FHA_USER_EVENT
(
ID STRING ,
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
DeleteInd BOOLEAN ,
EventTypeId INT NOT NULL,
Action STRING ,
EventDetails STRING
)USING DELTA
location 'abfss://silver@adlsdiuatcus.dfs.core.windows.net/fha/external/cba14/fha_user_event';

-- COMMAND ----------

		
USE cba14;

CREATE OR REPLACE TABLE CBA14.FHA_USER_EVENT_TYPE
(
ID BIGINT NOT NULL ,
xmd_InsertDateTimestamp	TIMESTAMP		NOT NULL	,
xmd_InsertUserId	STRING		NOT NULL	,
xmd_UpdateDateTimestamp	TIMESTAMP			,
xmd_UpdateUserId	STRING			,
xmd_DeleteDateTimestamp	TIMESTAMP			,
xmd_DeleteUserId	STRING			,
xmd_ChangeOperationCode	STRING	,
Name STRING NOT NULL		
)USING DELTA
location 'abfss://silver@adlsdiuatcus.dfs.core.windows.net/fha/external/cba14/fha_user_event_type';

-- COMMAND ----------


USE cba14;

CREATE OR REPLACE TABLE CBA14.FHA_USER_SESSIONS_SERVICER
(
ID STRING NOT NULL ,
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
DeleteInd BOOLEAN NOT NULL,
SessionId STRING NOT NULL,
ServicerId STRING NOT NULL,
StartTimestamp TIMESTAMP NOT NULL,
EndTimestamp TIMESTAMP ,
ActiveInd BOOLEAN NOT NULL

	
)USING DELTA
location 'abfss://silver@adlsdiuatcus.dfs.core.windows.net/fha/external/cba14/FHA_USER_SESSIONS_SERVICER';