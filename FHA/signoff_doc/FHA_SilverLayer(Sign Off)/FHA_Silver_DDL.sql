-- Databricks notebook source
use catalog silver_dev;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS CBR18 
MANAGED LOCATION 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/managed/cbr18';						
USE CBR18;						
                       
CREATE TABLE HUD_CAFMV (                        
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
location 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cbr18/HUD_CAFMV';

-- COMMAND ----------

						
						
CREATE SCHEMA IF NOT EXISTS CBR18
MANAGED LOCATION 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/managed/cbr18';						
USE  CBR18;	
					
CREATE TABLE HUD_DUE_DILIGENCE_TIMELINE(						
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
location 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cbr18/HUD_DUE_DILIGENCE_TIMELINE';

-- COMMAND ----------

						
CREATE SCHEMA IF NOT EXISTS CBR18
MANAGED LOCATION 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/managed/cbr18';							
USE  CBR18;	
						
CREATE TABLE HUD_FUTURE_EXPENSES (						
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
location 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cbr18/HUD_FUTURE_EXPENSES';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS CBR16
MANAGED LOCATION 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/managed/cbr16';			
USE CBR16;	
		
CREATE TABLE XOME_CLOSING_COST (			
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
location 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cbr16/XOME_CLOSING_COST';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS CBR16
MANAGED LOCATION 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/managed/cbr16';			
USE CBR16;	
		
CREATE TABLE XOME_ESCROW (			
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
location 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cbr16/XOME_ESCROW';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS CBR16
MANAGED LOCATION 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/managed/cbr16';				
USE CBR16;	
		
CREATE TABLE XOME_TITLE_CURE			
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
location 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cbr16/XOME_TITLE_CURE';	

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS CBR16 
MANAGED LOCATION 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/managed/cbr16';				
USE CBR16;
			
CREATE TABLE XOME_FHA_MARKETING 			
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
location 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cbr16/XOME_FHA_MARKETING';

-- COMMAND ----------

			
CREATE SCHEMA IF NOT EXISTS CBR17
MANAGED LOCATION 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/managed/cbr17';			
USE CBR17;	
		
CREATE TABLE FED_DEBENTURE_RATE			
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
location 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cbr17/FED_DEBENTURE_RATE';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS CBR20
MANAGED LOCATION 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/managed/cbr20';				
USE CBR20;	
		
CREATE TABLE USFN_EVICTION			
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
location 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cbr20/TABLE USFN_EVICTION';

-- COMMAND ----------



-- COMMAND ----------


			
CREATE SCHEMA IF NOT EXISTS CBR4
MANAGED LOCATION 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/managed/cbr4';				
USE CBR4;

CREATE TABLE SERVICER			
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
location 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cbr4/SERVICER';


-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS CBA14
MANAGED LOCATION 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/managed/cbr14';				
USE CBA14;

CREATE TABLE SERVICER_SOURCE_FILE(
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
location 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cbr14/SERVICER_SOURCE_FILE';





-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS CBA14
MANAGED LOCATION 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/managed/cba14';				
USE CBA14;


CREATE TABLE SERVICER_ASSET
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
--,CurtailedForeclosureEvictionInd	STRING			,
--ProjectedDueDiligenceTargetDate	DATE			
--CONSTRAINT SERVICER_SOURCE_FILE_FK FOREIGN KEY(Id ) REFERENCES CBA14.SERVICER_SOURCE_FILE
) USING DELTA
location 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/SERVICER_ASSET';


-- COMMAND ----------


CREATE SCHEMA IF NOT EXISTS CBA14
MANAGED LOCATION 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/managed/cba14';				
USE CBA14;

CREATE TABLE FHA_USER
(
Id	STRING		NOT NULL	,
src_xmd_InsertDateTimestamp	TIMESTAMP		NOT NULL	,
src_xmd_InsertUserId	STRING		NOT NULL	,
src_xmd_UpdateDateTimestamp	TIMESTAMP	,
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
Email	VARCHAR(255)		NOT NULL	,
FirstName	VARCHAR(255)		NOT NULL	,
LastName	VARCHAR(255)		NOT NULL	,
StatusId	INT		NOT NULL	,
LastLoginDateTimestamp	TIMESTAMP			,
B2CUserId	STRING		NOT NULL	
) USING DELTA
location 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/FHA_USER';


-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS CBA14
MANAGED LOCATION 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/managed/cba14';				
USE CBA14;

CREATE TABLE ROLE
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
location 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/ROLE';


-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS CBA14
MANAGED LOCATION 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/managed/cba14';				
USE CBA14;

CREATE TABLE USER_ROLE
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
location 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/USER_ROLE';




-- COMMAND ----------


CREATE SCHEMA IF NOT EXISTS CBA14
MANAGED LOCATION 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/managed/cba14';				
USE CBA14;

CREATE TABLE ROLE_PERMISSION
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
location 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/ROLE_PERMISSION';


-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS CBA14
MANAGED LOCATION 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/managed/cba14';				
USE CBA14;

CREATE TABLE PERMISSION
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
location 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/PERMISSION';



-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS CBA14
MANAGED LOCATION 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/managed/cba14';				
USE CBA14;

CREATE TABLE MODULE
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
location 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/MODULE';




-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS CBA14
MANAGED LOCATION 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/managed/cba14';				
USE CBA14;

CREATE TABLE USER_SERVICER
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
location 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/USER_SERVICER';


-- COMMAND ----------


CREATE SCHEMA IF NOT EXISTS CBA14
MANAGED LOCATION 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/managed/cba14';				
USE CBA14;

CREATE TABLE CBA14.FHA_USER_STATUS (
Id INT,
src_xmd_InsertDateTimestamp	TIMESTAMP		NOT NULL	,
src_xmd_InsertUserId	STRING		NOT NULL	,
src_xmd_UpdateDateTimestamp	TIMESTAMP			,
src_xmd_UpdateUserId	STRING			,
src_xmd_DeleteDateTimestamp	TIMESTAMP			,
src_xmd_DeleteUserId	STRING,			
src_xmd_ChangeOperationCode	STRING	,
xmd_InsertDateTimestamp	TIMESTAMP		NOT NULL	,
xmd_InsertUserId	STRING		NOT NULL	,
xmd_UpdateDateTimestamp	TIMESTAMP			,
xmd_UpdateUserId	STRING			,
xmd_DeleteDateTimestamp	TIMESTAMP			,
xmd_DeleteUserId	STRING			,
xmd_ChangeOperationCode	STRING			,
Name STRING
) USING DELTA
location 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/FHA_USER_STATUS';





-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS CBA14
MANAGED LOCATION 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/managed/cba14';				
USE CBA14;



CREATE TABLE cba14.SERVICER_ASSET_DISPOSITION (
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
ForeclosureActualSaleDate	DATE	NOT NULL	,
DueDiligenceTargetDate	DATE	NOT NULL	,
DueDiligenceCurtailmentInd	BOOLEAN	NOT NULL	,
EvictionFirstLegalTargetDate	DATE		,
EvictionStartExtendedDate	DATE		,
FirstTimeVacancyDate	DATE		,
EvictionStartActualDate	DATE		,
EvictionStartCurtailmentInd	BOOLEAN	NOT NULL	,
CalculatedConveyanceTargetDate	DATE	NOT NULL	,
ConveyanceExtendedDate	DATE		,
ConveyanceActualDate	DATE		,
ConveyanceCurtailment	BOOLEAN	NOT NULL	,
CurtailmentStatusCode	STRING	NOT NULL	,
ForeclosureHoldDate	DATE		,
ForeclosureHoldReasonDescription	STRING		,
RRCFollowUpDate	DATE		,
RRCCompletionDate	DATE		,
TitleGradeCode	STRING		,
FHATitleIssueId	STRING	NOT NULL	,
TitleCureTime	INT	NOT NULL	,
AppraisalStatusCode	STRING	NOT NULL	,
AppraisalTypeNeededCode	STRING	NOT NULL	
    ) USING DELTA
location 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/SERVICER_ASSET_DISPOSITION';


-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS CBA14
MANAGED LOCATION 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/managed/cba14';				
USE CBA14;

CREATE TABLE cba14.SERVICER_ASSET_DISPOSITION_PATH (
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
AdjustIfPastConveyanceTargetAmount	INT	NOT NULL	,
FHAEvictionDayCount	INT	NOT NULL	,
FHARepairsDayCount	INT	NOT NULL	,
MarketingDayCount	INT	NOT NULL	,
TitleCurativeDayCount	INT	NOT NULL	,
FHADateAdjustmentDayCount	INT	NOT NULL	,
ProjectedDispositionDate	DATE	NOT NULL	,
CurrentPropertyValueAmount	DECIMAL(38,19)	NOT NULL	,
CurrentPropertyValueTypeCode	INT	NOT NULL	,
CAFMVHaircutPercentage	DECIMAL(38,19)	NOT NULL	,
CAFMVAmount	DECIMAL(38,19)	NOT NULL	,
FHAValueAdjustmentAmount	DECIMAL(38,19)	NOT NULL	,
ProjectedSalesPrice	DECIMAL(38,19)	NOT NULL	,
ClosingCostAdjustmentFactorPercentage	DECIMAL(38,19)	NOT NULL	,
ProjectedNetCashfromClosingCost	DECIMAL(38,19)	NOT NULL	,
FHAEvictionCost	DECIMAL(38,19)	NOT NULL	,
FHACashForKeysCost	DECIMAL(38,19)	NOT NULL	,
TotalProjectedEvictionCosts	DECIMAL(38,19)	NOT NULL	,
LawnMaintenanceAmount	DECIMAL(38,19)	NOT NULL	,
FHAInitialSecureCost	DECIMAL(38,19)	NOT NULL	,
FHAWinterizationCost	DECIMAL(38,19)	NOT NULL	,
FHALockChangeCost	DECIMAL(38,19)	NOT NULL	,
FHADebrisRemovalCost	DECIMAL(38,19)	NOT NULL	,
FHAPoolSecuringInd	BOOLEAN	NOT NULL	,
FHAPoolSecuringaAmount	DECIMAL(38,19)	NOT NULL	,
FHASalesCleanCost	DECIMAL(38,19)	NOT NULL	,
FHAOtherPropPreservationExpenseAmount	DECIMAL(38,19)	NOT NULL	,
TotalProjectedPropertyPreservationCost	DECIMAL(38,19)	NOT NULL	,
FHAHOACondoDuesAmount	DECIMAL(38,19)	NOT NULL	,
TaxAndInsuranceAmount	DECIMAL(38,19)	NOT NULL	,
InspectionsCost	DECIMAL(38,19)	NOT NULL	,
UtilitiesCost	DECIMAL(38,19)	NOT NULL	,
TotalProjectedOnGoingCost	DECIMAL(38,19)	NOT NULL	,
FHARepairsCost	DECIMAL(38,19)	NOT NULL	,
FHALossDraftRepairOffsetAmount	DECIMAL(38,19)	NOT NULL	,
TotalProjectedSellingCost	DECIMAL(38,19)	NOT NULL	,
FHAAppraisalCost	DECIMAL(38,19)	NOT NULL	,
ProjectedExpensesCost	DECIMAL(38,19)	NOT NULL	,
FHAOutstandingMaintenanceClaimableAmount	DECIMAL(38,19)	NOT NULL	,
FHAOutstandingMaintenanceNonClaimableAmount	DECIMAL(38,19)	NOT NULL	,
TotalOutstandingMaintenanceAmount	DECIMAL(38,19)	NOT NULL	,
OutstandingRepairsCost	DECIMAL(38,19)	NOT NULL	,
FHAOutstandingRepairsClaimableAmount	DECIMAL(38,19)	NOT NULL	,
FHAOutstandingRepairsNonClaimableAmount	DECIMAL(38,19)	NOT NULL	,
TotalOutstandingRepairsCost	DECIMAL(38,19)	NOT NULL	,
FutureAdvancesAmount	DECIMAL(38,19)	NOT NULL	,
FutureInterestAmount	DECIMAL(38,19)	NOT NULL	,
TotalFutureExpensesAmount	DECIMAL(38,19)	NOT NULL	,
TotalExposureAmount	DECIMAL(38,19)	NOT NULL	,
CurrentBalanceAmount	DECIMAL(38,19)	NOT NULL	,
CurrentBalanceAmount	DECIMAL(38,19)	NOT NULL	,
CurrentDebentureInterestAmount	DECIMAL(38,19)	NOT NULL	,
ClaimFutureAdvancesAmount	DECIMAL(38,19)	NOT NULL	,
FutureDebentureInterestAmount	DECIMAL(38,19)	NOT NULL	,
FHANonRecoverableExpenseAdjustmentAmount	DECIMAL(38,19)	NOT NULL	,
ContractSalesPrice	DECIMAL(38,19)	NOT NULL	,
ClosingReimbursementCost	DECIMAL(38,19)	NOT NULL	,
FinalClaimsProceedsAmount	DECIMAL(38,19)	NOT NULL	,
SalesProceedsAmount	DECIMAL(38,19)	NOT NULL	,
ClaimProceedsAmount	DECIMAL(38,19)	NOT NULL	,
TotalNetGainLossAmount	DECIMAL(38,19)	NOT NULL
) USING DELTA
location 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/SERVICER_ASSET_DISPOSITION_PATH';



-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS CBA14
MANAGED LOCATION 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/managed/cba14';				
USE CBA14;


CREATE TABLE CBA14.SERVICER_SOURCE_FILE_STATUS (
Id INT,
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
    Name STRING
) USING DELTA
location 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/SERVICER_SOURCE_FILE_STATUS';





-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS CBA14
MANAGED LOCATION 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/managed/cba14';				
USE CBA14;

CREATE TABLE CBA14.WORKER_SERVICE_PROCESS_INFO (
    Id STRING NOT NULL,
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
    Name STRING,
    IsActive BOOLEAN,  
    Interval INT,
    CronExpression STRING,  
    IsListener BOOLEAN
 ) USING DELTA
location 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/WORKER_SERVICE_PROCESS_INFO';



-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS CBA14
MANAGED LOCATION 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/managed/cba14';				
USE CBA14;
CREATE TABLE CBA14.DISPOSITION_PATH_TYPE(
	Id	INT	NOT NULL,
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
	Name	STRING	NOT NULL
) USING DELTA
location 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/DISPOSITION_PATH_TYPE';


-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS CBA14
MANAGED LOCATION 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/managed/cba14';				
USE CBA14;
CREATE TABLE CBA14.DISPOSITION_STATUS_TYPE(
	Id	INT	NOT NULL,
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
	Name	STRING	NOT NULL
) USING DELTA
location 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/DISPOSITION_STATUS_TYPE';


-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS CBA14
MANAGED LOCATION 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/managed/cba14';				
USE CBA14;

CREATE TABLE CBA14.FHA_USER_SESSIONS (
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
location 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/FHA_USER_SESSIONS';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS CBA14
MANAGED LOCATION 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/managed/cba14';				
USE CBA14;

CREATE TABLE cba14.SERVICER_ASSET_DISPOSITION_SUMMARY (
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
EstimatedConveyanceLossOrGainAmount	DECIMAL(38,19)	NOT NULL	,
EstimatedCWCOTLossOrGainAmount	DECIMAL(38,19)	NOT NULL	,
ContributionAmount	DECIMAL(38,19)	NOT NULL	,
FHAContributionPercent	DECIMAL(38,19)	NOT NULL	,
EstimatedCWCOTOptimizedSavingsAmount	DECIMAL(38,19)	NOT NULL	,
ConveyanceMonthCount	INT	NOT NULL	,
ConveyanceDayCount	INT	NOT NULL	,
CWCOTSaleMonthCount	INT	NOT NULL	,
CWCOTSaleDayCount	INT	NOT NULL	


 ) USING DELTA
location 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/SERVICER_ASSET_DISPOSITION_SUMMARY';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS CBA14
MANAGED LOCATION 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/managed/cba14';				
USE cba14;

CREATE TABLE cba14.FHA_USER_EVENT
(
ID BIGINT ,
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
ActionCode STRING ,
EventDetailDescription STRING
)USING DELTA
location 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/fha_user_event';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS CBA14
MANAGED LOCATION 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/managed/cba14';				
USE cba14;

CREATE TABLE CBA14.FHA_USER_EVENT_TYPE
(
ID BIGINT NOT NULL ,
xmd_InsertDateTimestamp	TIMESTAMP		NOT NULL	,
xmd_InsertUserId	STRING		NOT NULL	,
xmd_UpdateDateTimestamp	TIMESTAMP			,
xmd_UpdateUserId	STRING			,
xmd_DeleteDateTimestamp	TIMESTAMP			,
xmd_DeleteUserId	STRING			,
xmd_ChangeOperationCode	STRING	,
src_xmd_InsertDateTimestamp	TIMESTAMP		NOT NULL	,
src_xmd_InsertUserId	STRING		NOT NULL	,
src_xmd_UpdateDateTimestamp	TIMESTAMP			,
src_xmd_UpdateUserId	STRING			,
src_xmd_DeleteDateTimestamp	TIMESTAMP			,
src_xmd_DeleteUserId	STRING			,
src_xmd_ChangeOperationCode	STRING			,
Name STRING NOT NULL		
)USING DELTA
location 'abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/fha_user_event_type';

-- COMMAND ----------

