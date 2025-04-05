# Databricks notebook source
dbutils.widgets.text("Src_Catalog", "", "")
Src_catalog = dbutils.widgets.get("Src_Catalog")
print(f'param "Src_Catalog" : {Src_catalog}')
dbutils.widgets.text("Tgt_Catalog", "", "")
Tgt_Catalog = dbutils.widgets.get("Tgt_Catalog")
print(f'param "Tgt_Catalog" : {Tgt_Catalog}')

# COMMAND ----------

spark.sql(f"use catalog {Tgt_Catalog}")


# COMMAND ----------

# MAGIC %sql
# MAGIC use dwh;

# COMMAND ----------

df = spark.sql(
    f"SELECT table_schema, table_name FROM {Tgt_Catalog}.INFORMATION_SCHEMA.TABLES "
    "WHERE table_schema = 'dwh' AND (table_name LIKE '%dim' OR table_name LIKE '%fact')"
)
display(df)

# COMMAND ----------

for schema, table in df.collect():
    print(f"truncat table {Tgt_Catalog}.{schema}.{table}")
    spark.sql(f"TRUNCATE TABLE {Tgt_Catalog}.{schema}.{table}")

# COMMAND ----------

load_status=spark.sql(f"""
WITH date_sequence AS (
    SELECT explode(sequence(to_date('2020-01-01'), to_date('2030-12-31'), interval 1 day)) AS calendar_date
)
INSERT INTO DWH.DATE_DIM(CALENDAR_DATE, DAY, MONTH, YEAR, QUARTER, WEEK_OF_YEAR, DAY_OF_WEEK, DAY_NAME, MONTH_NAME, WEEKEND_IND, YEAR_MONTH, HOLIDAY_IND)
SELECT 
    calendar_date AS CALENDAR_DATE,                                              -- The date value
    day(calendar_date) AS day,                                 -- Day of the month
    month(calendar_date) AS month,                             -- Month of the year
    year(calendar_date) AS year,                               -- Year
    quarter(calendar_date) AS quarter,                         -- Quarter of the year
    weekofyear(calendar_date) AS week_of_year,                 -- ISO week number of the year
    dayofweek(calendar_date) AS day_of_week,                   -- Day of the week (1=Sunday, 7=Saturday)
    date_format(calendar_date, 'EEEE') AS day_name,            -- Name of the day (e.g., Monday)
    date_format(calendar_date, 'MMMM') AS month_name,          -- Name of the month (e.g., January)
    CASE 
        WHEN dayofweek(calendar_date) IN (1, 7) THEN TRUE 
        ELSE FALSE 
    END AS WEEKEND_IND,                                         -- Indicates if the day is a weekend
    date_format(calendar_date, 'yyyy-MM') AS year_month,       -- Year and month in YYYY-MM format
    FALSE AS HOLIDAY_IND                                        -- Placeholder for holiday information
FROM date_sequence;
""" )



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from DWH.DATE_DIM

# COMMAND ----------

load_status=spark.sql(f""" 
INSERT INTO ASSET_DIM (xmd_InsertDateTimestamp	,xmd_InsertUserId	,xmd_UpdateDateTimestamp	,
xmd_UpdateUserId	,xmd_DeleteDateTimestamp	,xmd_DeleteUserId	,xmd_ChangeOperationCode	,
DeleteInd	,ID,Asset_Key	,XPK_Key,ServicerAssetKey,SourceFileID,StreetAddress	,CityName	,StateCode	,ZipCode	,EffectiveDate	,
ExpiryDate	,IsCurrent)
SELECT DISTINCT 
current_timestamp() as xmd_InsertDateTimestamp	,
current_user() as xmd_InsertUserId  	,
current_timestamp() as xmd_UpdateDateTimestamp	,
current_user() as xmd_UpdateUserId	,
NULL as xmd_DeleteDateTimestamp	,
NULL as xmd_DeleteUserId	,
'I' as xmd_ChangeOperationCode	,
NULL as DeleteInd	,ID,ServicerAssetKey	,XPKKey,ServicerAssetKey,SourceFileID,StreetAddress	,CityName	,StateCode	,ZipCode,TO_DATE('01-01-2020','dd-mm-yyyy') AS EFFECTIVE_DT,NULL AS ExpriyDt, 'Y' as IsCurrent
FROM {Src_catalog}.CBA14.servicer_asset; """)


# COMMAND ----------

load_status=spark.sql(f"""
INSERT INTO SERVICER_DIM (SERVICER_Id	,src_xmd_InsertDateTimestamp	,src_xmd_InsertUserId	,src_xmd_UpdateDateTimestamp	,src_xmd_UpdateUserId	,src_xmd_DeleteDateTimestamp	,src_xmd_DeleteUserId	,
src_xmd_ChangeOperationCode	,xmd_InsertDateTimestamp	,xmd_InsertUserId	,xmd_UpdateDateTimestamp	,
xmd_UpdateUserId	,xmd_DeleteDateTimestamp	,xmd_DeleteUserId	,xmd_ChangeOperationCode	,DeleteInd	,
Name	,ShortName	,EffectiveDate	,ExpiryDate	,IsCurrent	)
SELECT
Id	,
src_xmd_InsertDateTimestamp	,
src_xmd_InsertUserId	,
src_xmd_UpdateDateTimestamp	,
src_xmd_UpdateUserId	,
src_xmd_DeleteDateTimestamp	,
src_xmd_DeleteUserId	,
src_xmd_ChangeOperationCode	,
current_timestamp() as xmd_InsertDateTimestamp	,
current_user() as xmd_InsertUserId  	,
current_timestamp() as xmd_UpdateDateTimestamp	,
current_user() as xmd_UpdateUserId	,
NULL as xmd_DeleteDateTimestamp	,
NULL as xmd_DeleteUserId	,
'I' as xmd_ChangeOperationCode	,
NULL as DeleteInd	,
Name	,
ShortName	,
TO_DATE('01-01-2020','dd-mm-yyyy') AS EFFECTIVE_DT,
NULL AS ExpriyDt, 'Y' as IsCurrent
FROM {Src_catalog}.CBR4.SERVICER; """)


# COMMAND ----------

load_status=spark.sql(f"""
INSERT INTO USER_DIM(
User_Id	,src_xmd_InsertDateTimestamp	,src_xmd_InsertUserId	,src_xmd_UpdateDateTimestamp	,
src_xmd_UpdateUserId	,src_xmd_DeleteDateTimestamp	,src_xmd_DeleteUserId	,src_xmd_ChangeOperationCode	,
xmd_InsertDateTimestamp	,xmd_InsertUserId	,xmd_UpdateDateTimestamp	,xmd_UpdateUserId	,xmd_DeleteDateTimestamp	,xmd_DeleteUserId	,xmd_ChangeOperationCode	,DeleteInd	,Email	,
FirstName	,LastName	,StatusId	,LastLoginDateTimestamp	,B2CUserId	,EffectiveDate	,ExpiryDate	,
IsCurrent	) 
select 
Id	,
src_xmd_InsertDateTimestamp	,
src_xmd_InsertUserId	,
src_xmd_UpdateDateTimestamp	,
src_xmd_UpdateUserId	,
src_xmd_DeleteDateTimestamp	,
src_xmd_DeleteUserId	,
src_xmd_ChangeOperationCode	,
current_timestamp() as xmd_InsertDateTimestamp	,
current_user() as xmd_InsertUserId  	,
current_timestamp() as xmd_UpdateDateTimestamp	,
current_user() as xmd_UpdateUserId	,
NULL as xmd_DeleteDateTimestamp	,
NULL as xmd_DeleteUserId	,
'I' as xmd_ChangeOperationCode	,
NULL as DeleteInd	,
Email	,
FirstName	,
LastName	,
StatusId	,
LastLoginDateTimestamp	,
B2CUserId	,
TO_DATE('01-01-2020','dd-mm-yyyy') AS EFFECTIVE_DT,
NULL AS ExpriyDt, 'Y' as IsCurrent
FROM 
{Src_catalog}.cba14.FHA_USER; """)


# COMMAND ----------

load_status=spark.sql(f"""
INSERT INTO DWH.USER_SESSION_FACT (USER_DIM_KEY	,
DATE_DIM_KEY	,
User_Session_Id	,
src_xmd_InsertDateTimestamp	,
src_xmd_InsertUserId	,
src_xmd_UpdateDateTimestamp	,
src_xmd_UpdateUserId	,
src_xmd_DeleteDateTimestamp	,
src_xmd_DeleteUserId	,
src_xmd_ChangeOperationCode	,
xmd_InsertDateTimestamp	,
xmd_InsertUserId	,
xmd_UpdateDateTimestamp	,
xmd_UpdateUserId	,
xmd_DeleteDateTimestamp	,
xmd_DeleteUserId	,
xmd_ChangeOperationCode	,
DeleteInd	,
UserId	,
LoginTimestamp	,
LogoutTimestamp	,
LastActivityTimestamp	,
ExpirationTimestamp	,
IPAddressCode	,
UserAgentCode	,
InteractionCount	,
ActiveInd	
)
select UD.USER_DIM_KEY,DD.DATE_DIM_KEY,
FUS.Id	,
FUS.src_xmd_InsertDateTimestamp	,
FUS.src_xmd_InsertUserId	,
FUS.src_xmd_UpdateDateTimestamp	,
FUS.src_xmd_UpdateUserId	,
FUS.src_xmd_DeleteDateTimestamp	,
FUS.src_xmd_DeleteUserId	,
FUS.src_xmd_ChangeOperationCode	,
current_timestamp() as xmd_InsertDateTimestamp	,
current_user() as xmd_InsertUserId  	,
current_timestamp() as xmd_UpdateDateTimestamp	,
current_user() as xmd_UpdateUserId	,
NULL as xmd_DeleteDateTimestamp	,
NULL as xmd_DeleteUserId	,
'I' as xmd_ChangeOperationCode	,
NULL as DeleteInd	,
FUS.UserId	,
FUS.LoginTimestamp	,
FUS.LogoutTimestamp	,
FUS.LastActivityTimestamp	,
FUS.ExpirationTimestamp	,
FUS.IPAddress	,
FUS.UserAgent	,
FUS.InteractionCount	,
FUS.IsActive	
 from {Src_catalog}.CBA14.FHA_USER_SESSIONS FUS JOIN DWH.USER_DIM UD ON FUS.USERID = UD.USER_Id
JOIN DWH.DATE_DIM DD ON DATE_TRUNC('DAY', FUS.src_xmd_InsertDateTimestamp) = DD.CALENDAR_DATE;""")


# COMMAND ----------

load_status=spark.sql(f"""
INSERT INTO SERVICER_UPLOAD_FACT
(Servicer_Dim_Key	,User_Dim_Key	,Date_Dim_Key	,Servicer_Source_File_Id	,src_xmd_InsertDateTimestamp	,
src_xmd_InsertUserId	,src_xmd_UpdateDateTimestamp	,src_xmd_UpdateUserId	,src_xmd_DeleteDateTimestamp	,
src_xmd_DeleteUserId	,src_xmd_ChangeOperationCode	,xmd_InsertDateTimestamp	,xmd_InsertUserId	,
xmd_UpdateDateTimestamp	,xmd_UpdateUserId	,xmd_DeleteDateTimestamp	,xmd_DeleteUserId	,xmd_ChangeOperationCode	,DeleteInd	,ServicerId	,UserId	,FileName	,Path	,StatusId	,Message	
)
SELECT SD.SERVICER_DIM_KEY,UD.USER_DIM_KEY,DD.DATE_DIM_KEY,SSF.Id,
SSF.src_xmd_InsertDateTimestamp	,SSF.src_xmd_InsertUserId	,SSF.src_xmd_UpdateDateTimestamp	,
SSF.src_xmd_UpdateUserId	,SSF.src_xmd_DeleteDateTimestamp	,SSF.src_xmd_DeleteUserId	,
SSF.src_xmd_ChangeOperationCode	,
current_timestamp() as xmd_InsertDateTimestamp	,
current_user() as xmd_InsertUserId  	,
current_timestamp() as xmd_UpdateDateTimestamp	,
current_user() as xmd_UpdateUserId	,
NULL as xmd_DeleteDateTimestamp	,
NULL as xmd_DeleteUserId	,
'I' as xmd_ChangeOperationCode	,
NULL as DeleteInd	,
SSF.ServicerId	,SSF.UserId	,SSF.FileName	,SSF.Path	,
SSF.StatusId	,SSF.Message	
 FROM {Src_catalog}.CBA14.SERVICER_SOURCE_FILE SSF JOIN  DWH.SERVICER_DIM SD ON SSF.SERVICERID = SD.SERVICER_Id
JOIN DWH.USER_DIM UD ON SSF.USERID = UD.USER_Id
JOIN DWH.DATE_DIM DD ON DATE_TRUNC('DAY', SSF.src_xmd_InsertDateTimestamp) = DD.CALENDAR_DATE;""")


# COMMAND ----------

load_status=spark.sql(f"""
INSERT INTO DWH.user_event_fact (
  User_Event_ID	,
src_xmd_InsertDateTimestamp	,
src_xmd_InsertUserId	,
src_xmd_UpdateDateTimestamp	,
src_xmd_UpdateUserId	,
src_xmd_DeleteDateTimestamp	,
src_xmd_DeleteUserId	,
src_xmd_ChangeOperationCode	,
xmd_InsertDateTimestamp	,
xmd_InsertUserId	,
xmd_UpdateDateTimestamp	,
xmd_UpdateUserId	,
xmd_DeleteDateTimestamp	,
xmd_DeleteUserId	,
xmd_ChangeOperationCode	,
DeleteInd	,
EventTypeName	,
Action	,
EventDetails	
)
select ID	,
src_xmd_InsertDateTimestamp	,
src_xmd_InsertUserId	,
src_xmd_UpdateDateTimestamp	,
src_xmd_UpdateUserId	,
src_xmd_DeleteDateTimestamp	,
src_xmd_DeleteUserId	,
src_xmd_ChangeOperationCode	,
current_timestamp() as xmd_InsertDateTimestamp	,
current_user() as xmd_InsertUserId  	,
current_timestamp() as xmd_UpdateDateTimestamp	,
current_user() as xmd_UpdateUserId	,
NULL as xmd_DeleteDateTimestamp	,
NULL as xmd_DeleteUserId	,
'I' as xmd_ChangeOperationCode	,
NULL as DeleteInd	,
EventTypeId	,
Action	,
EventDetails	
from 
{Src_catalog}.CBA14.FHA_USER_EVENT;""")




# COMMAND ----------


spark.sql(f""" INSERT INTO SERVICER_ASSET_DISPOSITION_FACT(
  ASSET_DIM_KEY,
  DATE_DIM_KEY,
  SERVICER_DIM_KEY,
  Servicer_Asset_Id,
  src_xmd_InsertDateTimestamp,
  src_xmd_InsertUserId,
  src_xmd_UpdateDateTimestamp,
  src_xmd_UpdateUserId,
  src_xmd_DeleteDateTimestamp,
  src_xmd_DeleteUserId,
  src_xmd_ChangeOperationCode,
  xmd_InsertDateTimestamp,
  xmd_InsertUserId,
  xmd_UpdateDateTimestamp,
  xmd_UpdateUserId,
  xmd_DeleteDateTimestamp,
  xmd_DeleteUserId,
  xmd_ChangeOperationCode,
  DeleteInd,
  XpkKey,
  ServicerSystemExtractDate,
  ServicerAssetKey,
  ServicerId,
  SourceFileId,
  StreetAddress,
  CityName,
  StateCode,
  ZipCode,
  PropertyLoanBalanceAmount,
  PropertyLoanAccruedInterestAmount,
  ServicerNonRecoverableAdvanceBalanceAmount,
  ServicerRecoverableAdvanceBalanceAmount,
  ServicerRecoverable3rdPartyAdvanceBalanceAmount,
  ServicerEscrowAdvanceBalanceAmount,
  ServicerUnappliedBorrowerFundAmount,
  InsuranceLostDraftProceedAmount,
  FhaProjectedPropertyValueAmount,
  PropertyTypeCode,
  LoanFirstPaymentDueDate,
  LoanNextPaymentDueDate,
  FhaServicerForeclosureChecklistCompleteDate,
  ForeclosureTargetStartDate,
  ForeclosureExtendedStartDate,
  ForeclosureStartDate,
  ForeclosureScheduleSaleDate,
  ForeclosureSaleDate,
  FhaForeclosureDiligenceStartDate,
  StatutoryRightOfRedemptionStateInd,
  StatutoryRightOfRedemptionExpirationDate,
  StatutoryRightOfRedemptionCompletionDate,
  ForeclosureEvictionTargetStartDate,
  ForeclosureEvictionExtendedStartDate,
  ForeclosureEvictionStartDate,
  TargetHudConveyanceDate,
  ExtendedHudConveyanceDate,
  HudConveyanceDate,
  ForeclosureFirstVacancyDate,
  ForeclosureHoldDate,
  ForeclosureHoldDescriptionText,
  BorrowerOccupationalClassificationCode,
  ForeclosureBidReserveAmount,
  ServicerLoanOnboardingDate,
  LoanInterestRate,
  PostSaleClearTitleDate,
  FhaCurtailFirstLegalInd,
  CurtailDueDiligenceInd,
  DiligenceTargetStartDate,
  DiligenceExtendedStartDate,
  SoldTo3RdPartyInd,
  CwcotEligibilityInd,
  Servicer_Asset_Disposition_Id,
  Disposition_StatusId,
  Disposition_StatusName,
  RunDate,
  FHA203kFundsAmount,
  CurrentNetExposureTotalAmount,
  DebentureRate,
  DebentureInterestDate,
  DefaultDate,
  FCLApprovalDate,
  FirstLegalDueDate,
  FirstLegalCurtailmentInd,
  ForeclosureSaleDateActual,
  DueDiligenceTargetDate,
  DueDiligenceCurtailmentInd,
  EvictionFirstLegalTargetDate,
  EvictionStartExtendedDate,
  FirstTimeVacancyDate,
  EvictionStartActualDate,
  EvictionStartCurtailmentInd,
  ConveyanceTargetDateCalculated,
  ConveyanceExtendedDate,
  ConveyanceActualDate,
  ConveyanceCurtailmentInd,
  CurtailmentStatus,
  Disposition_ForeclosureHoldDate,
  ForeclosureHoldReason,
  RRCFollowUpDate,
  RRCCompletionDate,
  TitleGrade,
  FHATitleIssueId,
  TitleCureTime,
  AppraisalStatus,
  AppraisalTypeNeeded
)
SELECT 
  AD.ASSET_DIM_KEY,
  DD.DATE_DIM_KEY,
  SD.SERVICER_DIM_KEY,
  SA.Id,
  SA.src_xmd_InsertDateTimestamp,
  SA.src_xmd_InsertUserId,
  SA.src_xmd_UpdateDateTimestamp,
  SA.src_xmd_UpdateUserId,
  SA.src_xmd_DeleteDateTimestamp,
  SA.src_xmd_DeleteUserId,
  SA.src_xmd_ChangeOperationCode,
  current_timestamp() as xmd_InsertDateTimestamp,
  current_user() as xmd_InsertUserId,
  current_timestamp() as xmd_UpdateDateTimestamp,
  current_user() as xmd_UpdateUserId,
  NULL as xmd_DeleteDateTimestamp,
  NULL as xmd_DeleteUserId,
  'I' as xmd_ChangeOperationCode,
  NULL as DeleteInd,
  SA.XpkKey,
  SA.ServicerSystemExtractDate,
  SA.ServicerAssetKey,
  SA.ServicerId,
  SA.SourceFileId,
  SA.StreetAddress,
  SA.CityName,
  SA.StateCode,
  SA.ZipCode,
  SA.PropertyLoanBalanceAmount,
  SA.PropertyLoanAccruedInterestAmount,
  SA.ServicerNonRecoverableAdvanceBalanceAmount,
  SA.ServicerRecoverableAdvanceBalanceAmount,
  SA.ServicerRecoverable3rdPartyAdvanceBalanceAmount,
  SA.ServicerEscrowAdvanceBalanceAmount,
  SA.ServicerUnappliedBorrowerFundAmount,
  SA.InsuranceLostDraftProceedAmount,
  SA.FhaProjectedPropertyValueAmount,
  SA.PropertyTypeCode,
  SA.LoanFirstPaymentDueDate,
  SA.LoanNextPaymentDueDate,
  SA.FhaServicerForeclosureChecklistCompleteDate,
  SA.ForeclosureTargetStartDate,
  SA.ForeclosureExtendedStartDate,
  SA.ForeclosureStartDate,
  SA.ForeclosureScheduleSaleDate,
  SA.ForeclosureSaleDate,
  SA.FhaForeclosureDiligenceStartDate,
  SA.StatutoryRightOfRedemptionStateInd,
  SA.StatutoryRightOfRedemptionExpirationDate,
  SA.StatutoryRightOfRedemptionCompletionDate,
  SA.ForeclosureEvictionTargetStartDate,
  SA.ForeclosureEvictionExtendedStartDate,
  SA.ForeclosureEvictionStartDate,
  SA.TargetHudConveyanceDate,
  SA.ExtendedHudConveyanceDate,
  SA.HudConveyanceDate,
  SA.ForeclosureFirstVacancyDate,
  SA.ForeclosureHoldDate,
  SA.ForeclosureHoldDescriptionText,
  SA.BorrowerOccupationalClassificationCode,
  SA.ForeclosureBidReserveAmount,
  SA.ServicerLoanOnboardingDate,
  SA.LoanInterestRate,
  SA.PostSaleClearTitleDate,
  SA.FhaCurtailFirstLegalInd,
  SA.CurtailDueDiligenceInd,
  SA.DiligenceTargetStartDate,
  SA.DiligenceExtendedStartDate,
  SA.SoldTo3RdPartyInd,
  SA.CwcotEligibilityInd,
  SAD.Id,
  SAD.StatusId,
  SSFS.Name,
  SAD.RunDate,
  SAD.FHA203kFundsAmount,
  SAD.CurrentNetExposureTotalAmount,
  SAD.DebentureRate,
  SAD.DebentureInterestDate,
  SAD.DefaultDate,
  SAD.FCLApprovalDate,
  SAD.FirstLegalDueDate,
  SAD.FirstLegalCurtailmentInd,
  SAD.ForeclosureSaleDateActual,
  SAD.DueDiligenceTargetDate,
  SAD.DueDiligenceCurtailmentInd,
  SAD.EvictionFirstLegalTargetDate,
  SAD.EvictionStartExtendedDate,
  SAD.FirstTimeVacancyDate,
  SAD.EvictionStartActualDate,
  SAD.EvictionStartCurtailmentInd,
  SAD.ConveyanceTargetDateCalculated,
  SAD.ConveyanceExtendedDate,
  SAD.ConveyanceActualDate,
  SAD.ConveyanceCurtailmentInd,
  SAD.CurtailmentStatus,
  SAD.ForeclosureHoldDate,
  SAD.ForeclosureHoldReason,
  SAD.RRCFollowUpDate,
  SAD.RRCCompletionDate,
  SAD.TitleGrade,
  SAD.FHATitleIssueId,
  SAD.TitleCureTime,
  SAD.AppraisalStatus,
  SAD.AppraisalTypeNeeded
FROM 
  {Src_catalog}.CBA14.SERVICER_ASSET SA 
  LEFT OUTER JOIN {Src_catalog}.CBA14.servicer_asset_disposition SAD ON SA.ID = SAD.ServicerAssetId 
  JOIN {Src_catalog}.CBA14.SERVICER_SOURCE_FILE_STATUS SSFS ON SAD.StatusId = SSFS.ID 
  JOIN DWH.SERVICER_DIM SD ON SA.ServicerId = SD.SERVICER_ID 
  JOIN DWH.asset_dim AD ON SA.XpkKey = AD.XPK_Key
  JOIN DWH.DATE_DIM DD ON DATE_TRUNC('DAY', SA.src_xmd_InsertDateTimestamp) = DD.CALENDAR_DATE
""")

# COMMAND ----------

spark.sql(f"""
INSERT INTO USER_SESSIONS_SERVICER_FACT(ID,
src_xmd_InsertDateTimestamp ,
src_xmd_InsertUserId,
src_xmd_UpdateDateTimestamp ,
src_xmd_UpdateUserId,
src_xmd_DeleteDateTimestamp,
src_xmd_DeleteUserId,
src_xmd_ChangeOperationCode,
xmd_InsertDateTimestamp ,
xmd_InsertUserId    ,
xmd_UpdateDateTimestamp ,
xmd_UpdateUserId,
DeleteInd,
SessionId,
ServicerId,
StartTimestamp,
EndTimestamp,
ActiveInd)
SELECT
ID,
src_xmd_InsertDateTimestamp ,
src_xmd_InsertUserId  ,
src_xmd_UpdateDateTimestamp ,
src_xmd_UpdateUserId,
src_xmd_DeleteDateTimestamp,
src_xmd_DeleteUserId,
src_xmd_ChangeOperationCode,
current_timestamp() as xmd_InsertDateTimestamp  ,
current_user() as xmd_InsertUserId    ,
current_timestamp() as xmd_UpdateDateTimestamp  ,
current_user() as xmd_UpdateUserId,
DeleteInd,
SessionId,
ServicerId,
StartTimestamp,
EndTimestamp,
ActiveInd
FROM {Src_catalog}.CBA14.FHA_USER_SESSIONS_SERVICER """)