# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog gold_dev;
# MAGIC use dwh;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Generate and populate the calendar_dim table
# MAGIC WITH date_sequence AS (
# MAGIC     SELECT explode(sequence(to_date('2020-01-01'), to_date('2030-12-31'), interval 1 day)) AS calendar_date
# MAGIC )
# MAGIC INSERT INTO DWH.DATE_DIM(CALENDAR_DATE, DAY, MONTH, YEAR, QUARTER, WEEK_OF_YEAR, DAY_OF_WEEK, DAY_NAME, MONTH_NAME, WEEKEND_IND, YEAR_MONTH, HOLIDAY_IND)
# MAGIC SELECT 
# MAGIC     calendar_date AS CALENDAR_DATE,                                              -- The date value
# MAGIC     day(calendar_date) AS day,                                 -- Day of the month
# MAGIC     month(calendar_date) AS month,                             -- Month of the year
# MAGIC     year(calendar_date) AS year,                               -- Year
# MAGIC     quarter(calendar_date) AS quarter,                         -- Quarter of the year
# MAGIC     weekofyear(calendar_date) AS week_of_year,                 -- ISO week number of the year
# MAGIC     dayofweek(calendar_date) AS day_of_week,                   -- Day of the week (1=Sunday, 7=Saturday)
# MAGIC     date_format(calendar_date, 'EEEE') AS day_name,            -- Name of the day (e.g., Monday)
# MAGIC     date_format(calendar_date, 'MMMM') AS month_name,          -- Name of the month (e.g., January)
# MAGIC     CASE 
# MAGIC         WHEN dayofweek(calendar_date) IN (1, 7) THEN TRUE 
# MAGIC         ELSE FALSE 
# MAGIC     END AS WEEKEND_IND,                                         -- Indicates if the day is a weekend
# MAGIC     date_format(calendar_date, 'yyyy-MM') AS year_month,       -- Year and month in YYYY-MM format
# MAGIC     FALSE AS HOLIDAY_IND                                        -- Placeholder for holiday information
# MAGIC FROM date_sequence;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO USER_DIM(
# MAGIC User_Id	,src_xmd_InsertDateTimestamp	,src_xmd_InsertUserId	,src_xmd_UpdateDateTimestamp	,
# MAGIC src_xmd_UpdateUserId	,src_xmd_DeleteDateTimestamp	,src_xmd_DeleteUserId	,src_xmd_ChangeOperationCode	,
# MAGIC xmd_InsertDateTimestamp	,xmd_InsertUserId	,xmd_UpdateDateTimestamp	,xmd_UpdateUserId	,xmd_DeleteDateTimestamp	,xmd_DeleteUserId	,xmd_ChangeOperationCode	,DeleteInd	,Email	,
# MAGIC FirstName	,LastName	,StatusId	,LastLoginDateTimestamp	,B2CUserId	,EffectiveDate	,ExpiryDate	,
# MAGIC IsCurrent	) 
# MAGIC select 
# MAGIC Id	,
# MAGIC src_xmd_InsertDateTimestamp	,
# MAGIC src_xmd_InsertUserId	,
# MAGIC src_xmd_UpdateDateTimestamp	,
# MAGIC src_xmd_UpdateUserId	,
# MAGIC src_xmd_DeleteDateTimestamp	,
# MAGIC src_xmd_DeleteUserId	,
# MAGIC src_xmd_ChangeOperationCode	,
# MAGIC current_timestamp() as xmd_InsertDateTimestamp	,
# MAGIC current_user() as xmd_InsertUserId  	,
# MAGIC current_timestamp() as xmd_UpdateDateTimestamp	,
# MAGIC current_user() as xmd_UpdateUserId	,
# MAGIC NULL as xmd_DeleteDateTimestamp	,
# MAGIC NULL as xmd_DeleteUserId	,
# MAGIC 'I' as xmd_ChangeOperationCode	,
# MAGIC NULL as DeleteInd	,
# MAGIC Email	,
# MAGIC FirstName	,
# MAGIC LastName	,
# MAGIC StatusId	,
# MAGIC LastLoginDateTimestamp	,
# MAGIC B2CUserId	,
# MAGIC TO_DATE('01-01-2020','dd-mm-yyyy') AS EFFECTIVE_DT,
# MAGIC NULL AS ExpriyDt, 'Y' as IsCurrent
# MAGIC FROM 
# MAGIC silver_dev.cba14.FHA_USER
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO SERVICER_DIM (SERVICER_Id	,src_xmd_InsertDateTimestamp	,src_xmd_InsertUserId	,src_xmd_UpdateDateTimestamp	,src_xmd_UpdateUserId	,src_xmd_DeleteDateTimestamp	,src_xmd_DeleteUserId	,
# MAGIC src_xmd_ChangeOperationCode	,xmd_InsertDateTimestamp	,xmd_InsertUserId	,xmd_UpdateDateTimestamp	,
# MAGIC xmd_UpdateUserId	,xmd_DeleteDateTimestamp	,xmd_DeleteUserId	,xmd_ChangeOperationCode	,DeleteInd	,
# MAGIC Name	,ShortName	,EffectiveDate	,ExpiryDate	,IsCurrent	)
# MAGIC SELECT
# MAGIC Id	,
# MAGIC src_xmd_InsertDateTimestamp	,
# MAGIC src_xmd_InsertUserId	,
# MAGIC src_xmd_UpdateDateTimestamp	,
# MAGIC src_xmd_UpdateUserId	,
# MAGIC src_xmd_DeleteDateTimestamp	,
# MAGIC src_xmd_DeleteUserId	,
# MAGIC src_xmd_ChangeOperationCode	,
# MAGIC current_timestamp() as xmd_InsertDateTimestamp	,
# MAGIC current_user() as xmd_InsertUserId  	,
# MAGIC current_timestamp() as xmd_UpdateDateTimestamp	,
# MAGIC current_user() as xmd_UpdateUserId	,
# MAGIC NULL as xmd_DeleteDateTimestamp	,
# MAGIC NULL as xmd_DeleteUserId	,
# MAGIC 'I' as xmd_ChangeOperationCode	,
# MAGIC NULL as DeleteInd	,
# MAGIC Name	,
# MAGIC ShortName	,
# MAGIC TO_DATE('01-01-2020','dd-mm-yyyy') AS EFFECTIVE_DT,
# MAGIC NULL AS ExpriyDt, 'Y' as IsCurrent
# MAGIC FROM silver_dev.CBR4.SERVICER;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO ASSET_DIM (xmd_InsertDateTimestamp	,xmd_InsertUserId	,xmd_UpdateDateTimestamp	,
# MAGIC xmd_UpdateUserId	,xmd_DeleteDateTimestamp	,xmd_DeleteUserId	,xmd_ChangeOperationCode	,
# MAGIC DeleteInd	,Asset_Key	,XPK_Key	,StreetAddress	,CityName	,StateCode	,ZipCode	,EffectiveDate	,
# MAGIC ExpiryDate	,IsCurrent)
# MAGIC SELECT DISTINCT 
# MAGIC current_timestamp() as xmd_InsertDateTimestamp	,
# MAGIC current_user() as xmd_InsertUserId  	,
# MAGIC current_timestamp() as xmd_UpdateDateTimestamp	,
# MAGIC current_user() as xmd_UpdateUserId	,
# MAGIC NULL as xmd_DeleteDateTimestamp	,
# MAGIC NULL as xmd_DeleteUserId	,
# MAGIC 'I' as xmd_ChangeOperationCode	,
# MAGIC NULL as DeleteInd	,ServicerAssetKey	,XPKKey	,StreetAddress	,CityName	,StateCode	,ZipCode,TO_DATE('01-01-2020','dd-mm-yyyy') AS EFFECTIVE_DT,NULL AS ExpriyDt, 'Y' as IsCurrent
# MAGIC FROM silver_dev.CBA14.servicer_asset;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO SERVICER_UPLOAD_FACT
# MAGIC (Servicer_Dim_Key	,User_Dim_Key	,Date_Dim_Key	,Servicer_Source_File_Id	,src_xmd_InsertDateTimestamp	,
# MAGIC src_xmd_InsertUserId	,src_xmd_UpdateDateTimestamp	,src_xmd_UpdateUserId	,src_xmd_DeleteDateTimestamp	,
# MAGIC src_xmd_DeleteUserId	,src_xmd_ChangeOperationCode	,xmd_InsertDateTimestamp	,xmd_InsertUserId	,
# MAGIC xmd_UpdateDateTimestamp	,xmd_UpdateUserId	,xmd_DeleteDateTimestamp	,xmd_DeleteUserId	,xmd_ChangeOperationCode	,DeleteInd	,ServicerId	,UserId	,FileName	,Path	,StatusId	,Message	
# MAGIC )
# MAGIC SELECT SD.SERVICER_DIM_KEY,UD.USER_DIM_KEY,DD.DATE_DIM_KEY,SSF.Id,
# MAGIC SSF.src_xmd_InsertDateTimestamp	,SSF.src_xmd_InsertUserId	,SSF.src_xmd_UpdateDateTimestamp	,
# MAGIC SSF.src_xmd_UpdateUserId	,SSF.src_xmd_DeleteDateTimestamp	,SSF.src_xmd_DeleteUserId	,
# MAGIC SSF.src_xmd_ChangeOperationCode	,SSF.xmd_InsertDateTimestamp	,SSF.xmd_InsertUserId	,
# MAGIC SSF.xmd_UpdateDateTimestamp	,SSF.xmd_UpdateUserId	,SSF.xmd_DeleteDateTimestamp	,SSF.xmd_DeleteUserId	,
# MAGIC SSF.xmd_ChangeOperationCode	,SSF.DeleteInd	,SSF.ServicerId	,SSF.UserId	,SSF.FileName	,SSF.Path	,
# MAGIC SSF.StatusId	,SSF.Message	
# MAGIC  FROM silver_dev.CBA14.SERVICER_SOURCE_FILE SSF JOIN  DWH.SERVICER_DIM SD ON SSF.SERVICERID = SD.SERVICER_Id
# MAGIC JOIN DWH.USER_DIM UD ON SSF.USERID = UD.USER_Id
# MAGIC JOIN DWH.DATE_DIM DD ON DATE_TRUNC('DAY', SSF.src_xmd_InsertDateTimestamp) = DD.CALENDAR_DATE;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO DWH.USER_SESSION_FACT (USER_DIM_KEY	,
# MAGIC DATE_DIM_KEY	,
# MAGIC User_Session_Id	,
# MAGIC src_xmd_InsertDateTimestamp	,
# MAGIC src_xmd_InsertUserId	,
# MAGIC src_xmd_UpdateDateTimestamp	,
# MAGIC src_xmd_UpdateUserId	,
# MAGIC src_xmd_DeleteDateTimestamp	,
# MAGIC src_xmd_DeleteUserId	,
# MAGIC src_xmd_ChangeOperationCode	,
# MAGIC xmd_InsertDateTimestamp	,
# MAGIC xmd_InsertUserId	,
# MAGIC xmd_UpdateDateTimestamp	,
# MAGIC xmd_UpdateUserId	,
# MAGIC xmd_DeleteDateTimestamp	,
# MAGIC xmd_DeleteUserId	,
# MAGIC xmd_ChangeOperationCode	,
# MAGIC DeleteInd	,
# MAGIC UserId	,
# MAGIC LoginTimestamp	,
# MAGIC LogoutTimestamp	,
# MAGIC LastActivityTimestamp	,
# MAGIC ExpirationTimestamp	,
# MAGIC IPAddressCode	,
# MAGIC UserAgentCode	,
# MAGIC InteractionCount	,
# MAGIC ActiveInd	
# MAGIC )
# MAGIC select UD.USER_DIM_KEY,DD.DATE_DIM_KEY,
# MAGIC FUS.Id	,
# MAGIC FUS.src_xmd_InsertDateTimestamp	,
# MAGIC FUS.src_xmd_InsertUserId	,
# MAGIC FUS.src_xmd_UpdateDateTimestamp	,
# MAGIC FUS.src_xmd_UpdateUserId	,
# MAGIC FUS.src_xmd_DeleteDateTimestamp	,
# MAGIC FUS.src_xmd_DeleteUserId	,
# MAGIC FUS.src_xmd_ChangeOperationCode	,
# MAGIC current_timestamp() as xmd_InsertDateTimestamp	,
# MAGIC current_user() as xmd_InsertUserId  	,
# MAGIC current_timestamp() as xmd_UpdateDateTimestamp	,
# MAGIC current_user() as xmd_UpdateUserId	,
# MAGIC NULL as xmd_DeleteDateTimestamp	,
# MAGIC NULL as xmd_DeleteUserId	,
# MAGIC 'I' as xmd_ChangeOperationCode	,
# MAGIC NULL as DeleteInd	,
# MAGIC FUS.UserId	,
# MAGIC FUS.LoginTimestamp	,
# MAGIC FUS.LogoutTimestamp	,
# MAGIC FUS.LastActivityTimestamp	,
# MAGIC FUS.ExpirationTimestamp	,
# MAGIC FUS.IPAddress	,
# MAGIC FUS.UserAgent	,
# MAGIC FUS.InteractionCount	,
# MAGIC FUS.IsActive	
# MAGIC  from silver_dev.CBA14.FHA_USER_SESSIONS FUS JOIN DWH.USER_DIM UD ON FUS.USERID = UD.USER_Id
# MAGIC JOIN DWH.DATE_DIM DD ON DATE_TRUNC('DAY', FUS.src_xmd_InsertDateTimestamp) = DD.CALENDAR_DATE;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO DWH.user_event_fact (
# MAGIC   User_Event_ID	,
# MAGIC xmd_InsertDateTimestamp	,
# MAGIC xmd_InsertUserId	,
# MAGIC xmd_UpdateDateTimestamp	,
# MAGIC xmd_UpdateUserId	,
# MAGIC xmd_DeleteDateTimestamp	,
# MAGIC xmd_DeleteUserId	,
# MAGIC xmd_ChangeOperationCode	,
# MAGIC src_xmd_InsertDateTimestamp	,
# MAGIC src_xmd_InsertUserId	,
# MAGIC src_xmd_UpdateDateTimestamp	,
# MAGIC src_xmd_UpdateUserId	,
# MAGIC src_xmd_DeleteDateTimestamp	,
# MAGIC src_xmd_DeleteUserId	,
# MAGIC src_xmd_ChangeOperationCode	,
# MAGIC DeleteInd	,
# MAGIC EventTypeName	,
# MAGIC Action	,
# MAGIC EventDetails	
# MAGIC )
# MAGIC select ID	,
# MAGIC xmd_InsertDateTimestamp	,
# MAGIC xmd_InsertUserId	,
# MAGIC xmd_UpdateDateTimestamp	,
# MAGIC xmd_UpdateUserId	,
# MAGIC xmd_DeleteDateTimestamp	,
# MAGIC xmd_DeleteUserId	,
# MAGIC xmd_ChangeOperationCode	,
# MAGIC src_xmd_InsertDateTimestamp	,
# MAGIC src_xmd_InsertUserId	,
# MAGIC src_xmd_UpdateDateTimestamp	,
# MAGIC src_xmd_UpdateUserId	,
# MAGIC src_xmd_DeleteDateTimestamp	,
# MAGIC src_xmd_DeleteUserId	,
# MAGIC src_xmd_ChangeOperationCode	,
# MAGIC DeleteInd	,
# MAGIC EventTypeId	,
# MAGIC Action	,
# MAGIC EventDetails	
# MAGIC from 
# MAGIC silver_dev.CBA14.FHA_USER_EVENT;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO SERVICER_ASSET_DISPOSITION_FACT(
# MAGIC   ASSET_DIM_KEY,
# MAGIC   DATE_DIM_KEY,
# MAGIC   SERVICER_DIM_KEY,
# MAGIC   Servicer_Asset_Id,
# MAGIC   src_xmd_InsertDateTimestamp,
# MAGIC   src_xmd_InsertUserId,
# MAGIC   src_xmd_UpdateDateTimestamp,
# MAGIC   src_xmd_UpdateUserId,
# MAGIC   src_xmd_DeleteDateTimestamp,
# MAGIC   src_xmd_DeleteUserId,
# MAGIC   src_xmd_ChangeOperationCode,
# MAGIC   xmd_InsertDateTimestamp,
# MAGIC   xmd_InsertUserId,
# MAGIC   xmd_UpdateDateTimestamp,
# MAGIC   xmd_UpdateUserId,
# MAGIC   xmd_DeleteDateTimestamp,
# MAGIC   xmd_DeleteUserId,
# MAGIC   xmd_ChangeOperationCode,
# MAGIC   DeleteInd,
# MAGIC   XpkKey,
# MAGIC   ServicerSystemExtractDate,
# MAGIC   ServicerAssetKey,
# MAGIC   ServicerId,
# MAGIC   SourceFileId,
# MAGIC   StreetAddress,
# MAGIC   CityName,
# MAGIC   StateCode,
# MAGIC   ZipCode,
# MAGIC   PropertyLoanBalanceAmount,
# MAGIC   PropertyLoanAccruedInterestAmount,
# MAGIC   ServicerNonRecoverableAdvanceBalanceAmount,
# MAGIC   ServicerRecoverableAdvanceBalanceAmount,
# MAGIC   ServicerRecoverable3rdPartyAdvanceBalanceAmount,
# MAGIC   ServicerEscrowAdvanceBalanceAmount,
# MAGIC   ServicerUnappliedBorrowerFundAmount,
# MAGIC   InsuranceLostDraftProceedAmount,
# MAGIC   FhaProjectedPropertyValueAmount,
# MAGIC   PropertyTypeCode,
# MAGIC   LoanFirstPaymentDueDate,
# MAGIC   LoanNextPaymentDueDate,
# MAGIC   FhaServicerForeclosureChecklistCompleteDate,
# MAGIC   ForeclosureTargetStartDate,
# MAGIC   ForeclosureExtendedStartDate,
# MAGIC   ForeclosureStartDate,
# MAGIC   ForeclosureScheduleSaleDate,
# MAGIC   ForeclosureSaleDate,
# MAGIC   FhaForeclosureDiligenceStartDate,
# MAGIC   StatutoryRightOfRedemptionStateInd,
# MAGIC   StatutoryRightOfRedemptionExpirationDate,
# MAGIC   StatutoryRightOfRedemptionCompletionDate,
# MAGIC   ForeclosureEvictionTargetStartDate,
# MAGIC   ForeclosureEvictionExtendedStartDate,
# MAGIC   ForeclosureEvictionStartDate,
# MAGIC   TargetHudConveyanceDate,
# MAGIC   ExtendedHudConveyanceDate,
# MAGIC   HudConveyanceDate,
# MAGIC   ForeclosureFirstVacancyDate,
# MAGIC   ForeclosureHoldDate,
# MAGIC   ForeclosureHoldDescriptionText,
# MAGIC   BorrowerOccupationalClassificationCode,
# MAGIC   ForeclosureBidReserveAmount,
# MAGIC   ServicerLoanOnboardingDate,
# MAGIC   LoanInterestRate,
# MAGIC   PostSaleClearTitleDate,
# MAGIC   FhaCurtailFirstLegalInd,
# MAGIC   CurtailDueDiligenceInd,
# MAGIC   DiligenceTargetStartDate,
# MAGIC   DiligenceExtendedStartDate,
# MAGIC   SoldTo3RdPartyInd,
# MAGIC   CwcotEligibilityInd,
# MAGIC   Servicer_Asset_Disposition_Id,
# MAGIC   Disposition_StatusId,
# MAGIC   Disposition_StatusName,
# MAGIC   RunDate,
# MAGIC   FHA203kFundsAmount,
# MAGIC   CurrentNetExposureTotalAmount,
# MAGIC   DebentureRate,
# MAGIC   DebentureInterestDate,
# MAGIC   DefaultDate,
# MAGIC   FCLApprovalDate,
# MAGIC   FirstLegalDueDate,
# MAGIC   FirstLegalCurtailmentInd,
# MAGIC   ForeclosureSaleDateActual,
# MAGIC   DueDiligenceTargetDate,
# MAGIC   DueDiligenceCurtailmentInd,
# MAGIC   EvictionFirstLegalTargetDate,
# MAGIC   EvictionStartExtendedDate,
# MAGIC   FirstTimeVacancyDate,
# MAGIC   EvictionStartActualDate,
# MAGIC   EvictionStartCurtailmentInd,
# MAGIC   ConveyanceTargetDateCalculated,
# MAGIC   ConveyanceExtendedDate,
# MAGIC   ConveyanceActualDate,
# MAGIC   ConveyanceCurtailmentInd,
# MAGIC   CurtailmentStatus,
# MAGIC   Disposition_ForeclosureHoldDate,
# MAGIC   ForeclosureHoldReason,
# MAGIC   RRCFollowUpDate,
# MAGIC   RRCCompletionDate,
# MAGIC   TitleGrade,
# MAGIC   FHATitleIssueId,
# MAGIC   TitleCureTime,
# MAGIC   AppraisalStatus,
# MAGIC   AppraisalTypeNeeded
# MAGIC )
# MAGIC SELECT 
# MAGIC   AD.ASSET_DIM_KEY,
# MAGIC   DD.DATE_DIM_KEY,
# MAGIC   SD.SERVICER_DIM_KEY,
# MAGIC   SA.Id,
# MAGIC   SA.src_xmd_InsertDateTimestamp,
# MAGIC   SA.src_xmd_InsertUserId,
# MAGIC   SA.src_xmd_UpdateDateTimestamp,
# MAGIC   SA.src_xmd_UpdateUserId,
# MAGIC   SA.src_xmd_DeleteDateTimestamp,
# MAGIC   SA.src_xmd_DeleteUserId,
# MAGIC   SA.src_xmd_ChangeOperationCode,
# MAGIC   current_timestamp() as xmd_InsertDateTimestamp,
# MAGIC   current_user() as xmd_InsertUserId,
# MAGIC   current_timestamp() as xmd_UpdateDateTimestamp,
# MAGIC   current_user() as xmd_UpdateUserId,
# MAGIC   NULL as xmd_DeleteDateTimestamp,
# MAGIC   NULL as xmd_DeleteUserId,
# MAGIC   'I' as xmd_ChangeOperationCode,
# MAGIC   NULL as DeleteInd,
# MAGIC   SA.XpkKey,
# MAGIC   SA.ServicerSystemExtractDate,
# MAGIC   SA.ServicerAssetKey,
# MAGIC   SA.ServicerId,
# MAGIC   SA.SourceFileId,
# MAGIC   SA.StreetAddress,
# MAGIC   SA.CityName,
# MAGIC   SA.StateCode,
# MAGIC   SA.ZipCode,
# MAGIC   SA.PropertyLoanBalanceAmount,
# MAGIC   SA.PropertyLoanAccruedInterestAmount,
# MAGIC   SA.ServicerNonRecoverableAdvanceBalanceAmount,
# MAGIC   SA.ServicerRecoverableAdvanceBalanceAmount,
# MAGIC   SA.ServicerRecoverable3rdPartyAdvanceBalanceAmount,
# MAGIC   SA.ServicerEscrowAdvanceBalanceAmount,
# MAGIC   SA.ServicerUnappliedBorrowerFundAmount,
# MAGIC   SA.InsuranceLostDraftProceedAmount,
# MAGIC   SA.FhaProjectedPropertyValueAmount,
# MAGIC   SA.PropertyTypeCode,
# MAGIC   SA.LoanFirstPaymentDueDate,
# MAGIC   SA.LoanNextPaymentDueDate,
# MAGIC   SA.FhaServicerForeclosureChecklistCompleteDate,
# MAGIC   SA.ForeclosureTargetStartDate,
# MAGIC   SA.ForeclosureExtendedStartDate,
# MAGIC   SA.ForeclosureStartDate,
# MAGIC   SA.ForeclosureScheduleSaleDate,
# MAGIC   SA.ForeclosureSaleDate,
# MAGIC   SA.FhaForeclosureDiligenceStartDate,
# MAGIC   SA.StatutoryRightOfRedemptionStateInd,
# MAGIC   SA.StatutoryRightOfRedemptionExpirationDate,
# MAGIC   SA.StatutoryRightOfRedemptionCompletionDate,
# MAGIC   SA.ForeclosureEvictionTargetStartDate,
# MAGIC   SA.ForeclosureEvictionExtendedStartDate,
# MAGIC   SA.ForeclosureEvictionStartDate,
# MAGIC   SA.TargetHudConveyanceDate,
# MAGIC   SA.ExtendedHudConveyanceDate,
# MAGIC   SA.HudConveyanceDate,
# MAGIC   SA.ForeclosureFirstVacancyDate,
# MAGIC   SA.ForeclosureHoldDate,
# MAGIC   SA.ForeclosureHoldDescriptionText,
# MAGIC   SA.BorrowerOccupationalClassificationCode,
# MAGIC   SA.ForeclosureBidReserveAmount,
# MAGIC   SA.ServicerLoanOnboardingDate,
# MAGIC   SA.LoanInterestRate,
# MAGIC   SA.PostSaleClearTitleDate,
# MAGIC   SA.FhaCurtailFirstLegalInd,
# MAGIC   SA.CurtailDueDiligenceInd,
# MAGIC   SA.DiligenceTargetStartDate,
# MAGIC   SA.DiligenceExtendedStartDate,
# MAGIC   SA.SoldTo3RdPartyInd,
# MAGIC   SA.CwcotEligibilityInd,
# MAGIC   SAD.Id,
# MAGIC   SAD.StatusId,
# MAGIC   SSFS.Name,
# MAGIC   SAD.RunDate,
# MAGIC   SAD.FHA203kFundsAmount,
# MAGIC   SAD.CurrentNetExposureTotalAmount,
# MAGIC   SAD.DebentureRate,
# MAGIC   SAD.DebentureInterestDate,
# MAGIC   SAD.DefaultDate,
# MAGIC   SAD.FCLApprovalDate,
# MAGIC   SAD.FirstLegalDueDate,
# MAGIC   SAD.FirstLegalCurtailmentInd,
# MAGIC   SAD.ForeclosureSaleDateActual,
# MAGIC   SAD.DueDiligenceTargetDate,
# MAGIC   SAD.DueDiligenceCurtailmentInd,
# MAGIC   SAD.EvictionFirstLegalTargetDate,
# MAGIC   SAD.EvictionStartExtendedDate,
# MAGIC   SAD.FirstTimeVacancyDate,
# MAGIC   SAD.EvictionStartActualDate,
# MAGIC   SAD.EvictionStartCurtailmentInd,
# MAGIC   SAD.ConveyanceTargetDateCalculated,
# MAGIC   SAD.ConveyanceExtendedDate,
# MAGIC   SAD.ConveyanceActualDate,
# MAGIC   SAD.ConveyanceCurtailmentInd,
# MAGIC   SAD.CurtailmentStatus,
# MAGIC   SAD.ForeclosureHoldDate,
# MAGIC   SAD.ForeclosureHoldReason,
# MAGIC   SAD.RRCFollowUpDate,
# MAGIC   SAD.RRCCompletionDate,
# MAGIC   SAD.TitleGrade,
# MAGIC   SAD.FHATitleIssueId,
# MAGIC   SAD.TitleCureTime,
# MAGIC   SAD.AppraisalStatus,
# MAGIC   SAD.AppraisalTypeNeeded
# MAGIC FROM 
# MAGIC   silver_dev.CBA14.SERVICER_ASSET SA 
# MAGIC   LEFT OUTER JOIN silver_dev.CBA14.servicer_asset_disposition SAD ON SA.ID = SAD.ServicerAssetId 
# MAGIC   JOIN silver_dev.CBA14.SERVICER_SOURCE_FILE_STATUS SSFS ON SAD.StatusId = SSFS.ID 
# MAGIC   JOIN DWH.SERVICER_DIM SD ON SA.ServicerId = SD.SERVICER_ID 
# MAGIC   JOIN DWH.asset_dim AD ON SA.XpkKey = AD.XPK_Key
# MAGIC   JOIN DWH.DATE_DIM DD ON DATE_TRUNC('DAY', SA.src_xmd_InsertDateTimestamp) = DD.CALENDAR_DATE;