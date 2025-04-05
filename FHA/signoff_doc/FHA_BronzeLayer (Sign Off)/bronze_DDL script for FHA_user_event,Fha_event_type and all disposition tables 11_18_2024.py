# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS CBA14
# MAGIC MANAGED LOCATION 'abfss://bronze@adlsdidevcus.dfs.core.windows.net/fha/managed/cba14';				
# MAGIC USE cba14;
# MAGIC
# MAGIC CREATE TABLE cba14.SERVICER_ASSET_DISPOSITION_PATH (
# MAGIC     Id STRING,
# MAGIC src_xmd_InsertDateTimestamp	TIMESTAMP		NOT NULL	,
# MAGIC src_xmd_InsertUserId	STRING		NOT NULL	,
# MAGIC src_xmd_UpdateDateTimestamp	TIMESTAMP			,
# MAGIC src_xmd_UpdateUserId	STRING			,
# MAGIC src_xmd_DeleteDateTimestamp	TIMESTAMP			,
# MAGIC src_xmd_DeleteUserId	STRING			,
# MAGIC src_xmd_ChangeOperationCode	STRING	,
# MAGIC xmd_InsertDateTimestamp	TIMESTAMP		NOT NULL	,
# MAGIC xmd_InsertUserId	STRING		NOT NULL	,
# MAGIC xmd_UpdateDateTimestamp	TIMESTAMP			,
# MAGIC xmd_UpdateUserId	STRING			,
# MAGIC xmd_DeleteDateTimestamp	TIMESTAMP			,
# MAGIC xmd_DeleteUserId	STRING			,
# MAGIC xmd_ChangeOperationCode	STRING			,
# MAGIC DeleteInd	BOOLEAN	,
# MAGIC DispositionId	STRING	NOT NULL	,
# MAGIC DispositionPathTypeId	INT	NOT NULL	,
# MAGIC EventDate	DATE	NOT NULL	,
# MAGIC AdjustIfPastConveyanceTargetAmount	INT	NOT NULL	,
# MAGIC FHAEvictionDayCount	INT	NOT NULL	,
# MAGIC FHARepairsDayCount	INT	NOT NULL	,
# MAGIC MarketingDayCount	INT	NOT NULL	,
# MAGIC TitleCurativeDayCount	INT	NOT NULL	,
# MAGIC FHADateAdjustmentDayCount	INT	NOT NULL	,
# MAGIC ProjectedDispositionDate	DATE	NOT NULL	,
# MAGIC CurrentPropertyValueAmount	DECIMAL(38,19)	NOT NULL	,
# MAGIC CurrentPropertyValueTypeCode	INT	NOT NULL	,
# MAGIC CAFMVHaircutPercentage	DECIMAL(38,19)	NOT NULL	,
# MAGIC CAFMVAmount	DECIMAL(38,19)	NOT NULL	,
# MAGIC FHAValueAdjustmentAmount	DECIMAL(38,19)	NOT NULL	,
# MAGIC ProjectedSalesPrice	DECIMAL(38,19)	NOT NULL	,
# MAGIC ClosingCostAdjustmentFactorPercentage	DECIMAL(38,19)	NOT NULL	,
# MAGIC ProjectedNetCashfromClosingCost	DECIMAL(38,19)	NOT NULL	,
# MAGIC FHAEvictionCost	DECIMAL(38,19)	NOT NULL	,
# MAGIC FHACashForKeysCost	DECIMAL(38,19)	NOT NULL	,
# MAGIC TotalProjectedEvictionCosts	DECIMAL(38,19)	NOT NULL	,
# MAGIC LawnMaintenanceAmount	DECIMAL(38,19)	NOT NULL	,
# MAGIC FHAInitialSecureCost	DECIMAL(38,19)	NOT NULL	,
# MAGIC FHAWinterizationCost	DECIMAL(38,19)	NOT NULL	,
# MAGIC FHALockChangeCost	DECIMAL(38,19)	NOT NULL	,
# MAGIC FHADebrisRemovalCost	DECIMAL(38,19)	NOT NULL	,
# MAGIC FHAPoolSecuringInd	BOOLEAN	NOT NULL	,
# MAGIC FHAPoolSecuringaAmount	DECIMAL(38,19)	NOT NULL	,
# MAGIC FHASalesCleanCost	DECIMAL(38,19)	NOT NULL	,
# MAGIC FHAOtherPropPreservationExpenseAmount	DECIMAL(38,19)	NOT NULL	,
# MAGIC TotalProjectedPropertyPreservationCost	DECIMAL(38,19)	NOT NULL	,
# MAGIC FHAHOACondoDuesAmount	DECIMAL(38,19)	NOT NULL	,
# MAGIC TaxAndInsuranceAmount	DECIMAL(38,19)	NOT NULL	,
# MAGIC InspectionsCost	DECIMAL(38,19)	NOT NULL	,
# MAGIC UtilitiesCost	DECIMAL(38,19)	NOT NULL	,
# MAGIC TotalProjectedOnGoingCost	DECIMAL(38,19)	NOT NULL	,
# MAGIC FHARepairsCost	DECIMAL(38,19)	NOT NULL	,
# MAGIC FHALossDraftRepairOffsetAmount	DECIMAL(38,19)	NOT NULL	,
# MAGIC TotalProjectedSellingCost	DECIMAL(38,19)	NOT NULL	,
# MAGIC FHAAppraisalCost	DECIMAL(38,19)	NOT NULL	,
# MAGIC ProjectedExpensesCost	DECIMAL(38,19)	NOT NULL	,
# MAGIC FHAOutstandingMaintenanceClaimableAmount	DECIMAL(38,19)	NOT NULL	,
# MAGIC FHAOutstandingMaintenanceNonClaimableAmount	DECIMAL(38,19)	NOT NULL	,
# MAGIC TotalOutstandingMaintenanceAmount	DECIMAL(38,19)	NOT NULL	,
# MAGIC OutstandingRepairsCost	DECIMAL(38,19)	NOT NULL	,
# MAGIC FHAOutstandingRepairsClaimableAmount	DECIMAL(38,19)	NOT NULL	,
# MAGIC FHAOutstandingRepairsNonClaimableAmount	DECIMAL(38,19)	NOT NULL	,
# MAGIC TotalOutstandingRepairsCost	DECIMAL(38,19)	NOT NULL	,
# MAGIC FutureAdvancesAmount	DECIMAL(38,19)	NOT NULL	,
# MAGIC FutureInterestAmount	DECIMAL(38,19)	NOT NULL	,
# MAGIC TotalFutureExpensesAmount	DECIMAL(38,19)	NOT NULL	,
# MAGIC TotalExposureAmount	DECIMAL(38,19)	NOT NULL	,
# MAGIC CurrentBalanceAmount	DECIMAL(38,19)	NOT NULL	,
# MAGIC CurrentBalanceAmount	DECIMAL(38,19)	NOT NULL	,
# MAGIC CurrentDebentureInterestAmount	DECIMAL(38,19)	NOT NULL	,
# MAGIC ClaimFutureAdvancesAmount	DECIMAL(38,19)	NOT NULL	,
# MAGIC FutureDebentureInterestAmount	DECIMAL(38,19)	NOT NULL	,
# MAGIC FHANonRecoverableExpenseAdjustmentAmount	DECIMAL(38,19)	NOT NULL	,
# MAGIC ContractSalesPrice	DECIMAL(38,19)	NOT NULL	,
# MAGIC ClosingReimbursementCost	DECIMAL(38,19)	NOT NULL	,
# MAGIC FinalClaimsProceedsAmount	DECIMAL(38,19)	NOT NULL	,
# MAGIC SalesProceedsAmount	DECIMAL(38,19)	NOT NULL	,
# MAGIC ClaimProceedsAmount	DECIMAL(38,19)	NOT NULL	,
# MAGIC TotalNetGainLossAmount	DECIMAL(38,19)	NOT NULL
# MAGIC ) USING DELTA
# MAGIC location 'abfss://bronze@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/SERVICER_ASSET_DISPOSITION_PATH';
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS CBA14
# MAGIC MANAGED LOCATION 'abfss://bronze@adlsdidevcus.dfs.core.windows.net/fha/managed/cba14';				
# MAGIC USE cba14;
# MAGIC
# MAGIC
# MAGIC
# MAGIC CREATE TABLE cba14.SERVICER_ASSET_DISPOSITION (
# MAGIC     Id STRING,
# MAGIC src_xmd_InsertDateTimestamp	TIMESTAMP		NOT NULL	,
# MAGIC src_xmd_InsertUserId	STRING		NOT NULL	,
# MAGIC src_xmd_UpdateDateTimestamp	TIMESTAMP			,
# MAGIC src_xmd_UpdateUserId	STRING			,
# MAGIC src_xmd_DeleteDateTimestamp	TIMESTAMP			,
# MAGIC src_xmd_DeleteUserId	STRING			,
# MAGIC src_xmd_ChangeOperationCode	STRING	,
# MAGIC xmd_InsertDateTimestamp	TIMESTAMP		NOT NULL	,
# MAGIC xmd_InsertUserId	STRING		NOT NULL	,
# MAGIC xmd_UpdateDateTimestamp	TIMESTAMP			,
# MAGIC xmd_UpdateUserId	STRING			,
# MAGIC xmd_DeleteDateTimestamp	TIMESTAMP			,
# MAGIC xmd_DeleteUserId	STRING			,
# MAGIC xmd_ChangeOperationCode	STRING			,
# MAGIC DeleteInd	BOOLEAN	,
# MAGIC --SessionId	STRING		,
# MAGIC ServicerAssetId	STRING	NOT NULL	,
# MAGIC StatusId	INT	NOT NULL	,
# MAGIC RunDate	DATE	NOT NULL	,
# MAGIC FHA203kFundsAmount	DECIMAL(38,19)	NOT NULL	,
# MAGIC CurrentNetExposureTotalAmount	DECIMAL(38,19)	NOT NULL	,
# MAGIC DebentureRate	DECIMAL(38,19)	NOT NULL	,
# MAGIC DebentureInterestDate	DATE	NOT NULL	,
# MAGIC DefaultDate	DATE	NOT NULL	,
# MAGIC FCLApprovalDate	DATE		,
# MAGIC FirstLegalDueDate	DATE	NOT NULL	,
# MAGIC FirstLegalCurtailmentInd	BOOLEAN	NOT NULL	,
# MAGIC ForeclosureActualSaleDate	DATE	NOT NULL	,
# MAGIC DueDiligenceTargetDate	DATE	NOT NULL	,
# MAGIC DueDiligenceCurtailmentInd	BOOLEAN	NOT NULL	,
# MAGIC EvictionFirstLegalTargetDate	DATE		,
# MAGIC EvictionStartExtendedDate	DATE		,
# MAGIC FirstTimeVacancyDate	DATE		,
# MAGIC EvictionStartActualDate	DATE		,
# MAGIC EvictionStartCurtailmentInd	BOOLEAN	NOT NULL	,
# MAGIC CalculatedConveyanceTargetDate	DATE	NOT NULL	,
# MAGIC ConveyanceExtendedDate	DATE		,
# MAGIC ConveyanceActualDate	DATE		,
# MAGIC ConveyanceCurtailment	BOOLEAN	NOT NULL	,
# MAGIC CurtailmentStatusCode	STRING	NOT NULL	,
# MAGIC ForeclosureHoldDate	DATE		,
# MAGIC ForeclosureHoldReasonDescription	STRING		,
# MAGIC RRCFollowUpDate	DATE		,
# MAGIC RRCCompletionDate	DATE		,
# MAGIC TitleGradeCode	STRING		,
# MAGIC FHATitleIssueId	STRING	NOT NULL	,
# MAGIC TitleCureTime	INT	NOT NULL	,
# MAGIC AppraisalStatusCode	STRING	NOT NULL	,
# MAGIC AppraisalTypeNeededCode	STRING	NOT NULL	
# MAGIC     ) USING DELTA
# MAGIC location 'abfss://bronze@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/SERVICER_ASSET_DISPOSITION';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS CBA14
# MAGIC MANAGED LOCATION 'abfss://bronze@adlsdidevcus.dfs.core.windows.net/fha/managed/cba14';				
# MAGIC USE cba14;
# MAGIC
# MAGIC CREATE TABLE cba14.SERVICER_ASSET_DISPOSITION_SUMMARY (
# MAGIC Id	STRING	NOT NULL	,
# MAGIC src_xmd_InsertDateTimestamp	TIMESTAMP		NOT NULL	,
# MAGIC src_xmd_InsertUserId	STRING		NOT NULL	,
# MAGIC src_xmd_UpdateDateTimestamp	TIMESTAMP			,
# MAGIC src_xmd_UpdateUserId	STRING			,
# MAGIC src_xmd_DeleteDateTimestamp	TIMESTAMP			,
# MAGIC src_xmd_DeleteUserId	STRING			,
# MAGIC src_xmd_ChangeOperationCode	STRING	,
# MAGIC xmd_InsertDateTimestamp	TIMESTAMP		NOT NULL	,
# MAGIC xmd_InsertUserId	STRING		NOT NULL	,
# MAGIC xmd_UpdateDateTimestamp	TIMESTAMP			,
# MAGIC xmd_UpdateUserId	STRING			,
# MAGIC xmd_DeleteDateTimestamp	TIMESTAMP			,
# MAGIC xmd_DeleteUserId	STRING			,
# MAGIC xmd_ChangeOperationCode	STRING			,
# MAGIC DeleteInd	BOOLEAN	NOT NULL	,
# MAGIC DispositionId	STRING	NOT NULL	,
# MAGIC EstimatedConveyanceLossOrGainAmount	DECIMAL(38,19)	NOT NULL	,
# MAGIC EstimatedCWCOTLossOrGainAmount	DECIMAL(38,19)	NOT NULL	,
# MAGIC ContributionAmount	DECIMAL(38,19)	NOT NULL	,
# MAGIC FHAContributionPercent	DECIMAL(38,19)	NOT NULL	,
# MAGIC EstimatedCWCOTOptimizedSavingsAmount	DECIMAL(38,19)	NOT NULL	,
# MAGIC ConveyanceMonthCount	INT	NOT NULL	,
# MAGIC ConveyanceDayCount	INT	NOT NULL	,
# MAGIC CWCOTSaleMonthCount	INT	NOT NULL	,
# MAGIC CWCOTSaleDayCount	INT	NOT NULL	
# MAGIC
# MAGIC  ) USING DELTA
# MAGIC location 'abfss://bronze@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/SERVICER_ASSET_DISPOSITION_SUMMARY';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS CBA14
# MAGIC MANAGED LOCATION 'abfss://bronze@adlsdidevcus.dfs.core.windows.net/fha/managed/cba14';				
# MAGIC USE cba14;
# MAGIC
# MAGIC CREATE TABLE cba14.FHA_USER_EVENT
# MAGIC (
# MAGIC ID BIGINT ,
# MAGIC xmd_InsertDateTimestamp	TIMESTAMP		NOT NULL	,
# MAGIC xmd_InsertUserId	STRING		NOT NULL	,
# MAGIC xmd_UpdateDateTimestamp	TIMESTAMP			,
# MAGIC xmd_UpdateUserId	STRING			,
# MAGIC xmd_DeleteDateTimestamp	TIMESTAMP			,
# MAGIC xmd_DeleteUserId	STRING			,
# MAGIC xmd_ChangeOperationCode	STRING			,
# MAGIC src_xmd_InsertDateTimestamp	TIMESTAMP		NOT NULL	,
# MAGIC src_xmd_InsertUserId	STRING		NOT NULL	,
# MAGIC src_xmd_UpdateDateTimestamp	TIMESTAMP			,
# MAGIC src_xmd_UpdateUserId	STRING			,
# MAGIC src_xmd_DeleteDateTimestamp	TIMESTAMP			,
# MAGIC src_xmd_DeleteUserId	STRING			,
# MAGIC src_xmd_ChangeOperationCode	STRING			,
# MAGIC DeleteInd BOOLEAN ,
# MAGIC EventTypeId INT NOT NULL,
# MAGIC ActionCode STRING ,
# MAGIC EventDetailDescription STRING
# MAGIC )USING DELTA
# MAGIC location 'abfss://bronze@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/fha_user_event';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS CBA14
# MAGIC MANAGED LOCATION 'abfss://bronze@adlsdidevcus.dfs.core.windows.net/fha/managed/cba14';				
# MAGIC USE cba14;
# MAGIC
# MAGIC CREATE TABLE cba14.FHA_USER_EVENT_TYPE
# MAGIC (
# MAGIC ID BIGINT NOT NULL ,
# MAGIC xmd_InsertDateTimestamp	TIMESTAMP		NOT NULL	,
# MAGIC xmd_InsertUserId	STRING		NOT NULL	,
# MAGIC xmd_UpdateDateTimestamp	TIMESTAMP			,
# MAGIC xmd_UpdateUserId	STRING			,
# MAGIC xmd_DeleteDateTimestamp	TIMESTAMP			,
# MAGIC xmd_DeleteUserId	STRING			,
# MAGIC xmd_ChangeOperationCode	STRING	,
# MAGIC Name STRING NOT NULL		
# MAGIC )USING DELTA
# MAGIC location 'abfss://bronze@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/fha_user_event_type';