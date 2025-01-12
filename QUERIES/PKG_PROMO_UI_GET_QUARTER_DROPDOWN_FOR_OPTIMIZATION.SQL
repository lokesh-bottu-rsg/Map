WITH CTE_PERIOD_ID AS(
            SELECT PERIOD_ID FROM REF_CAL_PERIOD WHERE CURRENT_TIMESTAMP() between start_date and end_date and PERIOD_TYPE_CD='Q'
)
,CTE_START_DATE AS (
            select start_date from REF_CAL_PERIOD where DATEADD(DAY,-95,CURRENT_DATE) between start_date and end_date and PERIOD_TYPE_CD='Q'
)
,CTE_END_DATE AS (
            select end_date from REF_CAL_PERIOD RF
            JOIN CTE_PERIOD_ID CI ON 1=1
            where RF.PERIOD_ID=CI.PERIOD_ID+4 AND PERIOD_TYPE_CD='Q'
)
        SELECT 
        RCP.PERIOD_ID, 
        RCP.START_DATE,
        RCP.END_DATE,
        RCP.PERIOD_TYPE_CD,
        RCP.NAME,
        RCP.FISCAL_QTR
        FROM REF_CAL_PERIOD RCP
        JOIN CTE_START_DATE CS ON 1=1
        JOIN CTE_END_DATE CE ON 1=1
        WHERE RCP.PERIOD_TYPE_CD = 'Q' 
        AND RCP.start_date>=CS.start_date and RCP.end_date<=CE.end_date order by start_Date;