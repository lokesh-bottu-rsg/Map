CREATE OR REPLACE PROCEDURE PKG_MODEL_MAP_VALIDATE_DAILY_CAP(
    IN_PLAN_ID INT,
    IN_PLAN_VERSION_ID INT
)
RETURNS TABLE (PROJECT_NAME VARCHAR, WEEKS VARCHAR)
LANGUAGE SQL AS
DECLARE 
    V_MEDIA_OUTLET_ID INT DEFAULT 0;
    RES RESULTSET;
BEGIN

    SELECT MEDIA_OUTLET_ID INTO :V_MEDIA_OUTLET_ID FROM PR_PLAN PP WHERE PP.PLAN_ID = :IN_PLAN_ID;
    RES := (
        WITH CTE_PPR AS (
            SELECT 
            PP.PLAN_ID,PPV.PLAN_VERSION_ID,PPP.PROJECT_ID,
            CONCAT(PPR.NAME,' (',RMO.SHORT_NAME, ')') NAME,PPR.PREMIERE_DATE,PPPS.WEEK_DATE,
            PPPS.UNIT_COUNT,
            PPP.ON_MAP_START_DATE START_DATE,PPP.ON_MAP_END_DATE END_DATE,DS.DAILY_SPOTS,DS.PER_HOUR_SPOTS,
            GET_TIME_TO_OFFSET(TIME(CASE WHEN DATE(PPR.START_DATE)=PPP.ON_MAP_START_DATE THEN 
                PPR.START_DATE ELSE '06:00:00' END )) START_OFFSET,
            GET_TIME_TO_OFFSET(TIME(CASE WHEN DATE(PPR.END_DATE)=PPP.ON_MAP_END_DATE THEN 
                PPR.END_DATE ELSE '30:00:00' END )) END_OFFSET
            FROM PR_PLAN PP
            JOIN PR_PLAN_VERSION PPV
            ON PP.PLAN_ID = PPV.PLAN_ID
            JOIN PR_PLAN_PROJECT PPP
            ON PPP.PLAN_ID = PP.PLAN_ID
            AND PPP.PLAN_VERSION_ID = PPV.PLAN_VERSION_ID
            JOIN PR_PLAN_PROJECT_SPOTS PPPS
            ON PPP.PROJECT_ID = PPPS.PROJECT_ID
                AND PPPS.PLAN_ID = PP.PLAN_ID
                AND PPPS.PLAN_VERSION_ID = PPV.PLAN_VERSION_ID
                AND PPPS.WEEK_DATE BETWEEN PKG_GET_MONDAYS_DATE(PPP.ON_MAP_START_DATE) AND (PPP.ON_MAP_END_DATE)
            JOIN PR_PROJECT PPR ON PPR.PROJECT_ID = PPP.PROJECT_ID
            JOIN 
            (
                SELECT ROUND(SUM(SPOTS),0) DAILY_SPOTS, MIN(SPOTS) PER_HOUR_SPOTS FROM REF_SPOTS_PER_HOUR RSPH
                WHERE RSPH.MEDIA_OUTLET_ID = :V_MEDIA_OUTLET_ID
                GROUP BY RSPH.MEDIA_OUTLET_ID
            ) DS on 1=1
            JOIN REF_MEDIA_OUTLET RMO on PPR.MEDIA_OUTLET_ID=RMO.MEDIA_OUTLET_ID
            WHERE PP.PLAN_ID = :IN_PLAN_ID
            AND PPV.PLAN_VERSION_ID = :IN_PLAN_VERSION_ID
            AND PPP.OPTIMIZE_YN = 'Y'
        )
        , CTE_A AS (
            SELECT PPR.PLAN_ID,PPR.PLAN_VERSION_ID,PPR.PROJECT_ID,PPR.NAME,PPR.START_DATE, PPR.END_DATE,
                PPR.PREMIERE_DATE,PPR.WEEK_DATE,PPR.UNIT_COUNT, PPR.START_OFFSET, PPR.END_OFFSET,
                CASE WHEN DATEDIFF(DAY, DATE(PPR.START_DATE),PPR.WEEK_DATE) <= 0 
                    AND DATEDIFF(DAY,DATE(PPR.END_DATE),DATEADD(DAY,1,PPR.WEEK_DATE)) >= 0 THEN 
                        CASE WHEN DATEDIFF(DAY,DATE(PPR.START_DATE),DATE(PPR.END_DATE)) = 0 
                            THEN ((END_OFFSET-START_OFFSET)/3600.0)* PPR.PER_HOUR_SPOTS
                            WHEN DATEDIFF(DAY, DATE(PPR.START_DATE),DATE(PPR.END_DATE)) = 1 
                            
                            THEN (((86400-START_OFFSET)+ END_OFFSET)/3600.0) * PPR.PER_HOUR_SPOTS
                            ELSE 
                            
                            ((((86400-START_OFFSET)+ END_OFFSET)/3600.0) * PPR.PER_HOUR_SPOTS) +
                            (DATEDIFF(DAY, DATE(PPR.START_DATE)-1,DATE(PPR.END_DATE))* PPR.DAILY_SPOTS)
                            END
                            
                        WHEN DATEDIFF(DAY, DATE(PPR.START_DATE),PPR.WEEK_DATE) <= 0 
                        AND DATEDIFF(DAY, DATE(PPR.END_DATE),DATEADD(DAY,6,PPR.WEEK_DATE)) < 0
                        THEN 
                        CASE 
                            WHEN DATEDIFF(DAY, DATE(PPR.START_DATE),DATE(PPR.END_DATE)) = 1 
                                                
                            THEN ((86400-START_OFFSET)/3600.0) * PPR.PER_HOUR_SPOTS                    				
                        ELSE
                            ((86400-START_OFFSET)/3600.0) * PPR.PER_HOUR_SPOTS   +
                            DATEDIFF(DAY, DATE(PPR.START_DATE),DATEADD(DAY,6,PPR.WEEK_DATE))*PPR.DAILY_SPOTS
                        END
                
                        WHEN DATEDIFF(DAY, DATE(PPR.START_DATE),PPR.WEEK_DATE) > 0 
                        AND DATEDIFF(DAY,DATE(PPR.END_DATE),DATEADD(DAY,6,PPR.WEEK_DATE)) < 0
                        THEN 7 * PPR.DAILY_SPOTS
                        
                        WHEN DATEDIFF(DAY,DATE(PPR.START_DATE),PPR.WEEK_DATE) > 0 
                        AND DATEDIFF(DAY,DATE(PPR.END_DATE),DATEADD(DAY,6,PPR.WEEK_DATE)) >= 0       
                    THEN 
                        CASE 
                            WHEN DATEDIFF(DAY,DATE(PPR.START_DATE),DATE(PPR.END_DATE)) = 1 
                            THEN (PPR.END_OFFSET/3600.0) * PPR.PER_HOUR_SPOTS                    				
                        ELSE
                            (PPR.END_OFFSET/3600.0) * PPR.PER_HOUR_SPOTS   +
                            DATEDIFF(DAY,PPR.WEEK_DATE,DATE(PPR.END_DATE))* PPR.DAILY_SPOTS
                        END
                    END WEEKLY_CAP FROM CTE_PPR PPR
        )
        ,CTE_FINAL AS (
        SELECT A.PLAN_ID,A.PLAN_VERSION_ID,A.PROJECT_ID,A.NAME,A.START_DATE,
            A.PREMIERE_DATE,A.WEEK_DATE,A.UNIT_COUNT,A.WEEKLY_CAP FROM CTE_A A
        ) -- SELECT NAME,WEEK_DATE FROM CTE_FINAL

        SELECT NAME PROJECT_NAME,LISTAGG(WEEK_DATE,',') WITHIN GROUP (ORDER BY WEEK_DATE) WEEKS 
        FROM CTE_FINAL GROUP BY PROJECT_ID,NAME
    );

RETURN TABLE (RES);
END;

GRANT USAGE ON PROCEDURE PKG_MODEL_MAP_VALIDATE_DAILY_CAP(INT,INT) TO ROLE RA_PROMO_DEV_QA_UAT_ACCESS;
GRANT USAGE ON PROCEDURE PKG_MODEL_MAP_VALIDATE_DAILY_CAP(INT,INT) TO ROLE RA_PROMO_PROD_ACCESS;