
CREATE OR REPLACE PROCEDURE PKG_PROMO_UI_REFRESH_MAP_VALUES(IN_CALL_ID INT)
RETURNS TABLE( CALL_ID INT,PROJECT_ID INT,WEEK_DATE VARCHAR,WEEK_NAME VARCHAR,UNIT_COUNT INT)
LANGUAGE SQL AS 
DECLARE
    V_CALL_ID INT;
    V_MEDIA_OUTLET_ID INT;
    V_PROJECT_ID INT;
    OUT_DONE INT DEFAULT 0;
    V_CALC_BASED_ON VARCHAR(2);
    V_WEEK_COUNT INT;
    V_DEMO_ID INT;
    V_SEGMENT_ID INT;
    V_FREQUENCY INT;
    V_MIN_UNIT_COUNT INT;
    V_MAX_UNIT_COUNT INT;
    V_MIN_REACH_PCT NUMBER(38,2);
    V_MAX_REACH_PCT NUMBER(38,2);
    V_MIN_GRP DOUBLE;
    V_MAX_GRP DOUBLE;
    V_TARGET_REACH_PCT NUMBER(38,2);
    V_TARGET_GRPS FLOAT;
    V_TOTAL_SPOT INT;
    V_SPOT_COUNT INT;
    V_GRP DOUBLE;
    V_REACH_PCT NUMBER(38,2);
    V_WEEK_DATE DATE;
    V_CUR_WEEK_START_DATE DATE;
    V_END_DATE DATE;
    V_COUNT INT;
    V_REACH_GUIDE_DATA_EXISTS INT;
    V_QTR_START_DATE DATE;
    V_DAYS_IN_WEEK INT;
    V_TOTAL_DAYS INT;
    V_PROJ_ID INT;
    V_SPOT_COUNT_TEMP INT;
    V_TOTAL_DAYS_TEMP INT;
    V_PROJ_ORIG_WEEK_COUNT INT;
    V_REACH_ID INT;
    V_PROJECTS_MO_ID INT;
    V_AVAILABLE_WEEK_COUNTS INT;
    V_AVAILABLE_UNIT_COUNTS INT;
    RES RESULTSET;

BEGIN

    
    DELETE FROM TMP_REACH_GUIDE WHERE CALL_ID = :IN_CALL_ID;
    
    DELETE FROM TMP_PROJECT_DISTRIBUTION WHERE CALL_ID = :IN_CALL_ID;

    SELECT RCP.START_DATE INTO :V_CUR_WEEK_START_DATE FROM REF_CAL_PERIOD RCP
	WHERE PERIOD_TYPE_CD = 'W' AND CURRENT_TIMESTAMP() BETWEEN START_DATE AND END_DATE;

    CREATE OR REPLACE TABLE TMP_REFRESH_MAP AS SELECT * FROM TEMP_REFRESH_REACH_CALCULATION WHERE CALL_ID = :IN_CALL_ID;

    LET MAP_PROJECTS_CURSOR CURSOR FOR
        SELECT TRRC.CALL_ID, 
            TRRC.MEDIA_OUTLET_ID, 
            TRRC.PROJECT_ID,
            TRRC.CALC_BASED_ON,
            TRRC.FREQUENCY_ID,
            TRRC.TARGET_REACH_PCT,
            TRRC.TARGET_GRPS,
            TRRC.PLANNED_SPOT,
            TRRC.QTR_START_DATE,
            TRRC.DEMO_ID
        FROM TMP_REFRESH_MAP TRRC
        ORDER BY TRRC.PROJECT_ID;

    OPEN MAP_PROJECTS_CURSOR;
    FOR ROW_VARIABLE IN MAP_PROJECTS_CURSOR DO

        V_CALL_ID := ROW_VARIABLE.CALL_ID;
        V_MEDIA_OUTLET_ID := ROW_VARIABLE.MEDIA_OUTLET_ID;
        V_PROJECT_ID := ROW_VARIABLE.PROJECT_ID;
        V_CALC_BASED_ON := ROW_VARIABLE.CALC_BASED_ON;
        V_FREQUENCY := ROW_VARIABLE.FREQUENCY_ID;
        V_TARGET_REACH_PCT := ROW_VARIABLE.TARGET_REACH_PCT;
        V_TARGET_GRPS := ROW_VARIABLE.TARGET_GRPS;
        V_TOTAL_SPOT := ROW_VARIABLE.PLANNED_SPOT;
        V_QTR_START_DATE := ROW_VARIABLE.QTR_START_DATE;
        V_DEMO_ID := ROW_VARIABLE.DEMO_ID;


        SELECT ROUND((DATEDIFF(DAY,TRRC.FLIGHT_START_DATE,TRRC.FLIGHT_END_DATE)+1)/7,0),
    		TO_CHAR(TRRC.FLIGHT_END_DATE,'YYYY-MM-DD'),
            PP.MEDIA_OUTLET_ID,
            COALESCE(TRRC.SEGMENT_ID, PP.TARGET_SEGMENT),
            DATEDIFF(DAY,CASE WHEN TRRC.FLIGHT_START_DATE < :V_QTR_START_DATE 
    							THEN :V_QTR_START_DATE ELSE TRRC.FLIGHT_START_DATE END,TRRC.FLIGHT_END_DATE)+1
                                
        	INTO :V_WEEK_COUNT,:V_END_DATE,:V_PROJECTS_MO_ID,:V_SEGMENT_ID,:V_TOTAL_DAYS
        	FROM PR_PROJECT PP
            JOIN TEMP_REFRESH_REACH_CALCULATION TRRC ON TRRC.PROJECT_ID = PP.PROJECT_ID
            AND PP.PROJECT_ID = :V_PROJECT_ID 
            AND TRRC.CALL_ID = :V_CALL_ID;

        IF (:V_PROJECTS_MO_ID <> :V_MEDIA_OUTLET_ID) THEN
    		SELECT SEGMENT_ID INTO :V_SEGMENT_ID
            FROM REF_SEGMENT WHERE SEGMENT_AIRBYTE = :V_SEGMENT_ID;
        END IF;

        IF (:V_END_DATE < :V_CUR_WEEK_START_DATE) THEN
    		DELETE FROM TEMP_REFRESH_REACH_CALCULATION
            WHERE CALL_ID = :IN_CALL_ID
            AND PROJECT_ID = :V_PROJECT_ID;
        ELSE
        
            IF (:V_WEEK_COUNT > 26) THEN
                SET V_WEEK_COUNT:= 26;
            ELSE
                IF (:V_WEEK_COUNT < 1) THEN
                    SET V_WEEK_COUNT := 1;
                END IF;
            END IF;

            SELECT COUNT(*) INTO :V_REACH_GUIDE_DATA_EXISTS
            FROM TMP_REACH_GUIDE WHERE CALL_ID = :IN_CALL_ID
            AND TARGET_DEMO_ID = :V_DEMO_ID
            AND WEEK_COUNT= :V_WEEK_COUNT
            AND FREQUENCY = :V_FREQUENCY
            AND SEGMENT_ID = :V_SEGMENT_ID;


            IF (:V_REACH_GUIDE_DATA_EXISTS = 0) THEN

                -- Fetching reach ID of the reach guide of 2 quarters prior to Map's quarter
                -- If map is for Quarter 3Q19, then reach guide data will be retrieved for 1Q19
                SELECT COALESCE(MAX(REACH_ID),0) INTO :V_REACH_ID
                FROM REACH_HEADER RH
                JOIN REF_CAL_PERIOD RCP
                ON RH.BASE_PERIOD_END_DATE = RCP.END_DATE AND RH.BASE_PERIOD_START_DATE = RCP.START_DATE
                AND RH.TARGET_DEMO_ID = :V_DEMO_ID AND RH.MEDIA_OUTLET_ID = :V_MEDIA_OUTLET_ID
                AND RCP.PERIOD_TYPE_CD = 'Q'
                AND RCP.END_DATE = (SELECT MAX(P.END_DATE) FROM REF_CAL_PERIOD P 
                    WHERE P.END_DATE < CASE :V_MEDIA_OUTLET_ID WHEN 1 THEN :V_QTR_START_DATE ELSE DATEADD(DAY,1,TO_DATE(:V_QTR_START_DATE)) END
                    AND P.PERIOD_TYPE_CD = 'Q');
    
                    
                -- If reach guide data isnt available to the latest quarter (Current quarter - 2 quarters)
                -- then it will fetch latest reach id for the given network and demo
                IF (:V_REACH_ID = 0) THEN
                    SELECT MAX(REACH_ID) INTO :V_REACH_ID
                    FROM REACH_HEADER RH
                    WHERE RH.MEDIA_OUTLET_ID = :V_MEDIA_OUTLET_ID
                    AND RH.TARGET_DEMO_ID = :V_DEMO_ID;
                END IF;
                
                -- Checking maximum week counts and unit counts of the given reach ID
                -- Change was made to handle cases when reach guide would be generated for max 2000 units and max 13 weeks
                SELECT MAX(WEEK_COUNT), MAX(UNIT_COUNT)
                INTO :V_AVAILABLE_WEEK_COUNTS, :V_AVAILABLE_UNIT_COUNTS
                FROM REACH_DETAIL WHERE REACH_ID = :V_REACH_ID;
                
    
                -- Assigning max week count as the available week count if project has more weeks than the max available
                -- week count
                IF (:V_WEEK_COUNT > :V_AVAILABLE_WEEK_COUNTS) THEN
                    SET V_WEEK_COUNT := :V_AVAILABLE_WEEK_COUNTS;
                END IF;
    
    
                INSERT INTO TMP_REACH_GUIDE (CALL_ID,TARGET_DEMO_ID,SEGMENT_ID,WEEK_COUNT,
                FREQUENCY,UNIT_COUNT, REACH_PCT,COV_GRP,GRP)
                
                SELECT :IN_CALL_ID,RH.TARGET_DEMO_ID,RS.SEGMENT_ID,RD.WEEK_COUNT,RD.FREQUENCY,RD.UNIT_COUNT, 
                ROUND(SUM(REACH)*100.00/SUM(UNIVERSE),2) REACH_PCT,
                ROUND(AVG(COV_GRP),0) COV_GRP,				
                MAX(G.GRP) GRP
                FROM REACH_HEADER RH
                INNER JOIN REACH_DETAIL RD
                ON RD.REACH_ID = RH.REACH_ID
                AND RD.REACH_ID = :V_REACH_ID
                INNER JOIN REF_SEGMENT_COMPONENT RSC ON RSC.COMPONENT_SEGMENT_ID = RD.SEGMENT_ID
                INNER JOIN REF_SEGMENT RS ON RS.SEGMENT_ID = RSC.SEGMENT_ID
                INNER JOIN 
                (
                    SELECT A.UNIT_COUNT,A.TARGET_DEMO_ID,A.WEEK_COUNT,A.FREQUENCY,A.SEGMENT_ID,
                    ROUND(ROUND(AVG(A.IMPS),0)*100.0/GET_NAT_UNIVERSE(:V_DEMO_ID),2) GRP
                    FROM 
                    (
                        SELECT RDT.UNIT_COUNT,RHD.TARGET_DEMO_ID,
                        RDT.WEEK_COUNT,RDT.FREQUENCY,RDT.START_DATE,RDT.END_DATE,RS.SEGMENT_ID,
                        SUM(IMPRESSIONS) IMPS,
                        SUM(GRP) GRP
                        FROM REACH_HEADER RHD
                        INNER JOIN REACH_DETAIL RDT
                        ON RDT.REACH_ID = RHD.REACH_ID
                        INNER JOIN REF_SEGMENT_COMPONENT RSC ON RSC.COMPONENT_SEGMENT_ID = RDT.SEGMENT_ID
                        INNER JOIN REF_SEGMENT RS ON RS.SEGMENT_ID = RSC.SEGMENT_ID
                        WHERE RHD.MEDIA_OUTLET_ID = :V_MEDIA_OUTLET_ID
                            AND RHD.TARGET_DEMO_ID = :V_DEMO_ID
                            AND RDT.FREQUENCY = 1
                            AND RS.SEGMENT_ID = :V_SEGMENT_ID
                            AND RHD.REACH_ID = :V_REACH_ID
                        GROUP BY RDT.UNIT_COUNT,RHD.TARGET_DEMO_ID,RDT.WEEK_COUNT,
                            RDT.FREQUENCY,RDT.START_DATE,RDT.END_DATE,RS.SEGMENT_ID
                    )A
                    GROUP BY A.UNIT_COUNT,A.TARGET_DEMO_ID,A.WEEK_COUNT,
                        A.FREQUENCY,A.SEGMENT_ID,A.START_DATE,A.END_DATE
                )G 
                        
                ON G.UNIT_COUNT = RD.UNIT_COUNT
                AND G.TARGET_DEMO_ID = RH.TARGET_DEMO_ID
                AND G.WEEK_COUNT = RD.WEEK_COUNT
                AND G.SEGMENT_ID = RS.SEGMENT_ID
                
                WHERE RH.MEDIA_OUTLET_ID = :V_MEDIA_OUTLET_ID
                    AND RH.TARGET_DEMO_ID = :V_DEMO_ID
                    AND RH.REACH_ID = :V_REACH_ID
                    AND RS.SEGMENT_ID = :V_SEGMENT_ID
                GROUP BY RD.UNIT_COUNT,RH.TARGET_DEMO_ID,RS.SEGMENT_ID,
                    RD.WEEK_COUNT,RD.FREQUENCY;
            END IF;




            IF (:V_CALC_BASED_ON = 'R') THEN

                WITH CTE_CNT AS (
                    SELECT MAX(UNIT_COUNT) CNT FROM TMP_REACH_GUIDE RG WHERE RG.REACH_PCT <= :V_TARGET_REACH_PCT
                            AND RG.TARGET_DEMO_ID = :V_DEMO_ID AND RG.SEGMENT_ID = :V_SEGMENT_ID AND RG.FREQUENCY = :V_FREQUENCY 
                            AND RG.CALL_ID = :V_CALL_ID AND RG.WEEK_COUNT = :V_WEEK_COUNT
                )
                SELECT COALESCE(MAX(UNIT_COUNT),0), 
                COALESCE(MAX(REACH_PCT),0),COALESCE(MAX(GRP),0)
                INTO :V_MIN_UNIT_COUNT,:V_MIN_REACH_PCT,:V_MIN_GRP
                FROM TMP_REACH_GUIDE TRG
                WHERE UNIT_COUNT = (SELECT CNT FROM CTE_CNT)
                AND TRG.TARGET_DEMO_ID = :V_DEMO_ID
                    AND TRG.SEGMENT_ID = :V_SEGMENT_ID
                    AND TRG.FREQUENCY = :V_FREQUENCY
                    AND TRG.CALL_ID = :V_CALL_ID
                    AND TRG.WEEK_COUNT = :V_WEEK_COUNT;


    
                WITH CTE_CNT AS (
                    SELECT MIN(UNIT_COUNT) CNT FROM TMP_REACH_GUIDE RG
                        WHERE RG.REACH_PCT >= :V_TARGET_REACH_PCT AND RG.TARGET_DEMO_ID = :V_DEMO_ID AND RG.SEGMENT_ID = :V_SEGMENT_ID 
                        AND RG.FREQUENCY = :V_FREQUENCY AND RG.CALL_ID = :V_CALL_ID AND RG.WEEK_COUNT = :V_WEEK_COUNT
                )
                SELECT COALESCE(MAX(UNIT_COUNT),0), 
                    COALESCE(MAX(REACH_PCT),0),
                    COALESCE(MAX(GRP),0)
                INTO :V_MAX_UNIT_COUNT,:V_MAX_REACH_PCT,:V_MAX_GRP
                FROM TMP_REACH_GUIDE TRG
                WHERE UNIT_COUNT = (SELECT CNT FROM CTE_CNT)
                AND TRG.TARGET_DEMO_ID = :V_DEMO_ID
                    AND TRG.SEGMENT_ID = :V_SEGMENT_ID
                    AND TRG.FREQUENCY = :V_FREQUENCY
                    AND TRG.CALL_ID = :V_CALL_ID
                    AND TRG.WEEK_COUNT = :V_WEEK_COUNT;
    
    
    
                IF (:V_MIN_UNIT_COUNT <> :V_MAX_UNIT_COUNT) THEN
                
                    SET V_SPOT_COUNT := :V_MIN_UNIT_COUNT + 
                        ROUND(((:V_MAX_UNIT_COUNT - :V_MIN_UNIT_COUNT) *
                            (:V_TARGET_REACH_PCT - :V_MIN_REACH_PCT)) /
                            (:V_MAX_REACH_PCT - :V_MIN_REACH_PCT),0);
    
                    SET V_GRP := :V_MIN_GRP + 
                        ROUND(((:V_MAX_GRP - :V_MIN_GRP) *
                            (:V_TARGET_REACH_PCT - :V_MIN_REACH_PCT)) /
                            (:V_MAX_REACH_PCT - :V_MIN_REACH_PCT),2);
                    
                    IF (:V_MIN_UNIT_COUNT > :V_MAX_UNIT_COUNT) THEN
                        SET V_SPOT_COUNT := :V_MAX_UNIT_COUNT;
                        SET V_GRP := :V_MAX_GRP;
                    END IF;
                    
                    UPDATE TEMP_REFRESH_REACH_CALCULATION
                    SET SPOT_RESULT = :V_SPOT_COUNT,REACH_PCT_RESULT = :V_TARGET_REACH_PCT,GRPS_RESULT = ROUND(:V_GRP,1)
                    WHERE CALL_ID = :V_CALL_ID
                    AND PROJECT_ID = :V_PROJECT_ID;
                    
                ELSE
                    SET V_SPOT_COUNT := :V_MIN_UNIT_COUNT;
                    
                    UPDATE TEMP_REFRESH_REACH_CALCULATION 
                    SET SPOT_RESULT = :V_MIN_UNIT_COUNT, REACH_PCT_RESULT = :V_TARGET_REACH_PCT, GRPS_RESULT = ROUND(:V_MIN_GRP,1)
                    WHERE CALL_ID = :V_CALL_ID AND PROJECT_ID = :V_PROJECT_ID;
                    
                END IF;
    
    
                -- Assigning maximum units when even max available units in Reach guide doesnt give desired target
                -- value
                IF (:V_MAX_UNIT_COUNT IS NULL OR :V_MAX_UNIT_COUNT = 0) THEN
                
                    SELECT UNIT_COUNT, REACH_PCT,GRP
                    INTO :V_MIN_UNIT_COUNT,:V_MIN_REACH_PCT,:V_MIN_GRP
                    FROM TMP_REACH_GUIDE TRG
                    WHERE UNIT_COUNT = :V_AVAILABLE_UNIT_COUNTS
                    AND TRG.TARGET_DEMO_ID = :V_DEMO_ID
                    AND TRG.SEGMENT_ID = :V_SEGMENT_ID
                    AND TRG.FREQUENCY = :V_FREQUENCY
                    AND TRG.CALL_ID = :V_CALL_ID
                    AND TRG.WEEK_COUNT = :V_WEEK_COUNT;
                    
                    SET V_SPOT_COUNT := :V_MIN_UNIT_COUNT;
                    
                    UPDATE TEMP_REFRESH_REACH_CALCULATION
                    SET SPOT_RESULT = :V_MIN_UNIT_COUNT,
                        REACH_PCT_RESULT = :V_MIN_REACH_PCT,
                        GRPS_RESULT = ROUND(:V_MIN_GRP,1)
                    WHERE CALL_ID = :V_CALL_ID
                    AND PROJECT_ID = :V_PROJECT_ID;
                END IF;


            ELSEIF (:V_CALC_BASED_ON = 'G') THEN

                WITH CTE_CNT AS (
                    SELECT MAX(UNIT_COUNT) CNT FROM TMP_REACH_GUIDE RG WHERE RG.GRP <= :V_TARGET_GRPS AND RG.TARGET_DEMO_ID = :V_DEMO_ID
                        AND RG.SEGMENT_ID = :V_SEGMENT_ID AND RG.FREQUENCY = :V_FREQUENCY AND RG.CALL_ID = :V_CALL_ID
                )

                SELECT COALESCE(MAX(UNIT_COUNT),0), COALESCE(MAX(REACH_PCT),0),COALESCE(MAX(GRP),0)
                INTO :V_MIN_UNIT_COUNT,:V_MIN_REACH_PCT,:V_MIN_GRP
                -- FROM TMP_REACH_GUIDE TRG INNER JOIN CTE_CNT CC ON CC.CNT = TRG.UNIT_COUNT
                FROM TMP_REACH_GUIDE TRG WHERE UNIT_COUNT = (SELECT CNT FROM CTE_CNT)
                AND TRG.TARGET_DEMO_ID = :V_DEMO_ID AND TRG.SEGMENT_ID = :V_SEGMENT_ID AND TRG.FREQUENCY = :V_FREQUENCY 
                AND TRG.CALL_ID = :V_CALL_ID AND TRG.WEEK_COUNT = :V_WEEK_COUNT;

                
                WITH CTE_CNT AS (
                    SELECT MIN(UNIT_COUNT) CNT FROM TMP_REACH_GUIDE RG WHERE RG.GRP >= :V_TARGET_GRPS AND RG.TARGET_DEMO_ID = :V_DEMO_ID
                        AND RG.SEGMENT_ID = :V_SEGMENT_ID AND RG.FREQUENCY = :V_FREQUENCY AND RG.CALL_ID = :V_CALL_ID 
                        AND RG.WEEK_COUNT = :V_WEEK_COUNT 
                )
                SELECT COALESCE(MAX(UNIT_COUNT),0),COALESCE(MAX(REACH_PCT),0),COALESCE(MAX(GRP),0)
                INTO :V_MAX_UNIT_COUNT,:V_MAX_REACH_PCT,:V_MAX_GRP
                FROM TMP_REACH_GUIDE TRG WHERE UNIT_COUNT = (SELECT CNT FROM CTE_CNT) AND TRG.TARGET_DEMO_ID = :V_DEMO_ID
                    AND TRG.SEGMENT_ID = :V_SEGMENT_ID AND TRG.FREQUENCY = :V_FREQUENCY AND TRG.CALL_ID = :V_CALL_ID 
                    AND TRG.WEEK_COUNT = :V_WEEK_COUNT;
                    
                    
                IF (:V_MIN_UNIT_COUNT <> :V_MAX_UNIT_COUNT) THEN
                
                    SET V_SPOT_COUNT := :V_MIN_UNIT_COUNT + 
                        ROUND(((:V_MAX_UNIT_COUNT - :V_MIN_UNIT_COUNT) *
                            (:V_TARGET_GRPS - :V_MIN_GRP)) /
                            (:V_MAX_GRP - :V_MIN_GRP),0);
                    
                    SET V_REACH_PCT := :V_MIN_REACH_PCT + 
                        ROUND(((:V_MAX_REACH_PCT - :V_MIN_REACH_PCT) * 
                            (:V_TARGET_GRPS - :V_MIN_GRP))/
                            (:V_MAX_GRP - :V_MIN_GRP) ,2);
    
                    SET V_GRP := :V_MIN_GRP + 
                        ROUND(((:V_MAX_GRP - :V_MIN_GRP) *
                            (:V_TARGET_GRPS - :V_MIN_GRP))/
                            (:V_MAX_GRP - :V_MIN_GRP),2);
                    
                    IF (:V_MIN_UNIT_COUNT > :V_MAX_UNIT_COUNT) THEN
                        SET V_SPOT_COUNT := :V_MAX_UNIT_COUNT;
                        SET V_GRP := :V_MAX_GRP;
                    END IF;
                    
                    UPDATE TEMP_REFRESH_REACH_CALCULATION
                    SET SPOT_RESULT = :V_SPOT_COUNT,
                        REACH_PCT_RESULT = :V_REACH_PCT,
                        GRPS_RESULT = ROUND(:V_GRP,1)
                    WHERE CALL_ID = :V_CALL_ID AND PROJECT_ID = :V_PROJECT_ID;
                    
                ELSE
                    SET V_SPOT_COUNT := V_MIN_UNIT_COUNT;
                    
                    UPDATE TEMP_REFRESH_REACH_CALCULATION
                    SET SPOT_RESULT = :V_MIN_UNIT_COUNT,
                        REACH_PCT_RESULT = :V_MIN_REACH_PCT,
                        GRPS_RESULT = ROUND(:V_MIN_GRP,1)
                    WHERE CALL_ID = :V_CALL_ID AND PROJECT_ID = :V_PROJECT_ID;
                END IF;
    
                
                -- Assigning maximum units when even max available units in Reach guide doesnt give desired target
                -- value
                IF (:V_MAX_UNIT_COUNT IS NULL OR :V_MAX_UNIT_COUNT = 0) THEN
                    SELECT UNIT_COUNT, REACH_PCT,GRP
                    INTO :V_MIN_UNIT_COUNT,:V_MIN_REACH_PCT,:V_MIN_GRP
                    FROM TMP_REACH_GUIDE TRG
                    WHERE UNIT_COUNT = :V_AVAILABLE_UNIT_COUNTS
                    AND TRG.TARGET_DEMO_ID = :V_DEMO_ID
                    AND TRG.SEGMENT_ID = :V_SEGMENT_ID
                    AND TRG.FREQUENCY = :V_FREQUENCY
                    AND TRG.CALL_ID = :V_CALL_ID
                    AND TRG.WEEK_COUNT = :V_WEEK_COUNT;
                    
                    SET V_SPOT_COUNT := :V_MIN_UNIT_COUNT;
                    
                    UPDATE TEMP_REFRESH_REACH_CALCULATION
                    SET SPOT_RESULT = :V_MIN_UNIT_COUNT,
                        REACH_PCT_RESULT = :V_MIN_REACH_PCT,
                        GRPS_RESULT = ROUND(:V_MIN_GRP,1)
                    WHERE CALL_ID = :V_CALL_ID
                    AND PROJECT_ID = :V_PROJECT_ID;
                    
                END IF;




            ELSEIF(:V_CALC_BASED_ON = 'S') THEN
                WITH CTE_CNT AS (
                    SELECT MAX(UNIT_COUNT) CNT FROM TMP_REACH_GUIDE RG WHERE RG.UNIT_COUNT <= :V_TOTAL_SPOT 
                    AND RG.TARGET_DEMO_ID = :V_DEMO_ID AND RG.SEGMENT_ID = :V_SEGMENT_ID AND RG.FREQUENCY = :V_FREQUENCY 
                    AND RG.CALL_ID = :V_CALL_ID AND RG.WEEK_COUNT = :V_WEEK_COUNT

                )
                SELECT COALESCE(MAX(UNIT_COUNT),0), COALESCE(MAX(REACH_PCT),0),COALESCE(MAX(GRP),0)
                INTO :V_MIN_UNIT_COUNT,:V_MIN_REACH_PCT,:V_MIN_GRP
                FROM TMP_REACH_GUIDE TRG
                WHERE UNIT_COUNT = (SELECT CNT FROM CTE_CNT)
                    AND TRG.TARGET_DEMO_ID = :V_DEMO_ID
                    AND TRG.SEGMENT_ID = :V_SEGMENT_ID
                    AND TRG.FREQUENCY = :V_FREQUENCY
                    AND TRG.CALL_ID = :V_CALL_ID
                    AND TRG.WEEK_COUNT = :V_WEEK_COUNT;
                    
                WITH CTE_CNT AS (
                    SELECT MIN(UNIT_COUNT) CNT FROM TMP_REACH_GUIDE RG WHERE RG.UNIT_COUNT >= :V_TOTAL_SPOT AND RG.TARGET_DEMO_ID = :V_DEMO_ID
                    AND RG.SEGMENT_ID = :V_SEGMENT_ID AND RG.FREQUENCY = :V_FREQUENCY AND RG.CALL_ID = :V_CALL_ID AND RG.WEEK_COUNT = :V_WEEK_COUNT
                )
                SELECT COALESCE(MAX(UNIT_COUNT),0),COALESCE(MAX(REACH_PCT),0),COALESCE(MAX(GRP),0)
                INTO :V_MAX_UNIT_COUNT,:V_MAX_REACH_PCT,:V_MAX_GRP FROM TMP_REACH_GUIDE TRG
                WHERE UNIT_COUNT = ( SELECT CNT FROM CTE_CNT)
                    AND TRG.TARGET_DEMO_ID = :V_DEMO_ID
                    AND TRG.SEGMENT_ID = :V_SEGMENT_ID
                    AND TRG.FREQUENCY = :V_FREQUENCY
                    AND TRG.CALL_ID = :V_CALL_ID
                    AND TRG.WEEK_COUNT = :V_WEEK_COUNT;
                    
    
                IF (:V_MIN_UNIT_COUNT <> :V_MAX_UNIT_COUNT) THEN    
                    
                    SET V_SPOT_COUNT := :V_TOTAL_SPOT;
                    
                    SET V_REACH_PCT := :V_MIN_REACH_PCT + 
                        ROUND(((:V_MAX_REACH_PCT - :V_MIN_REACH_PCT) * 
                            (:V_TOTAL_SPOT - :V_MIN_UNIT_COUNT))/
                            (:V_MAX_UNIT_COUNT - :V_MIN_UNIT_COUNT),2);
                    
    
                    SET V_GRP := :V_MIN_GRP + 
                        ROUND(((:V_MAX_GRP - :V_MIN_GRP) * (:V_TOTAL_SPOT - :V_MIN_UNIT_COUNT))/
                            (:V_MAX_UNIT_COUNT - :V_MIN_UNIT_COUNT),2);
                            
                    IF (:V_MIN_UNIT_COUNT > :V_MAX_UNIT_COUNT) THEN
                        SET V_SPOT_COUNT := :V_MAX_UNIT_COUNT;
                        SET V_GRP := :V_MAX_GRP;
                    END IF;
                    
                    UPDATE TEMP_REFRESH_REACH_CALCULATION
                    SET SPOT_RESULT = :V_SPOT_COUNT,
                        REACH_PCT_RESULT = :V_REACH_PCT,
                        GRPS_RESULT = ROUND(:V_GRP,1)
                    WHERE CALL_ID = :V_CALL_ID
                    AND PROJECT_ID = :V_PROJECT_ID;
                    
                ELSE
                    SET V_SPOT_COUNT := :V_TOTAL_SPOT;
                    
                    UPDATE TEMP_REFRESH_REACH_CALCULATION
                    SET SPOT_RESULT = :V_SPOT_COUNT,
                        REACH_PCT_RESULT = :V_MIN_REACH_PCT,
                        GRPS_RESULT = ROUND(:V_MIN_GRP,1)
                    WHERE CALL_ID = :V_CALL_ID
                    AND PROJECT_ID = :V_PROJECT_ID;
                END IF;
    
                
                -- Assigning maximum units when even max available units in Reach guide doesnt give desired target
                -- value
                IF (:V_MAX_UNIT_COUNT IS NULL OR :V_MAX_UNIT_COUNT = 0) THEN
                
                    SELECT UNIT_COUNT, REACH_PCT,GRP
                    INTO :V_MIN_UNIT_COUNT,:V_MIN_REACH_PCT,:V_MIN_GRP
                    FROM TMP_REACH_GUIDE TRG
                    WHERE UNIT_COUNT = :V_AVAILABLE_UNIT_COUNTS
                    AND TRG.TARGET_DEMO_ID = :V_DEMO_ID
                    AND TRG.SEGMENT_ID = :V_SEGMENT_ID
                    AND TRG.FREQUENCY = :V_FREQUENCY
                    AND TRG.CALL_ID = :V_CALL_ID
                    AND TRG.WEEK_COUNT = :V_WEEK_COUNT;
                    
                    SET V_SPOT_COUNT := :V_MIN_UNIT_COUNT;
                    
                    UPDATE TEMP_REFRESH_REACH_CALCULATION
                    SET SPOT_RESULT = :V_MIN_UNIT_COUNT,
                        REACH_PCT_RESULT = :V_MIN_REACH_PCT,
                        GRPS_RESULT = ROUND(:V_MIN_GRP,1)
                    WHERE CALL_ID = :V_CALL_ID
                    AND PROJECT_ID = :V_PROJECT_ID;
                    
                END IF;
    
            ELSEIF (:V_CALC_BASED_ON = 'ER') THEN
        
                SET V_PROJ_ORIG_WEEK_COUNT := V_WEEK_COUNT;
                SET V_WEEK_COUNT := 1;

                WITH CTE_CNT AS (
                    SELECT MAX(UNIT_COUNT) CNT FROM TMP_REACH_GUIDE RG WHERE RG.REACH_PCT <= :V_TARGET_REACH_PCT AND RG.TARGET_DEMO_ID = :V_DEMO_ID
                        AND RG.SEGMENT_ID = :V_SEGMENT_ID AND RG.FREQUENCY = :V_FREQUENCY AND RG.CALL_ID = :V_CALL_ID AND RG.WEEK_COUNT = :V_WEEK_COUNT
                )
                SELECT COALESCE(MAX(UNIT_COUNT),0), COALESCE(MAX(REACH_PCT),0),COALESCE(MAX(GRP),0)
                INTO :V_MIN_UNIT_COUNT,:V_MIN_REACH_PCT,:V_MIN_GRP
                FROM TMP_REACH_GUIDE TRG
                WHERE UNIT_COUNT = (SELECT CNT FROM CTE_CNT)
                AND TRG.TARGET_DEMO_ID = :V_DEMO_ID
                    AND TRG.SEGMENT_ID = :V_SEGMENT_ID
                    AND TRG.FREQUENCY = :V_FREQUENCY
                    AND TRG.CALL_ID = :V_CALL_ID
                    AND TRG.WEEK_COUNT = :V_WEEK_COUNT;
    
                WITH CTE_CNT AS (
                    SELECT MIN(UNIT_COUNT) CNT FROM TMP_REACH_GUIDE RG WHERE RG.REACH_PCT >= :V_TARGET_REACH_PCT AND RG.TARGET_DEMO_ID = :V_DEMO_ID
                    AND RG.SEGMENT_ID = :V_SEGMENT_ID AND RG.FREQUENCY = :V_FREQUENCY AND RG.CALL_ID = :V_CALL_ID AND RG.WEEK_COUNT = :V_WEEK_COUNT
                )
                SELECT COALESCE(MAX(UNIT_COUNT),0),COALESCE(MAX(REACH_PCT),0),COALESCE(MAX(GRP),0)
                INTO :V_MAX_UNIT_COUNT,:V_MAX_REACH_PCT,:V_MAX_GRP
                FROM TMP_REACH_GUIDE TRG
                WHERE UNIT_COUNT = (SELECT CNT FROM CTE_CNT)
                AND TRG.TARGET_DEMO_ID = :V_DEMO_ID
                    AND TRG.SEGMENT_ID = :V_SEGMENT_ID
                    AND TRG.FREQUENCY = :V_FREQUENCY
                    AND TRG.CALL_ID = :V_CALL_ID
                    AND TRG.WEEK_COUNT = :V_WEEK_COUNT;
                    
                IF (:V_MIN_UNIT_COUNT <> :V_MAX_UNIT_COUNT) THEN
                
                    SET V_SPOT_COUNT := :V_MIN_UNIT_COUNT + 
                        ROUND(((:V_MAX_UNIT_COUNT - :V_MIN_UNIT_COUNT) *
                            (:V_TARGET_REACH_PCT - :V_MIN_REACH_PCT)) /
                            (:V_MAX_REACH_PCT - :V_MIN_REACH_PCT) ,0);
    
                    SET V_GRP = :V_MIN_GRP + 
                        ROUND(((:V_MAX_GRP - :V_MIN_GRP) *
                            (:V_TARGET_REACH_PCT - :V_MIN_REACH_PCT)) /
                            (:V_MAX_REACH_PCT - :V_MIN_REACH_PCT),0);
                    
                    IF (:V_MIN_UNIT_COUNT > :V_MAX_UNIT_COUNT) THEN
                        SET V_SPOT_COUNT := :V_MAX_UNIT_COUNT;
                        SET V_GRP := :V_MAX_GRP;
                    END IF;
                    
                    UPDATE TEMP_REFRESH_REACH_CALCULATION
                    SET SPOT_RESULT = :V_SPOT_COUNT,
                        REACH_PCT_RESULT = :V_TARGET_REACH_PCT,
                        GRPS_RESULT = ROUND(:V_GRP,1)
                    WHERE CALL_ID = :V_CALL_ID
                    AND PROJECT_ID = :V_PROJECT_ID;
                    
                ELSE
                    SET V_SPOT_COUNT := :V_MIN_UNIT_COUNT;
                    UPDATE TEMP_REFRESH_REACH_CALCULATION
                    SET SPOT_RESULT = :V_MIN_UNIT_COUNT,
                        REACH_PCT_RESULT = :V_TARGET_REACH_PCT,
                        GRPS_RESULT = ROUND(:V_MIN_GRP,1)
                    WHERE CALL_ID = :V_CALL_ID
                    AND PROJECT_ID = :V_PROJECT_ID;
                END IF;
    
            
                -- Assigning maximum units when even max available units in Reach guide doesnt give desired target
                -- value
                IF (:V_MAX_UNIT_COUNT IS NULL OR :V_MAX_UNIT_COUNT = 0) THEN
                    SELECT UNIT_COUNT, REACH_PCT,GRP
                    INTO :V_MIN_UNIT_COUNT,:V_MIN_REACH_PCT,:V_MIN_GRP
                    FROM TMP_REACH_GUIDE TRG
                    WHERE UNIT_COUNT = :V_AVAILABLE_UNIT_COUNTS
                    AND TRG.TARGET_DEMO_ID = :V_DEMO_ID
                    AND TRG.SEGMENT_ID = :V_SEGMENT_ID
                    AND TRG.FREQUENCY = :V_FREQUENCY
                    AND TRG.CALL_ID = :V_CALL_ID
                    AND TRG.WEEK_COUNT = :V_WEEK_COUNT;
                    
                    SET V_SPOT_COUNT := :V_MIN_UNIT_COUNT;
                    
                    UPDATE TEMP_REFRESH_REACH_CALCULATION
                    SET SPOT_RESULT = :V_MIN_UNIT_COUNT,
                        REACH_PCT_RESULT = :V_MIN_REACH_PCT,
                        GRPS_RESULT = ROUND(:V_MIN_GRP,1)
                    WHERE CALL_ID = :V_CALL_ID
                    AND PROJECT_ID = :V_PROJECT_ID;
                    
                END IF;

            ELSEIF (:V_CALC_BASED_ON = 'EG') THEN
        
                SET V_PROJ_ORIG_WEEK_COUNT := :V_WEEK_COUNT;
                SET V_WEEK_COUNT := 1;

                WITH CTE_CNT AS (
                    SELECT MAX(UNIT_COUNT) CNT FROM TMP_REACH_GUIDE RG WHERE RG.GRP <= :V_TARGET_GRPS
                    AND RG.TARGET_DEMO_ID = :V_DEMO_ID AND RG.SEGMENT_ID = :V_SEGMENT_ID AND RG.FREQUENCY = :V_FREQUENCY 
                    AND RG.CALL_ID = :V_CALL_ID AND RG.WEEK_COUNT = :V_WEEK_COUNT
                )
                SELECT COALESCE((UNIT_COUNT),0), COALESCE(MAX(REACH_PCT),0),COALESCE(MAX(GRP),0)
                INTO :V_MIN_UNIT_COUNT,:V_MIN_REACH_PCT,:V_MIN_GRP
                FROM TMP_REACH_GUIDE TRG
                WHERE UNIT_COUNT = ( SELECT CNT FROM CTE_CNT)
                AND TRG.TARGET_DEMO_ID = :V_DEMO_ID
                    AND TRG.SEGMENT_ID = :V_SEGMENT_ID
                    AND TRG.FREQUENCY = :V_FREQUENCY
                    AND TRG.CALL_ID = :V_CALL_ID
                    AND TRG.WEEK_COUNT = :V_WEEK_COUNT;
                    
                WITH CTE_CNT AS (
                    SELECT MIN(UNIT_COUNT) CNT FROM TMP_REACH_GUIDE RG WHERE RG.GRP >= :V_TARGET_GRPS AND RG.TARGET_DEMO_ID = :V_DEMO_ID
                        AND RG.SEGMENT_ID = :V_SEGMENT_ID AND RG.FREQUENCY = :V_FREQUENCY AND RG.CALL_ID = :V_CALL_ID AND RG.WEEK_COUNT = :V_WEEK_COUNT
                )
                SELECT COALESCE(MAX(UNIT_COUNT),0),COALESCE(MAX(REACH_PCT),0),COALESCE(MAX(GRP),0)
                INTO :V_MAX_UNIT_COUNT,:V_MAX_REACH_PCT,:V_MAX_GRP
                FROM TMP_REACH_GUIDE TRG
                WHERE UNIT_COUNT = (SELECT CNT FROM CTE_CNT)
                AND TRG.TARGET_DEMO_ID = :V_DEMO_ID
                    AND TRG.SEGMENT_ID = :V_SEGMENT_ID
                    AND TRG.FREQUENCY = :V_FREQUENCY
                    AND TRG.CALL_ID = :V_CALL_ID
                    AND TRG.WEEK_COUNT = :V_WEEK_COUNT;
                    
                    
                IF (:V_MIN_UNIT_COUNT <> :V_MAX_UNIT_COUNT) THEN 
                
                    SET V_SPOT_COUNT := :V_MIN_UNIT_COUNT + 
                        ROUND(((:V_MAX_UNIT_COUNT - :V_MIN_UNIT_COUNT) *
                            (:V_TARGET_GRPS - :V_MIN_GRP)) /
                            (:V_MAX_GRP - :V_MIN_GRP),0);
                    
                    SET V_REACH_PCT := :V_MIN_REACH_PCT + 
                        ROUND(((:V_MAX_REACH_PCT - :V_MIN_REACH_PCT) * 
                            (:V_TARGET_GRPS - :V_MIN_GRP))/
                            (:V_MAX_GRP - :V_MIN_GRP) ,0);
    
                    SET V_GRP := :V_MIN_GRP + 
                        ROUND(((:V_MAX_GRP - :V_MIN_GRP) *
                            (:V_TARGET_GRPS - :V_MIN_GRP))/
                            (:V_MAX_GRP - :V_MIN_GRP),0);
                    
                    IF (:V_MIN_UNIT_COUNT > :V_MAX_UNIT_COUNT) THEN
                    
                        SET V_SPOT_COUNT := :V_MAX_UNIT_COUNT;
                        SET V_GRP := :V_MAX_GRP;
                    END IF;
                    
                    UPDATE TEMP_REFRESH_REACH_CALCULATION
                    SET SPOT_RESULT = :V_SPOT_COUNT,
                        REACH_PCT_RESULT = :V_REACH_PCT,
                        GRPS_RESULT = ROUND(:V_GRP,1)
                    WHERE CALL_ID = :V_CALL_ID
                    AND PROJECT_ID = :V_PROJECT_ID;
                ELSE
                    SET V_SPOT_COUNT := :V_MIN_UNIT_COUNT;
                    
                    UPDATE TEMP_REFRESH_REACH_CALCULATION
                    SET SPOT_RESULT = :V_MIN_UNIT_COUNT,
                        REACH_PCT_RESULT = :V_MIN_REACH_PCT,
                        GRPS_RESULT = ROUND(:V_MIN_GRP,1)
                    WHERE CALL_ID = :V_CALL_ID
                    AND PROJECT_ID = :V_PROJECT_ID;
                    
                END IF;
    
                
                -- Assigning maximum units when even max available units in Reach guide doesnt give desired target
                -- value
                IF (:V_MAX_UNIT_COUNT IS NULL OR :V_MAX_UNIT_COUNT = 0) THEN
                
                    SELECT UNIT_COUNT, REACH_PCT,GRP
                    INTO :V_MIN_UNIT_COUNT,:V_MIN_REACH_PCT,:V_MIN_GRP
                    FROM TMP_REACH_GUIDE TRG
                    WHERE UNIT_COUNT = :V_AVAILABLE_UNIT_COUNTS
                    AND TRG.TARGET_DEMO_ID = :V_DEMO_ID
                    AND TRG.SEGMENT_ID = :V_SEGMENT_ID
                    AND TRG.FREQUENCY = :V_FREQUENCY
                    AND TRG.CALL_ID = :V_CALL_ID
                    AND TRG.WEEK_COUNT = :V_WEEK_COUNT;
                    
                    SET V_SPOT_COUNT := :V_MIN_UNIT_COUNT;
                    
                    UPDATE TEMP_REFRESH_REACH_CALCULATION
                    SET SPOT_RESULT = :V_MIN_UNIT_COUNT,
                        REACH_PCT_RESULT = :V_MIN_REACH_PCT,
                        GRPS_RESULT = ROUND(:V_MIN_GRP,1)
                    WHERE CALL_ID = :V_CALL_ID
                    AND PROJECT_ID = :V_PROJECT_ID;
                END IF;
            END IF;
        END IF; -- MAIN ELSE 
    END FOR;
    CLOSE MAP_PROJECTS_CURSOR;
    CALL PKG_PROMO_UI_DISTRIBUTE_UNITS(:IN_CALL_ID);

    RES := (SELECT CALL_ID,PROJECT_ID,
        TO_CHAR(WEEK_DATE, 'MM/DD') AS WEEK_DATE,
        CONCAT('Week',ROUND(DATEDIFF(DAY,:V_QTR_START_DATE,WEEK_DATE)/7,0)+1) WEEK_NAME,
        UNIT_COUNT FROM TMP_PROJECT_DISTRIBUTION WHERE CALL_ID = :IN_CALL_ID);

        CREATE OR REPLACE TABLE TMP_CHECK_REFRESH_VALUES AS 
        SELECT CALL_ID,PROJECT_ID,
        TO_CHAR(WEEK_DATE, 'MM/DD') AS WEEK_DATE,
        CONCAT('Week',ROUND(DATEDIFF(DAY,:V_QTR_START_DATE,WEEK_DATE)/7,0)+1) WEEK_NAME,
        UNIT_COUNT FROM TMP_PROJECT_DISTRIBUTION WHERE CALL_ID = :IN_CALL_ID;
    
RETURN TABLE(RES);
END;

GRANT USAGE ON PROCEDURE PKG_PROMO_UI_REFRESH_MAP_VALUES(INT) TO ROLE RA_PROMO_DEV_QA_UAT_ACCESS;
GRANT USAGE ON PROCEDURE PKG_PROMO_UI_REFRESH_MAP_VALUES(INT) TO ROLE RA_PROMO_PROD_ACCESS;
