SELECT DISTINCT t.MEDIA_OUTLET_ID,t.QTR_ID,t.DAYPART_ID,NULL AS TOTAL_CAPACITY,t.CAPACITY AS CAPACITY_PER_HOUR,t.START_DATE,
    t.START_DATE AS END_DATE,CURRENT_TIMESTAMP() AS MODIFY_DT,IN_USERID AS MODIFY_USER,t.DAY_OF_WEEK,'D' AS PERIOD_TYPE FROM 
    (
        SELECT rcp.start_date, WEEKDAY(rcp.start_date) + 1 AS DAY_OF_WEEK, t.* FROM REF_CAL_PERIOD rcp JOIN 
        (
            SELECT PARSE_JSON(val):DAY_OF_WEEK::STRING AS DAY_OF_WEEK, PARSE_JSON(val):DAYPART_ID::STRING AS DAYPART_ID, PARSE_JSON(val):MEDIA_OUTLET_ID::STRING AS MEDIA_OUTLET_ID,
            PARSE_JSON(val):CAPACITY::NUMBER AS CAPACITY,:IN_QTR_ID AS QTR_ID,week_key,START_DATE AS START_DATE1,END_DATE AS END_DATE1 FROM 
            (
                SELECT PARSE_JSON(IN_CAP):[counter] AS val, week_key, START_DATE, END_DATE FROM 
                (
                    SELECT PARSE_JSON(:IN_CAPACITY):[week_key] AS IN_CAP, week_key, START_DATE, END_DATE FROM 
                    (
                        SELECT week_key, START_DATE, END_DATE FROM 
                        (
                            SELECT PARSE_JSON(:IN_WEEK_KEYS):[counter] AS week_key
                            FROM REF_JSON_COUNTER
                            WHERE PARSE_JSON(:IN_WEEK_KEYS):[counter] IS NOT NULL
                        ) AS WK JOIN 
                        (
                            SELECT ROW_NUMBER() OVER (ORDER BY START_DATE) AS WEEK_NUMBER, START_DATE, END_DATE FROM REF_CAL_PERIOD WHERE FISCAL_QTR = :IN_QTR_ID 
                                AND PERIOD_TYPE_CD = 'W' ORDER BY START_DATE
                        ) T ON WK.week_key = T.WEEK_NUMBER
                    ) WK
                ) AS T JOIN REF_JSON_COUNTER ON COUNTER <= 800 WHERE PARSE_JSON(IN_CAP):[counter] IS NOT NULL
            ) AS t
        ) AS t
        ON t.MEDIA_OUTLET_ID = :IN_MEDIA_OUTLET_ID
        AND rcp.FISCAL_QTR = :IN_QTR_ID
        AND rcp.START_DATE BETWEEN t.START_DATE1 AND t.END_DATE1
        AND WEEKDAY(rcp.START_DATE) + 1 = t.DAY_OF_WEEK
        AND rcp.FISCAL_QTR = :IN_QTR_ID
        AND rcp.START_DATE > CURRENT_DATE()
        WHERE rcp.FISCAL_QTR = :IN_QTR_ID
        AND rcp.PERIOD_TYPE_CD = 'D'
    ) AS TT