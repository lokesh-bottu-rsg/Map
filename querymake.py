def convert(dic,query):
    for key, value in dic.items():
        try:
            replacement = str(int(value))
        except ValueError:
            replacement = f"'{value}'"
        query = query.replace(':' + key, replacement)
    file_path = 'converted_query.sql'
    with open(file_path, 'w') as file:
        file.write(query)
    print(f"Query has been written to {file_path}.")
dic = {'IN_MEDIA_OUTLET_ID': '96','IN_QTR': '1q23','IN_OUTPUT_TYPE': '1','IN_PLAN_ID': '340'}
# [params: \'IN_MEDIA_OUTLET_ID: 96\',\'IN_QTR: 1Q23\',\'IN_PLAN_ID: 340\',\'IN_OUTPUT_TYPE: 1\']

query = '''WITH CTE_PLAN_STATUS AS(
											SELECT PP.PERIOD_ID AS V_PERIOD_ID, IFNULL(PP.PLAN_STATUS_ID,0) AS V_PLAN_STATUS_ID,PP.PLAN_TYPE AS  V_PLAN_TYPE 
											FROM PR_PLAN PP 
											WHERE PP.PLAN_ID=:IN_PLAN_ID AND PP.MEDIA_OUTLET_ID=:IN_MEDIA_OUTLET_ID LIMIT 1
							)
							,CTE_PLAN_TYPE AS(
											select case when CP.V_PLAN_TYPE=1 then 1 when CP.V_PLAN_TYPE = 2 then 3 end AS V_MODEL_TYPE FROM CTE_PLAN_STATUS CP
							)
							,CTE_DEMO AS(
											SELECT ROD.DEMO_ID AS V_NETWORK_TARGET_DEMO_ID,RD.NAME AS V_NETWORK_TARGET_DEMO_NAME 
											FROM REF_MEDIA_OUTLET_DEMO ROD JOIN REF_DEMO RD on ROD.DEMO_ID=RD.DEMO_ID
											where ROD.MEDIA_OUTLET_ID=:IN_MEDIA_OUTLET_ID and ACTIVE_YN='Y' and DEFAULT_YN='Y'
							)  
							,CTE_VERSION_ID AS(
											SELECT MAX(PLAN_VERSION_ID) AS V_PLAN_VERSION_ID 
											FROM PR_PLAN_VERSION PR
											WHERE PR.PLAN_ID=:IN_PLAN_ID
							)
							,CTE_PAST_PLANID AS(
											SELECT PP.PLAN_ID AS V_PAST_PLAN_ID FROM PR_PLAN PP
											JOIN CTE_PLAN_STATUS CS ON 1=1 
											WHERE PERIOD_ID=(
											SELECT PP.PERIOD_ID-1 FROM PR_PLAN PP
											JOIN CTE_PLAN_STATUS CS ON 1=1
											WHERE PP.PLAN_ID=:IN_PLAN_ID AND PP.MEDIA_OUTLET_ID=:IN_MEDIA_OUTLET_ID AND PP.PLAN_TYPE=CS.V_PLAN_TYPE LIMIT 1) 
											AND PP.MEDIA_OUTLET_ID=:IN_MEDIA_OUTLET_ID AND ACTIVE_YN='Y' AND PP.PLAN_TYPE=CS.V_PLAN_TYPE
							)
							,CTE_PAST_VERSIONID AS(
											SELECT IFNULL(MAX(PLAN_VERSION_ID),0) AS V_PAST_PLAN_VERSION_ID FROM MODEL_EXEC ME
											JOIN CTE_PAST_PLANID CP ON 1=1
											WHERE ME.PLAN_ID=CP.V_PAST_PLAN_ID AND EXEC_STATUS_CD='C'
							)
							,CTE_QTR_DATE AS(
											SELECT START_DATE AS V_QTR_START_DATE, END_DATE AS V_QTR_END_DATE 
											FROM REF_CAL_PERIOD RC
											WHERE RC.NAME =:IN_QTR AND PERIOD_TYPE_CD='Q'
							)
							,CTE_WEEK_DATE AS(
											SELECT START_DATE AS V_WEEK_START_DATE, END_DATE AS V_WEEK_END_DATE,ROW_NUMBER() OVER (ORDER BY END_DATE) AS WEEK_COUNT
											FROM REF_CAL_PERIOD WHERE FISCAL_QTR IN (
											SELECT FISCAL_QTR FROM REF_CAL_PERIOD RF JOIN CTE_PLAN_STATUS CS ON 1=1
											WHERE PERIOD_ID IN (CS.V_PERIOD_ID, CS.V_PERIOD_ID+1)) AND PERIOD_TYPE_CD='W' ORDER BY END_DATE ASC LIMIT 26
							)
							,MAX_PLAN_VERSION AS(
											select M.PLAN_ID, MAX(PLAN_VERSION_ID) PLAN_VERSION_ID from MODEL_EXEC M 
											JOIN CTE_PLAN_TYPE CT ON 1=1
											where M.MODEL_TYPE_ID=CT.V_MODEL_TYPE
											and M.MEDIA_OUTLET_ID=:IN_MEDIA_OUTLET_ID and M.EXEC_STATUS_CD='C'
											group by M.PLAN_ID
							)
							,CTE_CAMPAIGN_ID AS(
											SELECT DISTINCT PP.CAMPAIGN_GOAL_ID
											FROM PR_PLAN_SCHEDULE PPS 
											JOIN CTE_PLAN_STATUS CS ON 1=1
											JOIN CTE_QTR_DATE CQ ON 1=1
											INNER JOIN PR_PLAN_PROJECT PP ON PPS.CAMPAIGN_GOAL_ID=PP.CAMPAIGN_GOAL_ID
											JOIN PR_PLAN P on PP.PLAN_ID=P.PLAN_ID and P.MEDIA_OUTLET_ID=:IN_MEDIA_OUTLET_ID 
											AND PPS.START_DATE BETWEEN  CQ.V_QTR_START_DATE  AND  CQ.V_QTR_END_DATE  
											AND P.PLAN_TYPE=CS.V_PLAN_TYPE
							)
							,CTE_EPISODE_CAMPAIGN AS(
											SELECT DISTINCT PP.CAMPAIGN_GOAL_ID
											FROM PR_PLAN_SCHEDULE_EPISODE PPS 
											JOIN CTE_PLAN_STATUS CS ON 1=1
											JOIN CTE_QTR_DATE CQ ON 1=1
											INNER JOIN PR_PLAN_PROJECT PP ON PPS.CAMPAIGN_GOAL_ID=PP.CAMPAIGN_GOAL_ID
											JOIN PR_PLAN P on PP.PLAN_ID=P.PLAN_ID and P.MEDIA_OUTLET_ID=:IN_MEDIA_OUTLET_ID 
											AND PPS.START_DATE BETWEEN  CQ.V_QTR_START_DATE  AND  CQ.V_QTR_END_DATE  
											AND P.PLAN_TYPE=CS.V_PLAN_TYPE
							)
							,EXCLUDED_PROJECTS AS (
											SELECT DISTINCT PROJECT_ID from PR_PLAN_SCHEDULE PPS 
											JOIN CTE_QTR_DATE CQ ON 1=1
											JOIN CTE_PLAN_STATUS CS ON 1=1
											JOIN PR_PLAN P on PPS.PLAN_ID=P.PLAN_ID 
											AND P.PLAN_TYPE=CS.V_PLAN_TYPE and PPS.MEDIA_OUTLET_ID=P.MEDIA_OUTLET_ID
											where P.MEDIA_OUTLET_ID=:IN_MEDIA_OUTLET_ID AND START_DATE BETWEEN CQ.V_QTR_START_DATE AND CQ.V_QTR_END_DATE
							)
							,CAMPAIGN_GOALS AS (
											SELECT DISTINCT PP.CAMPAIGN_GOAL_ID
											FROM PR_PLAN_SCHEDULE_EPISODE PPS
											INNER JOIN PR_PLAN_PROJECT_EPISODE PP 
												ON PPS.CAMPAIGN_GOAL_ID = PP.CAMPAIGN_GOAL_ID
											JOIN CTE_PLAN_STATUS CS ON 1=1
											JOIN CTE_QTR_DATE CQ ON 1=1
											JOIN PR_PLAN P 
												ON PP.PLAN_ID = P.PLAN_ID 
												AND P.MEDIA_OUTLET_ID = :IN_MEDIA_OUTLET_ID 
												AND PPS.START_DATE BETWEEN CQ.V_QTR_START_DATE AND CQ.V_QTR_END_DATE  
												AND P.PLAN_TYPE = CS.V_PLAN_TYPE
							)
							,PROJECT_EXISTS AS (
											SELECT 1 
											FROM PR_PLAN_SCHEDULE_EPISODE PPS1 
											JOIN PR_PLAN_PROJECT PP1 
												ON PP1.PLAN_ID = PPS1.PLAN_ID 
												AND PPS1.PLAN_VERSION_ID = 0 
												AND PP1.PROJECT_ID = PPS1.PROJECT_ID
											JOIN CTE_QTR_DATE CQ1 ON 1=1
											WHERE DATE(PPS1.START_DATE) BETWEEN CQ1.V_QTR_START_DATE AND CQ1.V_QTR_END_DATE
							)
							,CTE_PLAN_DATA AS(
											SELECT PP.PROJ_PRIORITY,PP.TARGET_REACH_PCT TARGET_REACH_PCT1,PP.TARGET_GRP TARGET_GRP1, PP.STATUS_CD,PP.PROJ_REACH_PERCENT,
											PP.EPISODE_PRIORITY ,PP.NO_OF_EPISODE ,PP.PROJ_GRP, PP.STATUS_CD AS PROJECT_STATUS_CD, RPS.NAME as STATUS_NAME,
											PP.DISPLAY_SEQ, PP.PROJECT_ID, PP.GUIDE_REACH_PCT, PP.GUIDE_GRP,PP.GUIDE_SPOTS, PP.CAMPAIGN_GOAL_ID,
											IFNULL(PP.IS_DIRTY_PROJECT,'N') IS_DIRTY_PROJECT, 'N' as PAST_QTR_PROJECT_FLAG, PP.USER_COMMENT,PP.CLUSTER_ID, 
											RC.CLUSTER_NAME, PP.SEGMENT, PP.SEGMENT_TYPE, PP.SEGMENT_AIRBYTE, PP.FREQUENCY, PP.ON_MAP_DEMO, PP.ON_MAP_START_DATE, PP.ON_MAP_END_DATE,
											PP.PLAN_ID,PP.PHASE_TYPE,PP.ON_MAP_PREMIER_DATE, PP.ON_MAP_START_DATE_TS, PP.ON_MAP_END_DATE_TS
											FROM PR_PLAN_PROJECT PP
											JOIN CTE_PLAN_STATUS CS ON 1=1
											JOIN CTE_VERSION_ID CV ON 1=1
											LEFT JOIN REF_PLAN_STATUS RPS ON CASE WHEN PP.OPTIMIZE_YN='Y' AND CS.V_PLAN_STATUS_ID=2 THEN 2 ELSE PP.STATUS_CD END=RPS.PLAN_STATUS_ID
											LEFT JOIN REF_CLUSTER RC on PP.CLUSTER_ID=RC.CLUSTER_ID
											WHERE PP.PLAN_ID= :IN_PLAN_ID AND PP.PLAN_VERSION_ID= CV.V_PLAN_VERSION_ID
											
									UNION
									
											SELECT PP.PROJ_PRIORITY,PP.TARGET_REACH_PCT TARGET_REACH_PCT1,PP.TARGET_GRP TARGET_GRP1, PP.STATUS_CD,PP.PROJ_REACH_PERCENT,
												PP.EPISODE_PRIORITY ,PP.NO_OF_EPISODE ,PP.PROJ_GRP, PP.STATUS_CD AS PROJECT_STATUS_CD, RPS.NAME as STATUS_NAME,
												PP.DISPLAY_SEQ, PP.PROJECT_ID, PP.GUIDE_REACH_PCT, PP.GUIDE_GRP, 
												PP.GUIDE_SPOTS, PP.CAMPAIGN_GOAL_ID, 
												IFNULL(PP.IS_DIRTY_PROJECT,'N') IS_DIRTY_PROJECT , 'Y' as PAST_QTR_PROJECT_FLAG , PP.USER_COMMENT, PP.CLUSTER_ID, 
												RC.CLUSTER_NAME, PP.SEGMENT, PP.SEGMENT_TYPE, PP.SEGMENT_AIRBYTE, PP.FREQUENCY, PP.ON_MAP_DEMO, PP.ON_MAP_START_DATE, PP.ON_MAP_END_DATE,
												PP.PLAN_ID,PP.PHASE_TYPE,PP.ON_MAP_PREMIER_DATE, PP.ON_MAP_START_DATE_TS, PP.ON_MAP_END_DATE_TS
											FROM PR_PLAN_PROJECT PP
											JOIN CTE_PLAN_STATUS CS ON 1=1
											JOIN CTE_QTR_DATE CQ ON 1=1
											JOIN MAX_PLAN_VERSION M ON M.PLAN_ID=PP.PLAN_ID AND M.PLAN_VERSION_ID=PP.PLAN_VERSION_ID
											JOIN PR_PLAN P on PP.PLAN_ID=P.PLAN_ID and P.PLAN_TYPE=CS.V_PLAN_TYPE and P.MEDIA_OUTLET_ID=:IN_MEDIA_OUTLET_ID 
											and P.PLAN_ID<>:IN_PLAN_ID
											AND PP.CAMPAIGN_GOAL_ID IN (SELECT CAMPAIGN_GOAL_ID FROM CTE_CAMPAIGN_ID) 
											JOIN PR_PLAN_SCHEDULE PPS on PP.PLAN_ID=PPS.PLAN_ID and PPS.PLAN_VERSION_ID=0 AND PP.PROJECT_ID=PPS.PROJECT_ID
											AND date(PPS.START_DATE)between CQ.V_QTR_START_DATE and CQ.V_QTR_END_DATE 
											LEFT JOIN REF_PLAN_STATUS RPS ON PP.STATUS_CD=RPS.PLAN_STATUS_ID
											LEFT JOIN REF_CLUSTER RC on PP.CLUSTER_ID=RC.CLUSTER_ID
										
											
									UNION
									
											SELECT  
											PP.PROJ_PRIORITY,
											PP.TARGET_REACH_PCT AS TARGET_REACH_PCT1,
											PP.TARGET_GRP AS TARGET_GRP1,
											PP.STATUS_CD,
											PP.PROJ_REACH_PERCENT,
											PP.EPISODE_PRIORITY,
											PP.NO_OF_EPISODE,
											PP.PROJ_GRP,
											PP.STATUS_CD AS PROJECT_STATUS_CD,
											RPS.NAME AS STATUS_NAME,
											PP.DISPLAY_SEQ,
											PP.PROJECT_ID,
											PP.GUIDE_REACH_PCT,
											PP.GUIDE_GRP,
											PP.GUIDE_SPOTS,
											PP.CAMPAIGN_GOAL_ID,
											IFNULL(PP.IS_DIRTY_PROJECT, 'N') AS IS_DIRTY_PROJECT,
											'Y' AS PAST_QTR_PROJECT_FLAG,
											PP.USER_COMMENT,
											PP.CLUSTER_ID,
											RC.CLUSTER_NAME,
											PP.SEGMENT,
											PP.SEGMENT_TYPE,
											PP.SEGMENT_AIRBYTE,
											PP.FREQUENCY,
											PP.ON_MAP_DEMO,
											PP.ON_MAP_START_DATE,
											PP.ON_MAP_END_DATE,
											PP.PLAN_ID,
											PP.PHASE_TYPE,
											PP.ON_MAP_PREMIER_DATE,
											PP.ON_MAP_START_DATE_TS,
											PP.ON_MAP_END_DATE_TS                
											FROM PR_PLAN_PROJECT PP
											JOIN CTE_PLAN_STATUS CS ON 1=1
											JOIN CTE_QTR_DATE CQ ON 1=1
											JOIN PR_PLAN P 
											ON PP.PLAN_ID = P.PLAN_ID 
											AND P.PLAN_TYPE = CS.V_PLAN_TYPE 
											AND P.MEDIA_OUTLET_ID = :IN_MEDIA_OUTLET_ID 
											AND P.PLAN_ID <> :IN_PLAN_ID
											JOIN MAX_PLAN_VERSION M
											ON PP.PLAN_VERSION_ID = M.PLAN_VERSION_ID
											AND M.PLAN_ID = PP.PLAN_ID
											AND PP.PROJECT_ID NOT in ( SELECT PROJECT_ID FROM EXCLUDED_PROJECTS)
											INNER JOIN PR_PLAN_PROJECT_EPISODE PPE ON PP.PROJECT_ID = PPE.PROJECT_ID AND PP.PLAN_ID = PPE.PLAN_ID
											AND PP.PLAN_VERSION_ID = PPE.PLAN_VERSION_ID AND PP.PROJECT_ID = PPE.PROJECT_ID
											AND PPE.CAMPAIGN_GOAL_ID IN (SELECT CAMPAIGN_GOAL_ID FROM CAMPAIGN_GOALS)
											AND EXISTS (
											SELECT 1 
											FROM PROJECT_EXISTS
											)
											LEFT JOIN REF_PLAN_STATUS RPS 
											ON PP.STATUS_CD = RPS.PLAN_STATUS_ID
											LEFT JOIN REF_CLUSTER RC 
											ON PP.CLUSTER_ID = RC.CLUSTER_ID
											GROUP BY ALL
											ORDER BY PAST_QTR_PROJECT_FLAG, DISPLAY_SEQ

							)
							,CTE_PROJECT AS (
										SELECT DISTINCT PLAN_ID,PROJECT_ID FROM CTE_PLAN_DATA
							)
							,CTE_COLOUR_CHECK AS(
										SELECT CP.PROJECT_ID,
										CASE WHEN PPEH.EPISODE_END_DT < CURRENT_TIMESTAMP() THEN 'N' 
										ELSE CASE 
										WHEN COALESCE(PPEH.MAP_COLOUR_UPDATED_TS, '1990-01-01') > PPP.UPDATE_TS THEN 'Y' 
										ELSE 'N' END END AS FLAG
										FROM PR_PLAN_PROJECT PPP 
										JOIN CTE_PROJECT CP ON 1=1
										LEFT JOIN PR_PROJECT_EPISODE_HEADER PPEH ON PPP.PROJECT_ID = PPEH.PROJECT_ID 
										WHERE PPP.PROJECT_ID = CP.PROJECT_ID 
										AND PPP.PLAN_ID = CP.PLAN_ID
										AND PPP.PLAN_VERSION_ID = (
										SELECT MAX(PLAN_VERSION_ID) 
										FROM PR_PLAN_VERSION 
										WHERE PLAN_ID = PPP.PLAN_ID)
							)
							,CTE_PLAN_ENDDATE AS (
										SELECT CP.PROJECT_ID,MAX(ME.EXEC_END_DT) AS V_END_DT FROM PR_PLAN_PROJECT PPP INNER JOIN MODEL_EXEC ME ON PPP.PLAN_ID=ME.PLAN_ID 
										AND PPP.PLAN_VERSION_ID=ME.PLAN_VERSION_ID
										JOIN CTE_PROJECT CP ON 1=1
										WHERE PPP.PLAN_ID=CP.PLAN_ID AND PPP.PROJECT_ID=CP.PROJECT_ID AND PPP.OPTIMIZE_YN='Y' AND EXEC_STATUS_CD = 'C'
										GROUP BY CP.PROJECT_ID
							)
							,CTE_EPISODE_ENDDATE AS (
										SELECT CP.PROJECT_ID,MAX(ME.EXEC_END_DT) AS V_EPISODIC_END_DT FROM PR_PLAN_PROJECT_EPISODE PPP 
										INNER JOIN MODEL_EXEC ME ON PPP.PLAN_ID=ME.PLAN_ID AND PPP.PLAN_VERSION_ID=ME.PLAN_VERSION_ID
										JOIN CTE_PROJECT CP ON 1=1
										WHERE PPP.PLAN_ID=CP.PLAN_ID AND PPP.PROJECT_ID=CP.PROJECT_ID AND PPP.OPTIMIZE_YN='Y' AND EXEC_STATUS_CD = 'C'
										GROUP BY CP.PROJECT_ID
							)
							,CTE_VERSION AS (
										SELECT CP.PROJECT_ID,CONCAT('v', MAX(PPP.PLAN_VERSION_ID)+1,' ', 
										TO_CHAR(CONVERT_TIMEZONE('UTC', 'US/Eastern', MAX(ME.EXEC_END_DT)), 'MM/DD/YYYY HH:MI AM')) AS  
										V_GET_VERSION 
										FROM PR_PLAN_PROJECT PPP INNER JOIN MODEL_EXEC ME ON PPP.PLAN_ID=ME.PLAN_ID AND PPP.PLAN_VERSION_ID=ME.PLAN_VERSION_ID
										LEFT JOIN CTE_PROJECT CP ON PPP.PROJECT_ID=CP.PROJECT_ID
										WHERE PPP.PLAN_ID=CP.PLAN_ID AND PPP.PROJECT_ID=CP.PROJECT_ID AND PPP.OPTIMIZE_YN='Y' AND EXEC_STATUS_CD = 'C'
										GROUP BY CP.PROJECT_ID
							)
							,CTE_VERSION_ID_DATE AS (
										SELECT CP.PROJECT_ID,CONCAT('v', MAX(PPP.PLAN_VERSION_ID)+1,' ', TO_CHAR(CONVERT_TIMEZONE('UTC', 'US/Eastern', MAX(ME.EXEC_END_DT)), 'MM/DD/YYYY HH:MI AM')) AS                     V_GET_VERSION 
										FROM PR_PLAN_PROJECT_EPISODE PPP 
										JOIN CTE_VERSION CV ON 1=1
										JOIN CTE_PLAN_ENDDATE CD ON 1=1
										JOIN CTE_EPISODE_ENDDATE CE ON 1=1
										JOIN CTE_PROJECT CP ON 1=1
										JOIN MODEL_EXEC ME ON PPP.PLAN_ID=ME.PLAN_ID AND PPP.PLAN_VERSION_ID=ME.PLAN_VERSION_ID
										WHERE PPP.PLAN_ID=CP.PLAN_ID AND PPP.PROJECT_ID=CP.PROJECT_ID AND PPP.OPTIMIZE_YN='Y' AND EXEC_STATUS_CD = 'C'
										AND CV.V_GET_VERSION IS NULL OR CV.V_GET_VERSION = '' 
										GROUP BY CP.PROJECT_ID
							)
							,CTE_UPDATED_VERSION AS(
										SELECT * FROM CTE_VERSION
										UNION
										SELECT * FROM CTE_VERSION_ID_DATE
							)
							,CTE_FINAL AS(
										SELECT CD.*, V_GET_VERSION,CCC.FLAG FROM CTE_PLAN_DATA CD 
										LEFT JOIN CTE_UPDATED_VERSION CV ON CD.PROJECT_ID =CV.PROJECT_ID
										LEFT JOIN CTE_COLOUR_CHECK CCC ON CCC.PROJECT_ID=CD.PROJECT_ID
							)
							,CTE_VERSION_PLAN AS (
										SELECT ME.PLAN_ID, ME.PLAN_VERSION_ID, MAX(EXEC_END_DT) AS EXEC_END_DT FROM MODEL_EXEC ME 
										JOIN CTE_VERSION_ID MV ON 1=1
										WHERE ME.PLAN_ID=:IN_PLAN_ID AND ME.PLAN_VERSION_ID=MV.V_PLAN_VERSION_ID
										GROUP BY ME.PLAN_ID,ME.PLAN_VERSION_ID
							)
							,CTE_PLAN_DATA1 AS (
												SELECT DISTINCT PP.DISPLAY_SEQ,PP.PROJECT_ID,PL.PLAN_ID,PP1.NAME AS PROJECT_NAME, (PP.PHASE_TYPE) AS "phTypeID",
													(RPPT.DESCRIPTION) AS "ph_description", PP1.IS_EPISODIC, PP1.MEDIA_OUTLET_ID,
													RMO.SHORT_NAME AS NETWORK_NAME, PP.PROJECT_STATUS_CD, PP.STATUS_NAME AS PROJECT_STATUS,
													(PP.PROJ_REACH_PERCENT) AS PROJ_REACH_PERCENT, (PROJ_GRP) AS PROJ_GRP, 
													PP1.START_DATE AS PROJECT_START_DATE,
													PP1.END_DATE AS PROJECT_END_DATE,(PP.ON_MAP_START_DATE) AS START_DATE,
													(PP.ON_MAP_END_DATE) AS END_DATE,(PP.ON_MAP_START_DATE_TS) AS ON_MAP_START_DATE_TS,
													(PP.ON_MAP_END_DATE_TS) AS ON_MAP_END_DATE_TS,
													PP1.TARGET_FREQUENCY AS PROJECT_FREQ, RS1.SEGMENT_NAME AS PROJECT_SEGMENT,
													RS1.SEGMENT_ID AS PROJECT_SEGMENT_ID, RD1.NAME AS PROJECT_DEMO,
													RD1.DEMO_ID AS PROJECT_DEMO_ID, PP1.PREMIERE_DATE,
													CEIL(CASE WHEN DATEDIFF(DAY,(PP.ON_MAP_START_DATE),(PP.ON_MAP_END_DATE)) = 0 THEN 1 
																ELSE DATEDIFF(DAY,(PP.ON_MAP_START_DATE),(PP.ON_MAP_END_DATE)) END / 7) AS NO_OF_WEEKS,
													CEIL(CASE WHEN DATEDIFF(DAY,PP1.START_DATE,PP1.END_DATE) = 0 THEN 1 
																ELSE DATEDIFF(DAY,PP1.START_DATE,PP1.END_DATE) END / 7) AS UPDATED_NO_OF_WKS,
													PP1.CAMPAIGN_TYPE_ID, RCT.NAME AS CAMPAIGN_TYPE, PP.ON_MAP_DEMO AS DEMO_ID,
													RD.NAME AS DEMO, PP.SEGMENT AS SEGMENT_ID,
													(SELECT LISTAGG(COMPONENT_SEGMENT_ID, ', ') FROM REF_SEGMENT_COMPONENT WHERE SEGMENT_ID = PCG.SEGMENT) AS SEGMENT_SELECTED,
													(PP.SEGMENT_TYPE) AS SEGMENT_TYPE, (PP.SEGMENT_AIRBYTE) AS SEGMENT_AIRBYTE, RS.SEGMENT_NAME AS SEGMENT_NAME,
													PL.NAME AS PLAN_NAME, PL_MSG.NAME AS MSG_PLAN_NAME, PL.PLAN_STATUS_ID,
													RPS.NAME AS PLAN_STATUS, PL.COMMENTS AS STATUS_COMMENT, 
													PP.FREQUENCY AS TARGET_FREQUENCY, PP.TARGET_REACH_PCT1 AS TARGET_REACH,
													PP.TARGET_GRP1 AS TARGET_GRP, (PP.EPISODE_PRIORITY) AS EPISODIC_PRIORITY,
													(PP.NO_OF_EPISODE) AS NO_OF_EPISODE,
														CASE WHEN PP.PAST_QTR_PROJECT_FLAG='Y' THEN 'N' ELSE IFNULL(PP.FLAG,'N') END AS COLOUR_CHECK_FLAG,
													PPEH.MAP_COLOUR_UPDATED_TS, PP.PROJ_PRIORITY AS PRIORITY,
													PP1.SCATTER_IMPS, PP1.SUBEVENT_ID, RES.SUBEVENT_NAME,
													(PP.GUIDE_REACH_PCT) AS GUIDE_REACH_PCT, (PP.GUIDE_GRP) AS GUIDE_GRP, (PP.GUIDE_SPOTS) AS GUIDE_SPOTS,
													IFNULL(ME.EXEC_END_DT, PL.UPDATE_DATE) AS EXEC_END_DT,
													PP.CAMPAIGN_GOAL_ID, PP.IS_DIRTY_PROJECT AS IS_DIRTY_PROJECT,
													PP.PAST_QTR_PROJECT_FLAG AS PAST_PROJECT, PP1.USER_COMMENT,
													PP.V_GET_VERSION AS LAST_OPT_VERSION_TS,
													CD.V_NETWORK_TARGET_DEMO_ID AS NETWORK_TARGET_DEMO_ID, 
													CD.V_NETWORK_TARGET_DEMO_NAME AS NETWORK_TARGET_DEMO_NAME,
													CASE WHEN PP1.MEDIA_OUTLET_ID = :IN_MEDIA_OUTLET_ID THEN 1 ELSE 99 END AS NETWORK_DISPLAY_SEQ,
													PP.CLUSTER_ID, PP.CLUSTER_NAME, (PP.PROJ_REACH_PERCENT) AS PROJ_PLANNED_REACH, 
													(PP.PROJ_GRP) AS PROJ_PLANNED_GRP, (PP.ON_MAP_PREMIER_DATE) AS ON_MAP_PREMIER_DATE
															FROM CTE_FINAL PP 
															JOIN CTE_DEMO CD ON 1=1  
															JOIN CTE_PLAN_STATUS CPS ON 1=1
														LEFT JOIN PR_PROJECT PP1 ON PP.PROJECT_ID=PP1.PROJECT_ID
														LEFT JOIN REF_PROJECT_PLAN_TYPE RPPT ON PP.PHASE_TYPE = RPPT.TYPE_ID
														LEFT JOIN PR_PROJECT_EPISODE_HEADER PPEH ON PP.PROJECT_ID = PPEH.PROJECT_ID
														LEFT JOIN PR_CAMPAIGN_GOAL PCG ON PP.CAMPAIGN_GOAL_ID = PCG.CAMPAIGN_GOAL_ID
														LEFT JOIN REF_DEMO RD ON RD.DEMO_ID = PP.ON_MAP_DEMO
														LEFT JOIN REF_DEMO RD1 ON RD1.DEMO_ID = PP1.TARGET_DEMO_ID
														LEFT JOIN REF_SEGMENT RS ON RS.SEGMENT_ID = PP.SEGMENT
														LEFT JOIN REF_SEGMENT RS1 ON RS1.SEGMENT_ID = PP1.TARGET_SEGMENT
														LEFT JOIN REF_2E_EVENT_SUBTYPE_PROJECT RES ON PP1.SUBEVENT_ID=RES.SUBEVENT_ID
														LEFT JOIN REF_MEDIA_OUTLET RMO ON PP1.MEDIA_OUTLET_ID=RMO.MEDIA_OUTLET_ID
														LEFT JOIN PR_PLAN PL ON PL.PLAN_ID=:IN_PLAN_ID
														LEFT JOIN PR_PLAN PL_MSG ON PL_MSG.PLAN_ID=PP.PLAN_ID
														LEFT JOIN REF_PLAN_STATUS RPS ON PL.PLAN_STATUS_ID=RPS.PLAN_STATUS_ID
														LEFT JOIN REF_CAMPAIGN_TYPE RCT ON RCT.CAMPAIGN_TYPE_ID=PP1.CAMPAIGN_TYPE_ID
														LEFT JOIN CTE_VERSION_PLAN ME ON ME.PLAN_ID=PL.PLAN_ID AND ME.PLAN_VERSION_ID=(SELECT V_PLAN_VERSION_ID FROM CTE_VERSION_ID)
														WHERE CPS.V_PLAN_STATUS_ID=3

													UNION
													
												SELECT DISTINCT PP.DISPLAY_SEQ, PP.PROJECT_ID,  PL.PLAN_ID,PP1.NAME AS PROJECT_NAME, (PP.PHASE_TYPE) AS "phTypeID",
													(RPPT.DESCRIPTION) AS "ph_description", PP1.IS_EPISODIC, PP1.MEDIA_OUTLET_ID,
													RMO.SHORT_NAME AS NETWORK_NAME, PP.PROJECT_STATUS_CD, PP.STATUS_NAME AS PROJECT_STATUS,
													(PP.PROJ_REACH_PERCENT) AS PROJ_REACH_PERCENT, (PROJ_GRP) AS PROJ_GRP, 
													PP1.START_DATE AS PROJECT_START_DATE,
													PP1.END_DATE AS PROJECT_END_DATE,(PP.ON_MAP_START_DATE) AS START_DATE,
													(PP.ON_MAP_END_DATE) AS END_DATE,(PP.ON_MAP_START_DATE_TS) AS ON_MAP_START_DATE_TS,
													(PP.ON_MAP_END_DATE_TS) AS ON_MAP_END_DATE_TS,
													PP1.TARGET_FREQUENCY AS PROJECT_FREQ, RS1.SEGMENT_NAME AS PROJECT_SEGMENT,
													RS1.SEGMENT_ID AS PROJECT_SEGMENT_ID, RD1.NAME AS PROJECT_DEMO,
													RD1.DEMO_ID AS PROJECT_DEMO_ID, PP1.PREMIERE_DATE,
													CEIL(CASE WHEN DATEDIFF(DAY,(PP.ON_MAP_START_DATE),(PP.ON_MAP_END_DATE)) = 0 THEN 1 
																ELSE DATEDIFF(DAY,(PP.ON_MAP_START_DATE),(PP.ON_MAP_END_DATE)) END / 7) AS NO_OF_WEEKS,
													CEIL(CASE WHEN DATEDIFF(DAY,PP1.START_DATE,PP1.END_DATE) = 0 THEN 1 
																ELSE DATEDIFF(DAY,PP1.START_DATE,PP1.END_DATE) END / 7) AS UPDATED_NO_OF_WKS,
													PP1.CAMPAIGN_TYPE_ID, RCT.NAME AS CAMPAIGN_TYPE, PP.ON_MAP_DEMO AS DEMO_ID,
													RD.NAME AS DEMO, PP.SEGMENT AS SEGMENT_ID,
													(SELECT LISTAGG(COMPONENT_SEGMENT_ID, ', ') FROM REF_SEGMENT_COMPONENT WHERE SEGMENT_ID = PP.SEGMENT) AS SEGMENT_SELECTED,
													(PP.SEGMENT_TYPE) AS SEGMENT_TYPE, (PP.SEGMENT_AIRBYTE) AS SEGMENT_AIRBYTE, RS.SEGMENT_NAME AS SEGMENT_NAME,
													PL.NAME AS PLAN_NAME, PL_MSG.NAME AS MSG_PLAN_NAME, PL.PLAN_STATUS_ID,
													RPS.NAME AS PLAN_STATUS, PL.COMMENTS AS STATUS_COMMENT, 
													PP.FREQUENCY AS TARGET_FREQUENCY, PP.TARGET_REACH_PCT1 AS TARGET_REACH,
													PP.TARGET_GRP1 AS TARGET_GRP, (PP.EPISODE_PRIORITY) AS EPISODIC_PRIORITY,
													(PP.NO_OF_EPISODE) AS NO_OF_EPISODE,
														CASE WHEN PP.PAST_QTR_PROJECT_FLAG='Y' THEN 'N' ELSE IFNULL(PP.FLAG,'N') END AS COLOUR_CHECK_FLAG,
													PPEH.MAP_COLOUR_UPDATED_TS, PP.PROJ_PRIORITY AS PRIORITY,
													PP1.SCATTER_IMPS, PP1.SUBEVENT_ID, RES.SUBEVENT_NAME,
													(PP.GUIDE_REACH_PCT) AS GUIDE_REACH_PCT, (PP.GUIDE_GRP) AS GUIDE_GRP, (PP.GUIDE_SPOTS) AS GUIDE_SPOTS,
													IFNULL(ME.EXEC_END_DT, PL.UPDATE_DATE) AS EXEC_END_DT,
													PP.CAMPAIGN_GOAL_ID, PP.IS_DIRTY_PROJECT AS IS_DIRTY_PROJECT,
													PP.PAST_QTR_PROJECT_FLAG AS PAST_PROJECT, PP1.USER_COMMENT,
													PP.V_GET_VERSION AS LAST_OPT_VERSION_TS,
													CD.V_NETWORK_TARGET_DEMO_ID AS NETWORK_TARGET_DEMO_ID, 
													CD.V_NETWORK_TARGET_DEMO_NAME AS NETWORK_TARGET_DEMO_NAME,
													CASE WHEN PP1.MEDIA_OUTLET_ID = :IN_MEDIA_OUTLET_ID THEN 1 ELSE 99 END AS NETWORK_DISPLAY_SEQ,
													PP.CLUSTER_ID, PP.CLUSTER_NAME, (PP.PROJ_REACH_PERCENT) AS PROJ_PLANNED_REACH, 
													(PP.PROJ_GRP) AS PROJ_PLANNED_GRP, (PP.ON_MAP_PREMIER_DATE) AS ON_MAP_PREMIER_DATE
															FROM CTE_FINAL PP 
															JOIN CTE_DEMO CD ON 1=1  
															JOIN CTE_PLAN_STATUS CPS ON 1=1
														LEFT JOIN PR_PROJECT PP1 ON PP.PROJECT_ID=PP1.PROJECT_ID
														LEFT JOIN REF_PROJECT_PLAN_TYPE RPPT ON PP.PHASE_TYPE = RPPT.TYPE_ID
														LEFT JOIN PR_PROJECT_EPISODE_HEADER PPEH ON PP.PROJECT_ID = PPEH.PROJECT_ID
														-- LEFT JOIN PR_CAMPAIGN_GOAL PCG ON PP.CAMPAIGN_GOAL_ID = PCG.CAMPAIGN_GOAL_ID
														LEFT JOIN REF_DEMO RD ON RD.DEMO_ID = PP.ON_MAP_DEMO
														LEFT JOIN REF_DEMO RD1 ON RD1.DEMO_ID = PP1.TARGET_DEMO_ID
														LEFT JOIN REF_SEGMENT RS ON RS.SEGMENT_ID = PP.SEGMENT
														LEFT JOIN REF_SEGMENT RS1 ON RS1.SEGMENT_ID = PP1.TARGET_SEGMENT
														LEFT JOIN REF_2E_EVENT_SUBTYPE_PROJECT RES ON PP1.SUBEVENT_ID=RES.SUBEVENT_ID
														LEFT JOIN REF_MEDIA_OUTLET RMO ON PP1.MEDIA_OUTLET_ID=RMO.MEDIA_OUTLET_ID
														LEFT JOIN PR_PLAN PL ON PL.PLAN_ID=:IN_PLAN_ID
														LEFT JOIN PR_PLAN PL_MSG ON PL_MSG.PLAN_ID=PP.PLAN_ID
														LEFT JOIN REF_PLAN_STATUS RPS ON PL.PLAN_STATUS_ID=RPS.PLAN_STATUS_ID
														LEFT JOIN REF_CAMPAIGN_TYPE RCT ON RCT.CAMPAIGN_TYPE_ID=PP1.CAMPAIGN_TYPE_ID
														LEFT JOIN CTE_VERSION_PLAN ME ON ME.PLAN_ID=PL.PLAN_ID AND ME.PLAN_VERSION_ID=(SELECT V_PLAN_VERSION_ID FROM CTE_VERSION_ID)
														WHERE CPS.V_PLAN_STATUS_ID<>3
							) 
							,CTE_SPOT_BASED AS(
												SELECT (V_WEEK_START_DATE) AS V_WEEK_START_DATE ,PPS.CAMPAIGN_GOAL_ID, PPS.PROJECT_ID, SUM(PPS.PLANNED_UNITS) AS UNITS,CONCAT('WEEK',MAX(CW.WEEK_COUNT)) AS WEEK
												FROM PR_PLAN_SCHEDULE PPS 
												JOIN CTE_WEEK_DATE CW ON 1=1
												JOIN CTE_PLAN_STATUS CS ON 1=1
												JOIN CTE_VERSION_ID CV ON 1=1
												JOIN PR_PLAN_PROJECT PPP on  PPS.PLAN_ID=:IN_PLAN_ID AND PPS.PLAN_VERSION_ID=0 
												AND PPS.START_DATE BETWEEN CW.V_WEEK_START_DATE AND CW.V_WEEK_END_DATE
												AND PPS.PLAN_ID=PPP.PLAN_ID AND PPP.PLAN_VERSION_ID=CV.V_PLAN_VERSION_ID
												AND PPS.PROJECT_ID=PPP.PROJECT_ID 
												AND CASE WHEN CS.V_PERIOD_ID>316 THEN PPP.STATUS_CD in (9,10) ELSE 1=1 END	
												WHERE CS.V_PLAN_STATUS_ID=3
												group by V_WEEK_START_DATE,PPS.CAMPAIGN_GOAL_ID,PPS.PROJECT_ID
														
											UNION	
											
											SELECT (V_WEEK_START_DATE) AS V_WEEK_START_DATE,CAMPAIGN_GOAL_ID,PPS.PROJECT_ID, SUM(PLANNED_UNITS) AS UNITS,CONCAT('WEEK',MAX(CW.WEEK_COUNT)) AS WEEK
												FROM PR_PLAN_SCHEDULE PPS
												JOIN CTE_WEEK_DATE CW ON 1=1
												JOIN CTE_PLAN_STATUS CS ON 1=1
												JOIN PR_PROJECT PP on PPS.PROJECT_ID=PP.PROJECT_ID and PPS.PLAN_VERSION_ID=0 and PPS.MEDIA_OUTLET_ID=:IN_MEDIA_OUTLET_ID
												and PROJECT_TYPE=CS.V_PLAN_TYPE
												WHERE PPS.START_DATE BETWEEN CW.V_WEEK_START_DATE AND CW.V_WEEK_END_DATE
												and PPS.PLAN_ID<>:IN_PLAN_ID AND CS.V_PLAN_STATUS_ID=3
												group by V_WEEK_START_DATE,PPS.CAMPAIGN_GOAL_ID,PPS.PROJECT_ID
												
											UNION
											
												SELECT V_WEEK_START_DATE,NULL CAMPAIGN_GOAL_ID,PPPS.PROJECT_ID, UNIT_COUNT AS UNITS,CONCAT('WEEK',(CW.WEEK_COUNT)) AS WEEK
												FROM PR_PLAN_PROJECT_SPOTS  PPPS
												JOIN CTE_WEEK_DATE CW ON 1=1
												JOIN CTE_VERSION_ID CV ON 1=1
												JOIN CTE_PLAN_STATUS CS ON 1=1
												JOIN PR_PLAN_PROJECT PPP on PPP.PLAN_ID=PPPS.PLAN_ID and PPP.PLAN_VERSION_ID=PPPS.PLAN_VERSION_ID  
												AND PPP.PROJECT_ID=PPPS.PROJECT_ID 
												WHERE PPPS.PLAN_ID=:IN_PLAN_ID AND PPPS.PLAN_VERSION_ID=CV.V_PLAN_VERSION_ID 
												AND PPP.STATUS_CD not in (9,10) AND CS.V_PLAN_STATUS_ID=3
												and WEEK_DATE=CW.V_WEEK_START_DATE
												
											UNION
												
											SELECT (V_WEEK_START_DATE) AS V_WEEK_START_DATE,NULL AS CAMPAIGN_GOAL_ID, PROJECT_ID, UNIT_COUNT AS UNITS,CONCAT('WEEK',MAX(CW.WEEK_COUNT)) AS WEEK
												FROM PR_PLAN_PROJECT_SPOTS PPS
												JOIN CTE_WEEK_DATE CW ON 1=1
												JOIN CTE_VERSION_ID CV ON 1=1
												JOIN CTE_PLAN_STATUS CPS ON 1=1
												WHERE PPS.PLAN_ID=:IN_PLAN_ID 
												AND PLAN_VERSION_ID=CV.V_PLAN_VERSION_ID AND WEEK_DATE=CW.V_WEEK_START_DATE
												AND CPS.V_PLAN_STATUS_ID<>3
												group by V_WEEK_START_DATE,PPS.PROJECT_ID,UNIT_COUNT,CAMPAIGN_GOAL_ID
												
												
											UNION
											
												SELECT (V_WEEK_START_DATE) AS V_WEEK_START_DATE,NULL AS CAMPAIGN_GOAL_ID,PPS.PROJECT_ID, SUM(PPS.PLANNED_UNITS) AS UNITS,CONCAT('WEEK',MAX(CW.WEEK_COUNT)) AS WEEK
												FROM PR_PLAN_SCHEDULE PPS
												JOIN CTE_WEEK_DATE CW ON 1=1
												JOIN CTE_PLAN_STATUS CS ON 1=1
												JOIN CTE_QTR_DATE CV ON 1=1
												JOIN PR_PROJECT PP on PPS.PROJECT_ID=PP.PROJECT_ID and PPS.PLAN_VERSION_ID=0 
												AND PPS.MEDIA_OUTLET_ID=:IN_MEDIA_OUTLET_ID
												and date(PP.START_DATE)<=date(CV.V_QTR_END_DATE)   
												and PROJECT_TYPE=CS.V_PLAN_TYPE
												WHERE PPS.START_DATE BETWEEN CW.V_WEEK_START_DATE AND CW.V_WEEK_END_DATE
												and PPS.PLAN_ID<>:IN_PLAN_ID  AND CS.V_PLAN_STATUS_ID<>3
												group by V_WEEK_START_DATE,PPS.PROJECT_ID,CAMPAIGN_GOAL_ID
							)
							,CTE_GRP_BASED AS(
											SELECT (V_WEEK_START_DATE) V_WEEK_START_DATE,PPS.CAMPAIGN_GOAL_ID, PPS.PROJECT_ID, 
												SUM(PPS.PLANNED_GRP) AS UNITS,CONCAT('WEEK',MAX(CW.WEEK_COUNT)) AS WEEK  
												FROM PR_PLAN_SCHEDULE PPS 
												JOIN CTE_WEEK_DATE CW ON 1=1
												JOIN CTE_VERSION_ID CV ON 1=1
												JOIN CTE_PLAN_STATUS CS ON 1=1
												JOIN PR_PLAN_PROJECT PPP on  PPS.PLAN_ID=:IN_PLAN_ID AND PPS.PLAN_VERSION_ID=0 
												AND PPS.START_DATE BETWEEN CW.V_WEEK_START_DATE AND CW.V_WEEK_END_DATE
												AND PPS.PLAN_ID=PPP.PLAN_ID AND PPP.PLAN_VERSION_ID=CV.V_PLAN_VERSION_ID
												AND PPS.PROJECT_ID=PPP.PROJECT_ID AND PPP.STATUS_CD in (9,10)
												WHERE CS.V_PLAN_STATUS_ID=3
												group by V_WEEK_START_DATE,PPS.CAMPAIGN_GOAL_ID,PPS.PROJECT_ID
												
											UNION
											SELECT (V_WEEK_START_DATE) V_WEEK_START_DATE,PPS.CAMPAIGN_GOAL_ID,PPS.PROJECT_ID, 
											SUM(PLANNED_GRP) AS UNITS,CONCAT('WEEK',MAX(CW.WEEK_COUNT)) AS WEEK
												FROM PR_PLAN_SCHEDULE PPS
												JOIN CTE_WEEK_DATE CW ON 1=1
												JOIN CTE_PLAN_STATUS CS ON 1=1
												JOIN PR_PROJECT PP on PPS.PROJECT_ID=PP.PROJECT_ID and PPS.PLAN_VERSION_ID=0 and PPS.MEDIA_OUTLET_ID=:IN_MEDIA_OUTLET_ID
												and PROJECT_TYPE=CS.V_PLAN_TYPE
												WHERE PPS.START_DATE BETWEEN CW.V_WEEK_START_DATE AND CW.V_WEEK_END_DATE
												and PPS.PLAN_ID<>:IN_PLAN_ID AND CS.V_PLAN_STATUS_ID=3
												group by V_WEEK_START_DATE,PPS.CAMPAIGN_GOAL_ID,PPS.PROJECT_ID 
												
											UNION
											SELECT V_WEEK_START_DATE,NULL CAMPAIGN_GOAL_ID,PPPS.PROJECT_ID, UNIT_COUNT AS UNITS,CONCAT('WEEK',(CW.WEEK_COUNT)) AS WEEK
												FROM PR_PLAN_PROJECT_SPOTS PPPS
												JOIN CTE_WEEK_DATE CW ON 1=1
												JOIN CTE_VERSION_ID CV ON 1=1
												JOIN CTE_PLAN_STATUS CS ON 1=1
												JOIN PR_PLAN_PROJECT PPP on PPP.PLAN_ID=PPPS.PLAN_ID and PPP.PLAN_VERSION_ID=PPPS.PLAN_VERSION_ID  
												AND PPP.PROJECT_ID=PPPS.PROJECT_ID 
												WHERE PPPS.PLAN_ID=:IN_PLAN_ID AND PPPS.PLAN_VERSION_ID=CV.V_PLAN_VERSION_ID 
												AND  PPP.STATUS_CD not in (9,10) and WEEK_DATE=CW.V_WEEK_START_DATE
												AND CS.V_PLAN_STATUS_ID=3
											UNION
											SELECT (V_WEEK_START_DATE) AS V_WEEK_START_DATE,NULL AS CAMPAIGN_GOAL_ID, PROJECT_ID, UNIT_COUNT AS UNITS,CONCAT('WEEK',MAX(CW.WEEK_COUNT)) AS WEEK
												FROM PR_PLAN_PROJECT_SPOTS PPS
												JOIN CTE_WEEK_DATE CW ON 1=1
												JOIN CTE_VERSION_ID CV ON 1=1
												JOIN CTE_PLAN_STATUS CS ON 1=1
												WHERE PPS.PLAN_ID=:IN_PLAN_ID AND CS.V_PLAN_STATUS_ID<>3
												AND PLAN_VERSION_ID=CV.V_PLAN_VERSION_ID AND WEEK_DATE=CW.V_WEEK_START_DATE
												group by V_WEEK_START_DATE,PPS.PROJECT_ID,UNIT_COUNT,CAMPAIGN_GOAL_ID
												
											UNION 
												SELECT (V_WEEK_START_DATE) AS V_WEEK_START_DATE,NULL AS CAMPAIGN_GOAL_ID,PPS.PROJECT_ID, SUM(PLANNED_GRP) AS UNITS,CONCAT('WEEK',MAX(CW.WEEK_COUNT)) AS WEEK
												FROM PR_PLAN_SCHEDULE PPS
												JOIN CTE_WEEK_DATE CW ON 1=1
												JOIN CTE_PLAN_STATUS CS ON 1=1
												JOIN CTE_QTR_DATE CV ON 1=1
												JOIN PR_PROJECT PP on PPS.PROJECT_ID=PP.PROJECT_ID and PPS.PLAN_VERSION_ID=0 
												AND PPS.MEDIA_OUTLET_ID=:IN_MEDIA_OUTLET_ID
												and date(PP.START_DATE)<=date(CV.V_QTR_END_DATE)   
												and PROJECT_TYPE=CS.V_PLAN_TYPE
												WHERE PPS.START_DATE BETWEEN CW.V_WEEK_START_DATE AND CW.V_WEEK_END_DATE
												and PPS.PLAN_ID<>:IN_PLAN_ID AND CS.V_PLAN_STATUS_ID<>3
												group by V_WEEK_START_DATE,PPS.PROJECT_ID,CAMPAIGN_GOAL_ID 
							)
							,CTE_SPOT_BASED_DATA AS(
											SELECT PP.*,WD.V_WEEK_START_DATE,WEEK,UNITS
											FROM CTE_PLAN_DATA1 PP
											JOIN CTE_PLAN_STATUS CS ON 1=1
											LEFT JOIN CTE_SPOT_BASED WD ON
											PP.PROJECT_ID = WD.PROJECT_ID 
											WHERE :IN_OUTPUT_TYPE=1 AND CS.V_PLAN_STATUS_ID<>3--AND V_WEEK_START_DATE IS NOT NULL

											UNION

											SELECT PP.*,WD.V_WEEK_START_DATE,WEEK,UNITS
											FROM CTE_PLAN_DATA1 PP
											JOIN CTE_PLAN_STATUS CS ON 1=1
											LEFT JOIN CTE_SPOT_BASED WD ON
											CASE WHEN WD.CAMPAIGN_GOAL_ID IS NULL THEN
											PP.PROJECT_ID = WD.PROJECT_ID ELSE 
											PP.CAMPAIGN_GOAL_ID=WD.CAMPAIGN_GOAL_ID END
											WHERE :IN_OUTPUT_TYPE=1 AND CS.V_PLAN_STATUS_ID=3--AND V_WEEK_START_DATE IS NOT NULL
							)
							,CTE_GRP_BASED_DATA AS(
											SELECT PP.*,WD.V_WEEK_START_DATE,WEEK,UNITS
											FROM CTE_PLAN_DATA1 PP
											JOIN CTE_PLAN_STATUS CS ON 1=1
											LEFT JOIN CTE_GRP_BASED WD 
											ON PP.PROJECT_ID = WD.PROJECT_ID
											WHERE :IN_OUTPUT_TYPE<>1 AND CS.V_PLAN_STATUS_ID<>3 --AND WD.V_WEEK_START_DATE IS NOT NULL

											UNION

											SELECT PP.*,WD.V_WEEK_START_DATE,WEEK,UNITS
											FROM CTE_PLAN_DATA1 PP
											JOIN CTE_PLAN_STATUS CS ON 1=1
											LEFT JOIN CTE_GRP_BASED WD 
											ON CASE WHEN WD.CAMPAIGN_GOAL_ID IS NULL THEN
											PP.PROJECT_ID = WD.PROJECT_ID ELSE 
											PP.CAMPAIGN_GOAL_ID=WD.CAMPAIGN_GOAL_ID END
											WHERE :IN_OUTPUT_TYPE<>1 AND CS.V_PLAN_STATUS_ID=3 --AND WD.V_WEEK_START_DATE IS NOT NULL
							)

							SELECT * FROM CTE_SPOT_BASED_DATA
							UNION
							SELECT * FROM CTE_GRP_BASED_DATA;

'''
convert(dic,query)