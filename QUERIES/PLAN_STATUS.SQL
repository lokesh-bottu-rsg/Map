SELECT IFNULL(PP.PLAN_STATUS_ID,0) V_PLAN_STATUS_ID FROM PR_PLAN PP WHERE PLAN_ID= :IN_PLAN_ID
AND MEDIA_OUTLET_ID= :IN_MEDIA_OUTLET_ID;