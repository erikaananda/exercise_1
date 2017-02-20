# Devise a SQL query that finds the 10 hospitals with the highest quality of care, 
# along with their aggregate and average quality scores, as well as variability in scores.


#spark SQL
# get the best TIMELY AND EFFECTIVE CARE, using standard deviation for each score
# max count is 38 - make sure they have at least 2/3 of the possible scores (25)
create or replace view V_BEST_CARE 
AS
SELECT H.PROVR_ID, H.HOSP_NM, H.HOSP_ST_CD, H.BASE_SCORE, H.CONST_SCORE,
round(sum(cast(M.RAW_SCORE as float)), 2) AS SUM_SCORE,
round(AVG(cast(M.RAW_SCORE as float)), 2) as avg_score,
count(M.RAW_SCORE) as COUNT_SCORE,
round(AVG((cast(M.RAW_SCORE as float) - VM.AVG_SCORE) / VM.STDEV_SCORE), 2) as AVG_SD_SCORE
FROM T_HOSP H, T_HOSP_MEDL_MEAS_SCORE M, V_MEDL_MEAS_VAR VM
WHERE H.PROVR_ID = M.PROVR_ID
AND M.RAW_Score  <> 'Not Available'
AND M.MEDL_MEAS_ID = VM.MEDL_MEAS_ID
and M.MEDL_MEAS_ID NOT IN ("EDV", "MV", "ED_1b", "ED_2b", "OP_18b")
and substring(M.MEDL_MEAS_ID,1, 4) NOT IN ("MORT", "READ")
GROUP BY H.PROVR_ID, H.HOSP_NM, H.HOSP_ST_CD, H.BASE_SCORE, H.CONST_SCORE
HAVING COUNT_SCORE > 25
ORDER BY AVG_SD_SCORE  DESC
LIMIT 200;

# spark SQL results: care
SELECT PROVR_ID, HOSP_NM, BASE_SCORE, CONST_SCORE, AVG_SCORE, AVG_SD_SCORE
FROM V_BEST_CARE
ORDER BY AVG_SD_SCORE DESC
LIMIT 10;


# get the best (lowest) MORTALITY AND READMISSIONS using standard deviation for each score
# max count is 14 - make sure they have at least 2/3 of the possible scores (9)

#spark mortality and readmission
create or replace view V_LOWEST_MORT
AS
select  H.PROVR_ID, H.HOSP_NM, H.HOSP_ST_CD, H.BASE_SCORE, H.CONST_SCORE,
round(sum(cast(M.RAW_SCORE as float)), 2) AS SUM_SCORE,
round(AVG(cast(M.RAW_SCORE as float)), 2) as avg_score,
count(M.RAW_SCORE) as COUNT_SCORE,
round(AVG((cast(M.RAW_SCORE as float) - VM.AVG_SCORE) / VM.STDEV_SCORE), 2) as AVG_SD_SCORE
from T_HOSP H, T_HOSP_MEDL_MEAS_SCORE M, V_MEDL_MEAS_VAR VM
where  H.PROVR_ID = M.PROVR_ID
AND M.MEDL_MEAS_ID = VM.MEDL_MEAS_ID
AND M.RAW_Score  <> 'Not Available'
and substring(M.MEDL_MEAS_ID,1, 4) IN ("MORT", "READ")
group by  H.PROVR_ID, H.HOSP_NM, H.HOSP_ST_CD, H.BASE_SCORE, H.CONST_SCORE
HAVING COUNT_SCORE > 9
order by AVG_SD_SCORE  ASC
limit 200;

# spark SQL results: deaths and readmissions
SELECT PROVR_ID, HOSP_NM, BASE_SCORE, CONST_SCORE, AVG_SCORE, AVG_SD_SCORE
FROM V_LOWEST_MORT
ORDER BY AVG_SD_SCORE ASC
LIMIT 10;


# SPARK BEST CARE WITH LOW MORTALITY FILTER
SELECT C.PROVR_ID, C.HOSP_NM, 
C.BASE_SCORE AS BASE_SCORE, C.CONST_SCORE AS CONST_SCORE,
C.SUM_SCORE AS CARE_SUM_SCORE,C.AVG_SCORE AS CARE_AVG_SCORE, C.AVG_SD_SCORE AS CARE_AVG_SD_SCORE,
M.SUM_SCORE AS MORT_SUM_SCORE,M.AVG_SCORE AS MORT_AVG_SCORE, M.AVG_SD_SCORE AS MORT_AVG_SD_SCORE
FROM V_BEST_CARE C, V_LOWEST_MORT M
WHERE C.PROVR_ID = M.PROVR_ID
order by C.AVG_SD_SCORE DESC
LIMIT 10;



