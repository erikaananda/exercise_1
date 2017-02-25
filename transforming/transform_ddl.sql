
DROP TABLE T_MEDL_COND;

CREATE TABLE T_MEDL_COND
AS 
SELECT DISTINCT 
MEDL_COND_DESC,
current_date AS CRT_DT,
current_date AS UPD_DT
FROM T_EFFCTV_CARE_RAW
;

DROP TABLE  T_MEDL_MEAS;

CREATE TABLE T_MEDL_MEAS
AS
SELECT DISTINCT
m.MEDL_MEAS_ID,
m.MEDL_MEAS_DESC , 
c.MEDL_COND_DESC,
m.MEAS_STRT_DT,
m.MEAS_END_DT ,
current_date AS CRT_DT,
current_date AS UPD_DT
FROM  T_MEDL_MEAS_RAW m, T_EFFCTV_CARE_RAW c
where c.MEDL_MEAS_ID = m.MEDL_MEAS_ID
;

DROP TABLE T_HOSP_TYPE;

CREATE TABLE T_HOSP_TYPE
AS
SELECT DISTINCT
HOSP_TYP_DESC,
current_date AS CRT_DT,
current_date AS UPD_DT
FROM T_HOSP_RAW
;

DROP TABLE T_ST;

CREATE TABLE T_ST
AS
SELECT DISTINCT
HOSP_ST_CD, 
current_date AS CRT_DT,
current_date AS UPD_DT
FROM T_HOSP_RAW
;

DROP TABLE T_HOSP_OWNRSHP;

CREATE TABLE T_HOSP_OWNRSHP
AS
SELECT DISTINCT
HOSP_OWNRSHP_DESC,
current_date AS CRT_DT,
current_date AS UPD_DT
FROM T_HOSP_RAW
;

drop table t_hosp;

create table t_hosp
as
select 
h.provr_id,
h.hosp_nm,
h.hosp_str_addr,
h.hosp_city_nm,
h.hosp_st_cd,
h.hosp_pstl_cd,
h.hosp_cnty_nm,
h.hosp_phn_num,
h.hosp_typ_desc,
h.hosp_ownrshp_desc,
h.emgc_svc_ind,
s.base_score,
s.const_score,
current_date as CRT_DT,
current_date as UPD_DT
FROM t_hosp_raw h, t_hosp_srvy_meas_score_raw s
where h.provr_id = s.provr_id
;

DROP TABLE T_HOSP_MEDL_MEAS_SCORE;

CREATE TABLE T_HOSP_MEDL_MEAS_SCORE
AS
SELECT DISTINCT
PROVR_ID,
MEDL_MEAS_ID,
MEDL_MEAS_DESC,
RAW_SCORE,
SAMPL_SIZE,
current_date as CRT_DT,
current_date as UPD_DT
FROM T_EFFCTV_CARE_RAW
UNION ALL
SELECT DISTINCT
R.PROVR_ID,
R.MEDL_MEAS_ID,
M.MEDL_MEAS_DESC,
R.RAW_SCORE,
'Not Available' as SAMPL_SIZE,
current_date as CRT_DT,
current_date as UPD_DT
FROM T_READM_DEATHS_RAW R, T_MEDL_MEAS M
where R.MEDL_MEAS_ID =  M.MEDL_MEAS_ID
;



DROP TABLE T_SRVY_MEAS;

CREATE TABLE T_SRVY_MEAS 
(SRVY_MEAS_ID VARCHAR(15),
SRVY_MEAS_DESC VARCHAR(256),
CRT_DT DATE,
UPD_DT DATE
)
;

INSERT INTO T_SRVY_MEAS
VALUES 
('CWN','Communication with Nurses', '2017-02-04', '2017-02-04'),
('CWD','Communication with Doctors', '2017-02-04', '2017-02-04'),
('RHS','Responsiveness of Hospital Staff', '2017-02-04', '2017-02-04'),
('PM','Pain Management', '2017-02-04', '2017-02-04'),
('CAM','Communication about Medicines', '2017-02-04', '2017-02-04'),
('CAQ','Cleanliness and Quietness of Hospital Environment', '2017-02-04', '2017-02-04'),
('DI','Discharge Information', '2017-02-04', '2017-02-04'),
('ORH', 'Overall Rating of Hospital', '2017-02-04', '2017-02-04')
;

DROP TABLE T_HOSP_SRVY_MEAS_SCORE;

CREATE TABLE T_HOSP_SRVY_MEAS_SCORE
AS
SELECT DISTINCT 
PROVR_ID,
'CWN' AS SRVY_MEAS_ID,
CWN_ACHV_SCORE AS ACHV_PT,
CWN_IMPRV_SCORE AS IMPRV_PT,
CWN_DIM_SCORE AS DIM_SCORE, 
CURRENT_DATE AS CPD_DT,
CURRENT_DATE AS UPD_DT
FROM T_HOSP_SRVY_MEAS_SCORE_RAW
UNION ALL
SELECT DISTINCT 
PROVR_ID,
'CWD' AS SRVY_MEAS_ID,
CWD_ACHV_SCORE AS ACHV_PT,
CWD_IMPRV_SCORE AS IMPRV_PT,
CWD_DIM_SCORE AS DIM_SCORE, 
CURRENT_DATE AS CPD_DT,
CURRENT_DATE AS UPD_DT
FROM T_HOSP_SRVY_MEAS_SCORE_RAW
UNION ALL
SELECT DISTINCT 
PROVR_ID,
'RHS' AS SRVY_MEAS_ID,
RHS_ACHV_SCORE AS ACHV_PT,
RHS_IMPRV_SCORE AS IMPRV_PT,
RHS_DIM_SCORE AS DIM_SCORE, 
CURRENT_DATE AS CPD_DT,
CURRENT_DATE AS UPD_DT
FROM T_HOSP_SRVY_MEAS_SCORE_RAW
UNION ALL
SELECT DISTINCT 
PROVR_ID,
'PM' AS SRVY_MEAS_ID,
PM_ACHV_SCORE AS ACHV_PT,
PM_IMPRV_SCORE AS IMPRV_PT,
PM_DIM_SCORE AS DIM_SCORE, 
CURRENT_DATE AS CPD_DT,
CURRENT_DATE AS UPD_DT
FROM T_HOSP_SRVY_MEAS_SCORE_RAW
UNION ALL
SELECT DISTINCT 
PROVR_ID,
'CAM' AS SRVY_MEAS_ID,
CAM_ACHV_SCORE AS ACHV_PT,
CAM_IMPRV_SCORE AS IMPRV_PT,
CAM_DIM_SCORE AS DIM_SCORE, 
CURRENT_DATE AS CPD_DT,
CURRENT_DATE AS UPD_DT
FROM T_HOSP_SRVY_MEAS_SCORE_RAW
UNION ALL
SELECT DISTINCT 
PROVR_ID,
'CAQ' AS SRVY_MEAS_ID,
CAQ_ACHV_SCORE AS ACHV_PT,
CAQ_IMPRV_SCORE AS IMPRV_PT,
CAQ_DIM_SCORE AS DIM_SCORE, 
CURRENT_DATE AS CPD_DT,
CURRENT_DATE AS UPD_DT
FROM T_HOSP_SRVY_MEAS_SCORE_RAW
UNION ALL
SELECT DISTINCT 
PROVR_ID,
'DI' AS SRVY_MEAS_ID,
DI_ACHV_SCORE AS ACHV_PT,
DI_IMPRV_SCORE AS IMPRV_PT,
DI_DIM_SCORE AS DIM_SCORE, 
CURRENT_DATE AS CPD_DT,
CURRENT_DATE AS UPD_DT
FROM T_HOSP_SRVY_MEAS_SCORE_RAW
UNION ALL
SELECT DISTINCT 
PROVR_ID,
'ORH' AS SRVY_MEAS_ID,
ORH_ACHV_SCORE AS ACHV_PT,
ORH_IMPRV_SCORE AS IMPRV_PT,
ORH_DIM_SCORE AS DIM_SCORE, 
CURRENT_DATE AS CPD_DT,
CURRENT_DATE AS UPD_DT
FROM T_HOSP_SRVY_MEAS_SCORE_RAW
;

CREATE OR REPLACE VIEW V_MEDL_MEAS_VAR
AS
SELECT 
MEDL_MEAS_ID, 
AVG(RAW_SCORE) AS AVG_SCORE,
STDDEV_POP(RAW_SCORE) AS STDEV_SCORE
FROM T_HOSP_MEDL_MEAS_SCORE
GROUP BY
MEDL_MEAS_ID
;


