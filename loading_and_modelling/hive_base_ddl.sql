

### DDL FOR T_HOSP_RAW hospital_info.csv ### 

DROP TABLE T_HOSP_RAW;  
CREATE EXTERNAL TABLE T_HOSP_RAW
( PROVR_ID INT,
HOSP_NM VARCHAR(100),
HOSP_STR_ADDR VARCHAR(100),
HOSP_CITY_NM VARCHAR(50),
HOSP_ST_CD CHAR(2),
HOSP_PSTL_CD CHAR(5),
HOSP_CNTY_NM VARCHAR(50),
HOSP_PHN_NUM CHAR(10),
HOSP_TYP_DESC VARCHAR(30),
HOSP_OWNRSHP_DESC VARCHAR(30),
EMGC_SVC_IND CHAR(3)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
"separatorChar" = ",",  
"quoteChar" = '"', 
"escapeChar" = '\\' ) 
STORED AS TEXTFILE 
LOCATION '/user/w205/hospital_compare/hospital_info'; 

### DDL FOR T_EFFCTV_CARE_RAW care_h.csv ### 

DROP TABLE T_EFFCTV_CARE_RAW;  
CREATE EXTERNAL TABLE T_EFFCTV_CARE_RAW
( PROVR_ID INT,
HOSP_NM VARCHAR(100),
HOSP_STR_ADDR VARCHAR(100),
HOSP_CITY_NM VARCHAR(50),
HOSP_ST_CD CHAR(2),
HOSP_PSTL_CD CHAR(5),
HOSP_CNTY_NM VARCHAR(50),
HOSP_PHN_NUM CHAR(10),
MEDL_COND_DESC VARCHAR(256),
MEDL_MEAS_ID VARCHAR(15),
MEDL_MEAS_DESC VARCHAR(256),
RAW_SCORE VARCHAR(100),
SAMPL_SIZE VARCHAR(15),
FOOTNOTE_TEXT VARCHAR(256),
MEAS_STRT_DT DATE,
MEAS_END_DT DATE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
"separatorChar" = ",",  
"quoteChar" = '"', 
"escapeChar" = '\\' ) 
STORED AS TEXTFILE 
LOCATION '/user/w205/hospital_compare/care_h'; 

### DDL FOR T_READM_DEATHS_RAW readmissions_deaths_h.csv ### 

DROP TABLE T_READM_DEATHS_RAW;  
CREATE EXTERNAL TABLE T_READM_DEATHS_RAW
( PROVR_ID INT,
HOSP_NM VARCHAR(100),
HOSP_STR_ADDR VARCHAR(100),
HOSP_CITY_NM VARCHAR(50),
HOSP_ST_CD CHAR(2),
HOSP_PSTL_CD CHAR(5),
HOSP_CNTY_NM VARCHAR(50),
HOSP_PHN_NUM CHAR(10),
MEDL_MEAS_DESC VARCHAR(256), 
MEDL_MEAS_ID VARCHAR(15),
NATL_COMPR_TXT VARCHAR(50),
DENOMNTR_NUM VARCHAR(15),
RAW_SCORE VARCHAR(100),
LOW_EST_NUM VARCHAR(15),
HIGH_EST_NUM VARCHAR(15),
FOOTNOTE_TEXT VARCHAR(256),
MEAS_STRT_DT DATE,
MEAS_END_DT DATE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
"separatorChar" = ",",  
"quoteChar" = '"', 
"escapeChar" = '\\' ) 
STORED AS TEXTFILE 
LOCATION '/user/w205/hospital_compare/readmissions_deaths_h';

### DDL FOR T_MEDL_MEAS_RAW measure_dates.csv ### 

DROP TABLE T_MEDL_MEAS_RAW;  
CREATE EXTERNAL TABLE T_MEDL_MEAS_RAW
( MEDL_MEAS_DESC VARCHAR(256), 
MEDL_MEAS_ID VARCHAR(15),
MEAS_STRT_QTR CHAR(6),
MEAS_END_QTR CHAR(6),
MEAS_STRT_DT DATE,
MEAS_END_DT DATE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
"separatorChar" = ",",  
"quoteChar" = '"', 
"escapeChar" = '\\' ) 
STORED AS TEXTFILE 
LOCATION '/user/w205/hospital_compare/measure_dates';

### DDL FOR T_HOSP_SRVY_MEAS_SCORE_RAW rating_hospital.csv ### 

DROP TABLE T_HOSP_SRVY_MEAS_SCORE_RAW;  
CREATE EXTERNAL TABLE T_HOSP_SRVY_MEAS_SCORE_RAW
( PROVR_ID INT,
HOSP_NM VARCHAR(100),
HOSP_STR_ADDR VARCHAR(100),
HOSP_CITY_NM VARCHAR(50),
HOSP_ST_CD CHAR(2),
HOSP_PSTL_CD CHAR(5),
HOSP_CNTY_NM VARCHAR(50),
CWN_ACHV_SCORE VARCHAR(15),
CWN_IMPRV_SCORE VARCHAR(15),
CWN_DIM_SCORE VARCHAR(15),
CWD_ACHV_SCORE VARCHAR(15),
CWD_IMPRV_SCORE VARCHAR(15),
CWD_DIM_SCORE VARCHAR(15),
RHS_ACHV_SCORE VARCHAR(15),
RHS_IMPRV_SCORE VARCHAR(15),
RHS_DIM_SCORE VARCHAR(15),
PM_ACHV_SCORE VARCHAR(15),
PM_IMPRV_SCORE VARCHAR(15),
PM_DIM_SCORE VARCHAR(15),
CAM_ACHV_SCORE VARCHAR(15),
CAM_IMPRV_SCORE VARCHAR(15),
CAM_DIM_SCORE VARCHAR(15),
CAQ_ACHV_SCORE VARCHAR(15),
CAQ_IMPRV_SCORE VARCHAR(15),
CAQ_DIM_SCORE VARCHAR(15),
DI_ACHV_SCORE VARCHAR(15),
DI_IMPRV_SCORE VARCHAR(15),
DI_DIM_SCORE VARCHAR(15),
ORH_ACHV_SCORE VARCHAR(15),
ORH_IMPRV_SCORE VARCHAR(15),
ORH_DIM_SCORE VARCHAR(15),
BASE_SCORE SMALLINT,
CONST_SCORE SMALLINT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
"separatorChar" = ",",  
"quoteChar" = '"', 
"escapeChar" = '\\' ) 
STORED AS TEXTFILE 
LOCATION '/user/w205/hospital_compare/survey_h';