mkdir ../data/hosp/

mv data/"HCAHPS - State.csv" data/rating_state.csv
mv data/"HCAHPS - Hospital.csv" data/rating_hospital.csv
mv data/"Hospital General Information.csv" data/hospital_info.csv
mv data/"Measure Dates.csv" data/measure_dates.csv
mv data/"Readmissions and Deaths - Hospital.csv" data/readmissions_deaths_h.csv
mv data/"Timely and Effective Care - Hospital.csv" data/care_h.csv
mv data/"hvbp_hcahps_05_28_2015.csv" data/survey_h.csv

tail -n +2 /root/data/rating_state.csv > /home/w205/data/hosp/rating_state.csv
tail -n +2 /root/data/rating_hospital.csv > /home/w205/data/hosp/rating_hospital.csv
tail -n +2 /root/data/hospital_info.csv > /home/w205/data/hosp/hospital_info.csv
tail -n +2 /root/data/measure_dates.csv > /home/w205/data/hosp/measure_dates.csv
tail -n +2 /root/data/readmissions_deaths_h.csv > /home/w205/data/hosp/readmissions_deaths_h.csv
tail -n +2 /root/data/care_h.csv > /home/w205/data/hosp/care_h.csv
tail -n +2 /root/data/survey_h.csv > /home/w205/data/hosp/survey_h.csv



su - w205

cd data/hosp

ls
hdfs dfs -mkdir /user/w205/hospital_compare
hdfs dfs -mkdir /user/w205/hospital_compare/rating_hospital
hdfs dfs -put /home/w205/data/hosp/rating_hospital.csv /user/w205/hospital_compare/rating_hospital

hdfs dfs -mkdir /user/w205/hospital_compare/hospital_info
hdfs dfs -put /home/w205/data/hosp/hospital_info.csv /user/w205/hospital_compare/hospital_info

hdfs dfs -mkdir /user/w205/hospital_compare/measure_dates
hdfs dfs -put /home/w205/data/hosp/measure_dates.csv /user/w205/hospital_compare/measure_dates

hdfs dfs -mkdir /user/w205/hospital_compare/readmissions_deaths_h
hdfs dfs -put /home/w205/data/hosp/readmissions_deaths_h.csv /user/w205/hospital_compare/readmissions_deaths_h

hdfs dfs -mkdir /user/w205/hospital_compare/care_h
hdfs dfs -put /home/w205/data/hosp/care_h.csv /user/w205/hospital_compare/care_h

hdfs dfs -mkdir /user/w205/hospital_compare/survey_h
hdfs dfs -put /home/w205/data/hosp/survey_h.csv /user/w205/hospital_compare/survey_h
