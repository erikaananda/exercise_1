mkdir ../data/hosp/

mv data/"HCAHPS - State.csv" data/rating_state.csv
mv data/"HCAHPS - Hospital.csv" data/rating_hospital.csv
mv data/"Hospital General Information.csv" data/hospital_info.csv
mv data/"Measure Dates.csv" data/measure_dates.csv
mv data/"Readmissions and Deaths - Hospital.csv" data/readmissions_deaths_h.csv
mv data/"Timely and Effective Care - Hospital.csv" data/care_h.csv

tail -n +2 /root/data/rating_state.csv > /home/w205/data/hosp/rating_state.csv
tail -n +2 /root/data/rating_hospital.csv > /home/w205/data/hosp/rating_hospital.csv
tail -n +2 /root/data/hospital_info.csv > /home/w205/dat/hosp/hospital_info.csv
tail -n +2 /root/data/measure_dates.csv > /home/w205/data/hosp/measure_dates.csv
tail -n +2 /root/data/readmissions_deaths_h.csv > /home/w205/data/hosp/readmissions_deaths_h.csv
tail -n +2 /root/data/care_h.csv > /home/w205/data/hosp/care_h.csv



su - w205

cd ../../data/hosp

ls
hdfs dfs -mkdir /user/w205/hospital_compare

hdfs dfs -put * /user/w205/hospital_compare
