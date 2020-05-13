export PYSPARK_PYTHON=python3.6
time spark-submit --packages graphframes:graphframes:0.6.0-spark2.3-s_2.11 task1.py 7 ../resource/asnlib/publicdata/ub_sample_data.csv task1_1_ans
