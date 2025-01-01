taskkill /im python.exe /f
python clean.py
cls
spark-submit --driver-memory 24g --executor-memory 16g --conf "spark.pyspark.python=C:\Users\romeo\AppData\Local\Programs\Python\Python311\python.exe" --conf "spark. app.name=BigData_Romeo_Ruggiero_PySpark" --conf "spark.master=local[*]" --conf "spark.driver.bindAddress=127.0.0.1" --conf "spark.network. timeout=600s" --conf "spark.executor.heartbeatInterval=100s" --conf "spark.driver.maxResultSize=12g" --conf "spark.driver.host=127.0.0.1" main.py