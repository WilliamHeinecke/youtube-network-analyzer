from pyspark.sql import SparkSession
# import os
# os.environ['JAVA_HOME'] = "C:\\Program Files\\Java\\jdk-23"  # Replace with your actual Java path
# os.environ['PATH'] = os.environ['JAVA_HOME'] + "\\bin;" + os.environ['PATH']
spark = SparkSession.builder.master("local").appName("Test").getOrCreate()
print("Spark is working!")
spark.stop()
