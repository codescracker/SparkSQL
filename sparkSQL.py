from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *


spark = SparkSession.builder\
		.master('local[*]')\
		.appName('SQL')\
		.getOrCreate()

sc = spark.sparkContext

lines = sc.textFile('/root/spark_course/fakefriends.csv')
people = lines\
			.map(lambda x : x.split(','))\
			.map(lambda p : (p[0], p[1], p[2], p[3]))


schema = ['ID', 'Name', 'age', 'numofFriends']

fields = [StructField(fieldName, StringType(), True) for fieldName in schema]

schemaUsers = spark.createDataFrame(people, schema).cache()
schemaUsers.createOrReplaceTempView("users")


schemaUsers.filter(schemaUsers.age>=13).filter(schemaUsers.age<=19).show()
spark.sql('SELECT * FROM users WHERE age>=13 and age<=19').show()


spark.stop()