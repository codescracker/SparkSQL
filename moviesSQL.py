from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *

spark = SparkSession\
		.builder\
		.master('local[*]')\
		.appName('popular movies')\
		.getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")

lines_movies = sc.textFile('/root/spark_course/ml-100k/u.data')
movies = lines_movies.map(lambda x : x.split()).map(lambda x : Row(movie_id = int(x[1])))
movies_df = spark.createDataFrame(movies).cache()

lines_items = sc.textFile('/root/spark_course/ml-100k/u.item')
items = lines_items.map(lambda x : x.split('|')).map(lambda x : Row(id = int(x[0]), name = x[1]))
items_df = spark.createDataFrame(items).cache()

joined_df = movies_df.join(items_df, movies_df.movie_id == items_df.id)

group_df = joined_df.groupBy(joined_df.name).count().orderBy("count", ascending=False).cache()


group_df.show()

spark.close()


