from pyspark.sql import SparkSession, Row, functions


def load_movie_names():
  movie_names = {}
  with open('ml-100k/u.ITEM') as f:
    for line in f:
      fields = line.split('|')
      movie_names[int(fields[0])] = fields[1]
  return movie_names

# create a sparkSession ( the config bit is only for windows)

spark = SparkSession.builder.config('spark.sql.warehouse.dir','file:///C:/temp').appName('PopularMovies').getOrCreate()
# load up our movie ID -> Name dictionary
name_dict = load_movie_names()

#get the raw data
lines = spark.sparkContext.textFile('ml-100k/u.data')
movies = lines.map(lambda x: Row(movieID = int(x.split()[1])))

movie_dataset = spark.createDataFrame(movies)

top_movies_ids = movie_dataset.groupBy('movieID').count().orderBy('count', ascending=False).cache()

# top_movies_ids.show()

# #grab the top 10
# top10= top_movies_ids.take(10)
# for result in top10:
#   print("%s: %d") % (name_dict[result[0]], result[1])

# Stop the session
spark.stop()