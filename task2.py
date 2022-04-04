"""
Task 2. Top 10 movies by genres

Лучшие фильмы нам известны, а что если ты захочешь посмотреть, ну, скажем лучший триллер всех времен и народов.
Так вот нужно найти по топ-10 фильмов каждого жанра (результат должен быть в одном файлике =))
"""

import pyspark.sql.functions as f
from pyspark.sql.window import Window
from SparkManager.SparkManager import SparkManager

title_basics = SparkManager().get_df('title.basics.tsv.gz')
rating = SparkManager().get_df('title.ratings.tsv.gz')

movies_popular_rating = (title_basics
                         .select('tconst', 'primaryTitle', 'startYear', 'genres')
                         .filter(f.col('titleType') == 'movie')
                         .join(rating, 'tconst')
                         .filter(f.col('numVotes') >= 100000)
                         )

# caching as we'll need it
SparkManager().cache_df(movies_popular_rating)

# window for genre, ordering by averageRating and numVotes
window_genre = (Window
                .partitionBy(movies_popular_rating['genres'])
                .orderBy(*[f.desc(col) for col in movies_popular_rating['averageRating', 'numVotes']])
                )

movies_by_genre = (movies_popular_rating
                   .select(f.col('*'), f.rank().over(window_genre).alias('rank'))
                   .filter(f.col('rank') <= 10)
                   .drop(f.col('rank'))
                   .orderBy(f.col('genres'))
                   .withColumnRenamed('genres', 'genre')
                   )

# saving df as csv file
SparkManager().save_csv(movies_by_genre, 'output', 'movies_by_genre.csv')
