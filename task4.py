"""
Task 4. Top actors

Представь, что ты собрался снимать фильм и необходимо подобрать актерский состав.
Твоей задачей будет выбрать самых востребованных актеров, будем считать, что актер востребованный,
если он снимался в топовых фильмах и не один раз
"""

import pyspark.sql.functions as f
from SparkManager.SparkManager import SparkManager

name_basics = SparkManager().get_df('name.basics.tsv.gz')
title_basics = SparkManager().get_df('title.basics.tsv.gz')
principals = SparkManager().get_df('title.principals.tsv.gz')
rating = SparkManager().get_df('title.ratings.tsv.gz')

movies_popular_rating = (title_basics
                         .select('tconst', 'primaryTitle', 'startYear', 'genres')
                         .filter(f.col('titleType') == 'movie')
                         .join(rating, 'tconst')
                         .filter(f.col('numVotes') >= 100000)
                         )

# caching
SparkManager().cache_df(movies_popular_rating)

# Top 100 movies ever
movies_top100_alltime = (movies_popular_rating
                         .select('tconst', 'primaryTitle', 'numVotes', 'averageRating', 'startYear')
                         .orderBy('averageRating', 'numVotes', ascending=False)
                         .limit(100)
                         )

# cashing
SparkManager().cache_df(movies_top100_alltime)

top_actors = (movies_top100_alltime
              .join(principals.filter(f.col('category') == 'actor'), 'tconst')
              .groupBy(f.col('nconst')).count()
              .filter(f.col('count') >= 2)
              .join(name_basics, 'nconst')
              .orderBy(f.col('count'), ascending=False)
              .select('primaryName')
              )

# saving df as csv file
SparkManager().save_csv(top_actors, 'output', 'top_actors.csv')
