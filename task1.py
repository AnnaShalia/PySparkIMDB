"""
Task 1. Top movies

Найди топ-100 фильмов(да-да, именно фильмов, а не сериалов или ТВ-шоу),
заметь что фильмы с рейтингом 9,9 и со 100 голосами мы не можем считать популярными,
пусть если за фильм проголосовало хотя бы 100 000 человек - он популярный
- за все время,
- за последние 10 лет,
- фильмы которые были популярны в 60-х годах прошлого века.
"""

import pyspark.sql.functions as f
from datetime import date
from SparkManager.SparkManager import SparkManager

title_basics = SparkManager().get_df('title.basics.tsv.gz')
rating = SparkManager().get_df('title.ratings.tsv.gz')

movies_popular_rating = (title_basics
                         .select('tconst', 'primaryTitle', 'startYear', 'genres')
                         .filter(f.col('titleType') == 'movie')
                         .join(rating, 'tconst')
                         .filter(f.col('numVotes') >= 100000)
                         )

# caching as we'll need it in the future tasks
SparkManager().cache_df(movies_popular_rating)

# 1.1. Top 100 movies ever
movies_top100_alltime = (movies_popular_rating
                         .select('tconst', 'primaryTitle', 'numVotes', 'averageRating', 'startYear')
                         .orderBy('averageRating', 'numVotes', ascending=False)
                         .limit(100)
                         )

# cashing as we'll need it in the future tasks
SparkManager().cache_df(movies_top100_alltime)

# saving df as csv file
SparkManager().save_csv(movies_top100_alltime, 'output', 'movies_top100_alltime.csv')

# 1.2. Top 100 movies for the last 10 years
movies_top100_10years = (movies_popular_rating
                         .select('tconst', 'primaryTitle', 'numVotes', 'averageRating', 'startYear')
                         .filter(f.col('StartYear') >= date.today().year - 10)
                         .orderBy('averageRating', 'numVotes', ascending=False)
                         .limit(100))

# saving df as csv file
SparkManager().save_csv(movies_top100_10years, 'output', 'movies_top100_10years.csv')

# 1.3. Top 100 movies released in 60s
movies_top100_60s = (movies_popular_rating
                     .select('tconst', 'primaryTitle', 'numVotes', 'averageRating', 'startYear')
                     .filter((f.col('StartYear') >= 1960) &
                             (f.col('StartYear') <= 1969))
                     .orderBy('averageRating', 'numVotes', ascending=False)
                     .limit(100))

# saving df as csv file
SparkManager().save_csv(movies_top100_60s, 'output', 'movies_top100_60s.csv')
