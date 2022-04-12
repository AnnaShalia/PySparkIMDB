"""
Task 3. Top 10 movies by year range

А теперь усложним задачу. Нужно найти все то же самое,
но только для каждого десятилетия c сейчас до 1950х (тоже в одном файле)
"""

import pyspark.sql.functions as f
from pyspark.sql.window import Window
from SparkManager.SparkManager import SparkManager

title_basics = SparkManager().get_df('title.basics.tsv.gz')
rating = SparkManager().get_df('title.ratings.tsv.gz')

movies_popular_rating_genre = (title_basics
                               .select('tconst', 'primaryTitle', 'startYear', 'genres')
                               .filter(f.col('titleType') == 'movie')
                               .join(rating, 'tconst')
                               .filter(f.col('numVotes') >= 100000)
                               .withColumn('genre', f.explode(f.split('genres', ',')))
                               )

# caching
SparkManager().cache_df(movies_popular_rating_genre)

movies_year_range = (movies_popular_rating_genre
                     .filter(f.col('startYear') >= 1950)
                     .withColumn('yearRange', f.concat_ws(' - ', f.floor(f.col('startYear') / 10) * 10 + 1,
                                                          f.floor(f.col('startYear') / 10) * 10 + 10)))

window_decade_genre = (Window
                       .partitionBy(movies_year_range['yearRange'], movies_year_range['genre'])
                       .orderBy(*[f.desc(col) for col in movies_popular_rating_genre['averageRating', 'numVotes']])
                       )

movies_by_decade = (movies_year_range
                    .select(f.col('*'),
                            f.rank().over(window_decade_genre).alias('rank'))
                    .filter(f.col('rank') <= 10)
                    .drop('rank', 'genres')
                    .orderBy(f.col('yearRange'), f.col('genre'))
                    )

# saving df as csv file
SparkManager().save_csv(movies_by_decade, 'output', 'movies_by_decade.csv')
