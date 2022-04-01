"""
Task 5. Top 5 movies for each director

Ну и напоследок, найди топ-5 фильмов по рейтингу у каждого режиссера
"""

import pyspark.sql.functions as f
from pyspark.sql.window import Window
from SparkManager.SparkManager import SparkManager

name_basics = SparkManager().get_df('name.basics.tsv.gz')
title_basics = SparkManager().get_df('title.basics.tsv.gz')
principals = SparkManager().get_df('title.principals.tsv.gz')
rating = SparkManager().get_df('title.ratings.tsv.gz')

movies_directors = (title_basics
                    .join(principals.filter(f.col('category') == 'director'), 'tconst')
                    .join(rating, 'tconst')
                    .join(name_basics, 'nconst')
                    .select('nconst', 'primaryName', 'primaryTitle', 'startYear', 'averageRating', 'numVotes')
                    )

window_directors = (Window
                    .partitionBy(movies_directors['nconst'])
                    .orderBy(*[f.desc(col) for col in movies_directors['averageRating', 'numVotes']])
                    )

movies_directors_top5 = (movies_directors
                         .select(f.col('*'), f.rank().over(window_directors).alias('rank'))
                         .filter(f.col('rank') <= 5)
                         .drop('rank', 'nconst')
                         )

# saving df as csv file
SparkManager().save_csv(movies_directors_top5, 'output', 'movies_directors_top5.csv')
