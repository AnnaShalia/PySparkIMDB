import os
from pyspark import SparkFiles
from pyspark.sql import SparkSession


class SparkManager:

    """Class to work with dataframes using Spark"""

    # method to add url and get df from url
    @staticmethod
    def get_df(file_name, base_url='https://datasets.imdbws.com/'):
        spark.sparkContext.addFile(base_url + file_name)
        df = (spark.read
              .option('sep', '\t')
              .option('header', 'true')
              .option('inferSchema', 'true')
              .csv(SparkFiles.get(file_name)))
        return df

    @staticmethod
    def cache_df(df):
        df.cache()

    # method to save df as csv and rename it to have specified title
    # implies that 'path' doesn't exist or doesn't contain any filenames matching the below pattern
    @staticmethod
    def save_csv(df, path, new_file_name, mode='append'):
        df.coalesce(1).write.csv(path=path, header='true', mode=mode)
        for filename in os.listdir(path):
            if filename.startswith('part-00000'):
                old_f = os.path.join(path, filename)
                new_f = os.path.join(path, new_file_name)
                os.rename(old_f, new_f)


spark = (SparkSession.builder
         .master('local')
         .appName('IMDB analysis')
         .getOrCreate()
         )
# turn on INFO logging
spark.sparkContext.setLogLevel('INFO')