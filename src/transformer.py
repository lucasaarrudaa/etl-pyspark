from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, FloatType, TimestampType
from pyspark.sql.functions import col
from pyspark.sql.functions import expr

class Transformer:
    
    def __init__(self):
        self.spark = SparkSession.builder.appName("Transformer").getOrCreate()
    
    def table_maker(self, df):
        '''
        Changing types of columns in DF.        
        '''
        try:
            self.spark.log.info("Converting column types...")
            dtypes = {'campaign_date': TimestampType(),
                      'campaign_name': StringType(),
                      'impressions': IntegerType(),
                      'clicks': StringType(),
                      'cost': FloatType(),
                      'advertising': StringType(),
                      'ip': StringType(),
                      'device_id': StringType(),
                      'campaign_link': StringType(),
                      'data_click': TimestampType(),
                      'lead_id': StringType(),
                      'registered_at': TimestampType(),
                      'credit_decision': StringType(),
                      'credit_decision_at': TimestampType(),
                      'signed_at': TimestampType(),
                      'revenue': FloatType()}
            dt = df.astype(dtypes)
            self.spark.log.info("Column types converted successfully.")
            return dt
        except Exception as e:
            self.spark.log.error("Error in table_maker: {}".format(str(e)))
            raise e
    
    def extract_string(self, table, column, regex):
        '''
        Extracting a string using REGEX.
        
        Parameters:
                column: name of column 
                regex: regex code
                new_col_name: name of the new column.
                
        Returns:
                Return what do you want to extract.
        '''
        try:
            self.spark.log.info("Extracting string from column: {}".format(column))
            extracted = table.withColumn(column, expr('regexp_extract({}, "{}", 1)'.format(column, regex)))
            self.spark.log.info("String extracted successfully.")
            return extracted
        except Exception as e:
            self.spark.log.error("Error in extract_string: {}".format(str(e)))
            raise e
    
    def delete_col(self, df, column):
        '''
        Deleting a column from DF.
        
        Parameters: 
                df: dataframe
                column: column to delete
        NOTE: you need to assign to a df. 
        '''
        try:
            self.spark.log.info("Deleting column: {}".format(column))
            deleted = df.drop(column)
            self.spark.log.info("Column deleted successfully.")
            return deleted
        except Exception as e:
            self.spark.log.error("Error in delete_col: {}".format(str(e)))
            raise e
    
    def rename(self, df, column1, new_name):
        '''
        Renaming a column from a DF.
        '''
        try:
            self.spark.log.info("Renaming column: {} to {}".format(column1, new_name))
            df = df.withColumnRenamed(column1, new_name)
            self.spark.log.info("Column renamed successfully.")
            return df
        except Exception as e:
            self.spark.log.error("Error in rename: {}".format(str(e)))
            raise e

    def concat(self, df1, df2, ignore_index=True):
        '''
        Concat dfs.
        '''
        try:
            self.spark.log.info("Concatenating dataframes...")
            concatenated = df1.union(df2)
            if ignore_index:
                concatenated = concatenated.withColumn("index", expr("monotonically_increasing_id()")).drop("index")
            self.spark.log.info("Dataframes concatenated successfully.")
            return concatenated
        except Exception as e:
            self.spark.log.error("Error in concat: {}".format(str(e)))
            raise e
    
    
