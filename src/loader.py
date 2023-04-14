import logging
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType


class Loader:

    def __init__(self):
        self.host = 'localhost'
        self.port = 15432
        self.dbname = 'campaign'
        self.user = 'postgres'
        self.password = 'Postgres'
        self.table_name = 'advertisings'
        self.spark = SparkSession.builder.appName('Loader').getOrCreate()
        self.conn = None
        self.cursor = None
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def connect(self):

        self.conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.user,
            password=self.password
        )
        self.cursor = self.conn.cursor()
        self.logger.info("Connected to database.")

    def upload_dataframe(self, dataframe):

        self.logger.info('Starting upload...')

        rows = [Row(*row) for row in dataframe.rdd.collect()]

        schema = StructType([StructField(str(col), StringType(), True)
                            for col in dataframe.columns])

        df = self.spark.createDataFrame(rows, schema=schema)

        df.write.jdbc(
            url=f"jdbc:postgresql://{self.host}:{self.port}/{self.dbname}",
            table=self.table_name,
            mode='overwrite',
            properties={
                'user': self.user,
                'password': self.password
            }
        )

        self.logger.info("Upload successful!")

    def disconnect(self):

        self.cursor.close()
        self.conn.close()
        self.logger.info("Disconnected from database.")
