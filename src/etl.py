from src.extractor import Extractor
from src.transformer import Transformer
from src.loader import Loader
from pyspark.sql import SparkSession
import pandas as pd

class Etl:
    def __init__(self):
        self.spark = SparkSession.builder.appName("ETLJob").getOrCreate()

        leads = ExtractorS3('datalake-my-lucas-bucket').download('data/customer_leads_funnel.csv', 'datasets/customer_leads_funnel.csv')
        fb = ExtractorS3('datalake-my-lucas-bucket').download('data/facebook_ads_media_costs.jsonl', 'datasets/facebook_ads_media_costs.jsonl')
        pv = ExtractorS3('datalake-my-lucas-bucket').download('data/pageview.txt','datasets/pageview.txt')
        ggl = ExtractorS3('datalake-my-lucas-bucket').download('data/google_ads_media_costs.jsonl','datasets/google_ads_media_costs.jsonl')

        cols2 = ['ips',
                'device_id',
                'refer']

        cols1 = ['device_id',
                'lead_id',
                'registered_at',
                'credit_decision',
                'credit_decision_at',
                'signed_at',
                'revenue']

        df_leads = self.spark.read.csv(leads, header=True, schema=cols1)
        df_pv = self.spark.read.option("delimiter", "|").csv(pv, header=True, schema=cols2)
        df_fb = self.spark.read.json(fb, lines=True)
        df_ggl = self.spark.read.json(ggl, lines=True)

        transforms = [
            ('ip', 'ips', '\d{3}\.\d{1,3}\.\d{1,3}\.\d{1,3}'),
            ('device_id', 'device_id', '\:\s.+'),
            ('device_id', 'device_id', '\s.+'),
            ('click', 'ips', 'http.+'),
            ('referer', 'refer', 'http.+'),
            ('data', 'ips', '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}')]

        for new_col, old_col, regex in transforms:
            df_pv = Transformer().extract_string(df_pv, old_col, regex, new_col)

        delete_cols_pv = ['ips', 'refer']
        for col in delete_cols_pv:
            df_pv = Transformer().delete_col(df_pv, col)

        delete_cols_ggl = ['ad_creative_name', 'ad_creative_id', 'google_campaign_id']
        for col in delete_cols_ggl:
            df_ggl = Transformer().delete_col(df_ggl, col)

        df_fb = Transformer().delete_col(df_fb, 'facebook_campaign_id')

        renames = [
            ('data', 'data_click'),
            ('click', 'campaign_link'),
            ('referer', 'advertising'),
            ('date', 'campaign_date'),
            ('facebook_campaign_name', 'campaign_name'),
            ('date', 'campaign_date'),
            ('google_campaign_name', 'campaign_name')]

        for old_name, new_name in renames:
            df_pv = Transformer().rename(df_pv, old_name, new_name)
            df_fb = Transformer().rename(df_fb, old_name, new_name)
            df_ggl = Transformer().rename(df_ggl, old_name, new_name)

        df_fb = Transformer().fill(df_fb, 460, 'http://www.facebook.com')
        df_ggl = Transformer().fill(df_ggl, 5796, 'http://google.com.br')

        df_fb_spark = self.spark.createDataFrame(df_fb)
        df_ggl_spark = self.spark.createDataFrame(df_ggl)

        advertisings = df_fb_spark.union(df_ggl_spark)

        advertisings = df_fb_spark.union(df_ggl_spark)

        leads_data = df_pv.union(df_leads)

        leads_data = leads_data.fillna(0)

        table = advertisings.union(leads_data)

        table = Transformer().table_maker(table)

        load = Loader()
        load.connect()
        load.upload_dataframe(table)
        load.disconnect()
