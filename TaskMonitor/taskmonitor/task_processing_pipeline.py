import sys
import os
import time
from luigi.mock import MockFile
import luigi
import csv
import pandas as pd


class CopyFileFromNetwork(luigi.Task):
    """
    SimpleTask taht gets file from network!.
    """

    def output(self):
        return MockFile("File Copied Successfully!", mirror_on_stderr=True)

    def run(self):
        os.system("scp -r -i Test_WPT_Server_Virginiapem.pem ubuntu@ec2-52-90-188-162.compute-1.amazonaws.com:/tmp/domain_information.csv ~")
        _out = self.output().open('w')
        _out.write(u"File Copied!\n")
        _out.close()


class PopulateDataTask(luigi.Task):
    """
    Populates the Data from CSV File
    """
    def output(self):
        return MockFile("PopulateDataTask", mirror_on_stderr=True)

    def requires(self):
        return CopyFileFromNetwork()

    def run(self):
        with open('/Users/rokumar/domain_information.csv') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                print row['DOMAIN_NAME'], row['CDN']

                domain_id = row['DOMAIN_ID']
                domain_name = row['DOMAIN_NAME']
                timestamp = row['TIMESTAMP']
                name = row['NAME']
                cdn = row['CDN']
                ttl = row['TTL']
                typ = row['TYPE']
                val = row['VALUE']
                dt_yr = row['DT_YEAR']
                dt_month = row['DT_MONTH']
                dt_day = row['DT_DAY']
                dt_hour = row['DT_HOUR']
                dt_min = row['DT_MIN']
                dt_sec = row['DT_SEC']
                weight = row['WEIGHT']
        _out = self.output().open('w')
        _out.write(u"Data Saved!\n")
        _out.close()

class GetInitialAggregate(luigi.Task):
    """
    Gets the initial aggregate from the data.
    """

    def output(self):
        return MockFile("GetAggregateData", mirror_on_stderr=True)
    
    def requires(self):
        return PopulateDataTask()

    def run(self):
        raise ValueError('Cannot convert intger to float')

class AggregateDataBySumOfTTL(luigi.Task):
    """
    Gets the final aggregate from the data i.e. count of CDN.
    """
    def output(self):
        return MockFile("GetAggregateData", mirror_on_stderr=True)
    
    def requires(self):
        return GetInitialAggregate()

    def run(self):
        df = pd.read_csv('/Users/rokumar/domain_information.csv')
        df1 = df[['TTL', 'TIMESTAMP']]
        df1.columns = ['ttl', 'ttime']
        time.sleep(15)
        result_aggregate = df1.groupby(df1['ttime']).apply(lambda x:x.ttl.sum())
        _out = self.output().open('w')
        _out.write(u"Aggregate generated!\n")
        _out.close()
                    
class DailyReportGeneration(luigi.Task):
    def output(self):
        return MockFile("GetAggregateData", mirror_on_stderr=True)
    
    def requires(self):
        return AggregateDataBySumOfTTL()

    def run(self):
        time.sleep(25)
        _out = self.output().open('w')
        _out.write(u"Report generated!\n")
        _out.close()
