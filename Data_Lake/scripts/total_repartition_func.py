import re
import sh

def get_hdfs_files(directory):
    output = sh.hdfs('dfs','-ls',directory).split('\n')
    files = []
    for line in output:
        match = re.search(f'({re.escape(directory)}.*$)', line)
        if match:
            files.append(match.group(0))

    return files


def get_dates(li):
    new_list = []
    for item in li[1:]:
        new_list.append(item[-10:])
    return new_list


def copy_raw_data(date,spark):
    for el in date:
        df = spark.read.parquet(f'/user/master/data/geo/events/date={el}')
        df.write\
          .format('parquet')\
          .partitionBy("event_type")\
          .mode('overwrite')\
          .save(f'/user/vasiliikus/analytics/project_7/data/events/date={el}') 