import os
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf

# Get input paths
input_file_path = sys.argv[1]

# Get target mobile country code
target_mcc = sys.argv[2]

# Get output index name
output_index = sys.argv[3]

# Alert before starting
print('='*50+'\n\tFilter OpenCellid by MCC\n'+'='*50+
	'\n= Input file name: '+input_file_path.split('/')[-1]+
	'\n= Target MCC: '+target_mcc+
	'\n= Output Elasticsearch index name: '+output_index+'\n')

# Pass arguments to Spark shell
SUBMIT_ARGS = '--packages org.elasticsearch:elasticsearch-spark-20_2.11:6.2.3 pyspark-shell'
os.environ['PYSPARK_SUBMIT_ARGS'] = SUBMIT_ARGS

# Create Spark context
conf = SparkConf().setAppName('MobileCountryCodeFilter').setMaster('local[*]')
sc = SparkContext(conf=conf)

# Create SQL context
sqlContext = SQLContext(sc)

# Load data
loaded_data = sqlContext.read.format('com.databricks.spark.csv').option('header', 'true').load(input_file_path)

# Filter data using Egypt's mobile country code
filter_column_name = 'mcc'
filtered_df = loaded_data[(loaded_data[filter_column_name] == target_mcc)]

# Combine coordinates data into one column
join_coordinates = udf(lambda lat,lon: ', '.join([lat,lon]))
formatted_df = filtered_df.select(join_coordinates('lat','lon').alias('location'))

# Write data to Elasticsearch
formatted_df.write.format('org.elasticsearch.spark.sql').save(output_index+'/doc')

# Alert upon completion
print('Filtering process completed successfully. Exiting..')

# Exit gracefully
exit(0)
