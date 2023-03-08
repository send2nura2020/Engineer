import logging
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import col
#from prettytable import PrettyTable


logger = logging.getLogger('compare_df_parallel_demo')
formatter = logging.Formatter("%(asctime)s %(levelname)s \t[%(filename)s:%(lineno)s - %(funcName)s()] %(message)s")
logger.setLevel(logging.INFO)
io_log_handler = logging.StreamHandler()
logger.addHandler(io_log_handler)

conf = SparkConf().\
        setAll(
            [
                ("spark.driver.memory", "2g")
           ]
        )

spark = (
    SparkSession. \
    builder. \
    config(conf=conf). \
    appName("compare_df_parallel_demo"). \
    getOrCreate()
)
sc = spark.sparkContext
sqlContext = SQLContext(sc)

sc.setLogLevel('WARN')






##############################################################
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('-query','--query',help='Enter the Source Data Query?',required=True)
parser.add_argument('-tdir','--targetDir',help='Enter the Target File Path',required=True)
args=parser.parse_args()

query=args.query.lower()
targetDir=args.targetDir.lower()
print(query,targetDir)
sep='\007'









def compareGrain(src_df, tar_df, grain, logger, diff_limit=10): 
	"""
	Function to compare grain columns of tow dataframes

	Arguments:
		src_df {pyspark.sql.dataframe.DataFrame} -- Source Pyspark DataFrame
		tar_df {pyspark.sql.dataframe.DataFrame} -- Target Pyspark DataFrame
		grain {list} -- list of string containing grain
		logger {logger} -- logger

	Keyword Arguments:
		diff_limit {int} -- Number of diff records to be printed (default: {10})
	"""
	try:
		logger.info("Checking Grain...")
		
		df1 = src_df.select( grain )
		df2 = tar_df.select( grain )

		diff1 = df1.subtract(df2)
		diff1_count = diff1.count()

		diff2 = df2.subtract(df1)
		diff2_count = diff2.count()

		if diff1_count==0:
			logger.info("Source - Target : PASSED")

		else:
			diff_pdf = diff1.limit(diff_limit).toPandas()
			list_diff = diff_pdf.iloc[:].values.tolist()
			print(list_diff)
			#diff_str = pretty_print_table(list_diff, grain)['data']
			diff_str=list_diff
			logger.error("Source - Target : FAILED Diff Count : {0}\n{1}".format(diff1_count, diff_str))
			
		if diff2_count==0:
			logger.info("Target - Source : PASSED")

		else:
			diff_pdf = diff2.limit(diff_limit).toPandas()
			list_diff = diff_pdf.iloc[:].values.tolist()
			#diff_str = pretty_print_table(list_diff, grain)['data']
			diff_str=list_diff
			logger.error("Target - Source : FAILED Diff Count : {0}\n{1}".format(diff2_count,diff_str))
		
	except Exception as e:
		exception_message = "message: {0}\nline no:{1}\n".format(str(e),sys.exc_info()[2].tb_lineno)
		logger.error(exception_message)



list1 = [
     ['customer1', '13' , 1200, 12],
     ['customer1', '14' , 900, 3],
     ['customer2', '13' , 1100, 11]
 ]

list1 = [
     ['customer1', '13' , 1200, 12],
     ['customer1', '13' , 1200, 12],
     ['customer2', '13' , 1000, 11]
 ]


#header = ['customer_id', 'product_id', 'billing_amount', 'subscription_period_months']

#df1 = sqlContext.createDataFrame(sc.parallelize(list1), header).persist()
#df2 = sqlContext.createDataFrame(sc.parallelize(list2), header).persist()



#df1=spark.query(query)
df2=spark.read.option('delimiter',sep).csv(targetDir)
mapping = dict(zip(df2.columns,df1.columns))
df2=df2.select([col(c).alias(mapping.get(c, c)) for c in df2.columns])
df1.show(10)
df2.show(10)


#grain_cols = ['customer_id', 'product_id']
#grain_cols=header

grain_cols=df1.columns

compareGrain(df1, df2, grain_cols, logger)
