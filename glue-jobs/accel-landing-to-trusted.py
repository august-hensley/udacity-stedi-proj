import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Accel-landing
Accellanding_node1727109406182 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://big-tiddy-goth-girls/accelerometer/landing/"], "recurse": True}, transformation_ctx="Accellanding_node1727109406182")

# Script generated for node customer-landing
customerlanding_node1727109409010 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://big-tiddy-goth-girls/customer/landing/"], "recurse": True}, transformation_ctx="customerlanding_node1727109409010")

# Script generated for node Join
Join_node1727110019675 = Join.apply(frame1=customerlanding_node1727109409010, frame2=Accellanding_node1727109406182, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1727110019675")

# Script generated for node select fields
SqlQuery5352 = '''
select user, timestamp, x, y, z 
from myDataSource
'''
selectfields_node1727110260199 = sparkSqlQuery(glueContext, query = SqlQuery5352, mapping = {"myDataSource":Join_node1727110019675}, transformation_ctx = "selectfields_node1727110260199")

# Script generated for node accel-trusted
acceltrusted_node1727110113143 = glueContext.getSink(path="s3://big-tiddy-goth-girls/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="acceltrusted_node1727110113143")
acceltrusted_node1727110113143.setCatalogInfo(catalogDatabase="stedidb",catalogTableName="accelerometer_trusted")
acceltrusted_node1727110113143.setFormat("json")
acceltrusted_node1727110113143.writeFrame(selectfields_node1727110260199)
job.commit()