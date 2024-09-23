import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Landing Bucket
LandingBucket_node1726703877977 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://big-tiddy-goth-girls/customer/landing/"], "recurse": True}, transformation_ctx="LandingBucket_node1726703877977")

# Script generated for node Filter for trusted records
Filterfortrustedrecords_node1726703906561 = Filter.apply(frame=LandingBucket_node1726703877977, f=lambda row: (not(row["shareWithResearchAsOfDate"] == 0)), transformation_ctx="Filterfortrustedrecords_node1726703906561")

# Script generated for node Amazon S3
AmazonS3_node1726703912686 = glueContext.getSink(path="s3://big-tiddy-goth-girls/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1726703912686")
AmazonS3_node1726703912686.setCatalogInfo(catalogDatabase="stedidb",catalogTableName="data_catalog")
AmazonS3_node1726703912686.setFormat("json")
AmazonS3_node1726703912686.writeFrame(Filterfortrustedrecords_node1726703906561)
job.commit()