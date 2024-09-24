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

# Script generated for node step-trainer-landing
steptrainerlanding_node1727143940950 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://big-tiddy-goth-girls/step-trainer/landing/"], "recurse": True}, transformation_ctx="steptrainerlanding_node1727143940950")

# Script generated for node customer-curated
customercurated_node1727144791177 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://big-tiddy-goth-girls/customer/curated/"], "recurse": True}, transformation_ctx="customercurated_node1727144791177")

# Script generated for node Join SQL
SqlQuery4945 = '''
select * 
from stl
where stl.serialnumber in 
    (select distinct serialnumber from cc)

'''
JoinSQL_node1727144849759 = sparkSqlQuery(glueContext, query = SqlQuery4945, mapping = {"cc":customercurated_node1727144791177, "stl":steptrainerlanding_node1727143940950}, transformation_ctx = "JoinSQL_node1727144849759")

# Script generated for node step trainer trusted
steptrainertrusted_node1727145186481 = glueContext.getSink(path="s3://big-tiddy-goth-girls/step-trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="steptrainertrusted_node1727145186481")
steptrainertrusted_node1727145186481.setCatalogInfo(catalogDatabase="stedidb",catalogTableName="step_trainer_trusted")
steptrainertrusted_node1727145186481.setFormat("json")
steptrainertrusted_node1727145186481.writeFrame(JoinSQL_node1727144849759)
job.commit()