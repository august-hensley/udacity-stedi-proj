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

# Script generated for node customer trusted
customertrusted_node1727109409010 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://big-tiddy-goth-girls/customer/trusted/"], "recurse": True}, transformation_ctx="customertrusted_node1727109409010")

# Script generated for node select fields and join
SqlQuery2234 = '''
select customername, 
    email, 
    phone, 
    birthday, 
    serialnumber, 
    registrationdate, 
    lastupdatedate, 
    sharewithresearchasofdate, 
    sharewithpublicasofdate, 
    sharewithfriendsasofdate 
from ct
where email in (select distinct user from al)

'''
selectfieldsandjoin_node1727110260199 = sparkSqlQuery(glueContext, query = SqlQuery2234, mapping = {"ct":customertrusted_node1727109409010, "al":Accellanding_node1727109406182}, transformation_ctx = "selectfieldsandjoin_node1727110260199")

# Script generated for node customer curated
customercurated_node1727110113143 = glueContext.getSink(path="s3://big-tiddy-goth-girls/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customercurated_node1727110113143")
customercurated_node1727110113143.setCatalogInfo(catalogDatabase="stedidb",catalogTableName="customer_curated")
customercurated_node1727110113143.setFormat("json")
customercurated_node1727110113143.writeFrame(selectfieldsandjoin_node1727110260199)
job.commit()