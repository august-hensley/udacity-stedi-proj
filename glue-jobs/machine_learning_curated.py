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

# Script generated for node steps trusted
stepstrusted_node1727147368551 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://big-tiddy-goth-girls/step-trainer/trusted/"], "recurse": True}, transformation_ctx="stepstrusted_node1727147368551")

# Script generated for node Accel trusted
Acceltrusted_node1727147498341 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://big-tiddy-goth-girls/accelerometer/trusted/"], "recurse": True}, transformation_ctx="Acceltrusted_node1727147498341")

# Script generated for node SQL Query
SqlQuery5800 = '''
select *
from st
left join at
    on st.sensorreadingtime = at.timestamp
'''
SQLQuery_node1727147549090 = sparkSqlQuery(glueContext, query = SqlQuery5800, mapping = {"at":Acceltrusted_node1727147498341, "st":stepstrusted_node1727147368551}, transformation_ctx = "SQLQuery_node1727147549090")

# Script generated for node machine learn curated
machinelearncurated_node1727148006105 = glueContext.getSink(path="s3://big-tiddy-goth-girls/step-trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="machinelearncurated_node1727148006105")
machinelearncurated_node1727148006105.setCatalogInfo(catalogDatabase="stedidb",catalogTableName="machine_learning_curated")
machinelearncurated_node1727148006105.setFormat("json")
machinelearncurated_node1727148006105.writeFrame(SQLQuery_node1727147549090)
job.commit()