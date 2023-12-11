import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Trusted
CustomerTrusted_node1682029299591 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://jm-lakehouse/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1682029299591",
)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://jm-lakehouse/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node1",
)

# Script generated for node Join Data
JoinData_node2 = Join.apply(
    frame1=AccelerometerLanding_node1,
    frame2=CustomerTrusted_node1682029299591,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="JoinData_node2",
)

# Script generated for node Drop Fields
DropFields_node1682029592230 = DropFields.apply(
    frame=JoinData_node2,
    paths=[
        "serialNumber",
        "birthDay",
        "shareWithPublicAsOfDate",
        "registrationDate",
        "customerName",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
        "email",
    ],
    transformation_ctx="DropFields_node1682029592230",
)

# Script generated for node Acceleromter Trusted
AcceleromterTrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1682029592230,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://jm-lakehouse/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AcceleromterTrusted_node3",
)

job.commit()
