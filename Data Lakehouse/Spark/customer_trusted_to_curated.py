import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node customer_trusted
customer_trusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://jm-lakehouse/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customer_trusted_node1",
)

# Script generated for node acc_trusted
acc_trusted_node1682031356067 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://jm-lakehouse/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="acc_trusted_node1682031356067",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1682034806462 = ApplyMapping.apply(
    frame=acc_trusted_node1682031356067,
    mappings=[
        ("z", "double", "`(a) z`", "double"),
        ("timeStamp", "long", "`(a) timeStamp`", "long"),
        (
            "shareWithResearchAsOfDate",
            "long",
            "`(a) shareWithResearchAsOfDate`",
            "long",
        ),
        ("user", "string", "`(a) user`", "string"),
        ("y", "double", "`(a) y`", "double"),
        ("x", "double", "`(a) x`", "double"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1682034806462",
)

# Script generated for node Join
Join_node2 = Join.apply(
    frame1=customer_trusted_node1,
    frame2=RenamedkeysforJoin_node1682034806462,
    keys1=["email"],
    keys2=["`(a) user`"],
    transformation_ctx="Join_node2",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1682038717436 = DynamicFrame.fromDF(
    Join_node2.toDF().dropDuplicates(["email"]),
    glueContext,
    "DropDuplicates_node1682038717436",
)

# Script generated for node Drop Fields
DropFields_node1682038762934 = DropFields.apply(
    frame=DropDuplicates_node1682038717436,
    paths=[
        "`(a) z`",
        "`(a) timeStamp`",
        "`(a) shareWithResearchAsOfDate`",
        "`(a) user`",
        "`(a) y`",
        "`(a) x`",
    ],
    transformation_ctx="DropFields_node1682038762934",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1682038762934,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://jm-lakehouse/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
