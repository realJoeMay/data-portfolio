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

# Script generated for node Step Trainer LAnding
StepTrainerLAnding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://jm-lakehouse/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLAnding_node1",
)

# Script generated for node Customer Curated
CustomerCurated_node1682069320509 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://jm-lakehouse/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCurated_node1682069320509",
)

# Script generated for node Rename Field
RenameField_node1682069661448 = RenameField.apply(
    frame=StepTrainerLAnding_node1,
    old_name="serialNumber",
    new_name="step_serialNumber",
    transformation_ctx="RenameField_node1682069661448",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = Join.apply(
    frame1=CustomerCurated_node1682069320509,
    frame2=RenameField_node1682069661448,
    keys1=["serialNumber"],
    keys2=["step_serialNumber"],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Drop Fields
DropFields_node1682070557780 = DropFields.apply(
    frame=ApplyMapping_node2,
    paths=[
        "step_serialNumber",
        "shareWithFriendsAsOfDate",
        "phone",
        "lastUpdateDate",
        "email",
        "customerName",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "shareWithPublicAsOfDate",
        "birthDay",
    ],
    transformation_ctx="DropFields_node1682070557780",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1682070557780,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://jm-lakehouse/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
