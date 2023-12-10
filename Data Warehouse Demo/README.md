# Data Warhouse Demonstration

A music streaming startup, Sparkify, wants to move their processes and data onto the cloud.

The goal of this project is to build an ETL pipeline that extracts their music streaming data from S3, stages them in Redshift, and transforms the data into dimensional tables. Sparkify's analytics team will be able to use the data to find insights into what songs their users are listening to, enhance their recomendation engine, or explore other analytical initiatives.

This project comes from the Udacity Nanodegree: Data Engineering with AWS.

# How To Run
Add configuration data to dwh.cfg
Create the IAM Role and Redshift cluster with redshift.ipynb (or in AWS Console)
Run create_tables.py
Run etl.py

# Explanation Of Files

**create_tables.py** creates new tables in AWS Redshift for staging and analytics.

**etl.py** extracts the streaming and song data from S3, loads into the staging tables, and transforms the data into dimensional data into the final analytics tables.

**sql_queries.py** stores all the queries needed for create_tables.py and etl.py.

**dwh.cfg** contains configuration data for AWS, the Redshift Cluster, and the Redshift Database.

**redshift.ipynb** creates the IAM role and Redshift cluster that will hold the new database tables.