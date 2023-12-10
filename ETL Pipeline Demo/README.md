# ETL Pipeline Demonstration

## Overview

This project demonstrates a simple ETL (Extract, Transform, Load) pipeline using Apache Airflow. The pipeline takes data from S3 files, stages them in AWS Redshift, and transforms the data into a star database format. The goal is to provide a scalable and efficient solution for managing and processing large datasets.

This project comes from the Udacity Nanodegree: Data Engineering with AWS.

## Components

### 1. Apache Airflow
Apache Airflow is used as the orchestrator for the ETL pipeline. It allows you to define, schedule, and monitor workflows as directed acyclic graphs (DAGs).

### 2. Amazon S3
Amazon S3 is used as the source for raw data files. The pipeline retrieves these files for further processing.

### 3. AWS Redshift
AWS Redshift serves as the data warehouse where staged data is loaded and transformed into a star database format.