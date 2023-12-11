# AWS Lakehouse Solution

## Overview

This project presents a comprehensive AWS lakehouse solution, seamlessly integrating AWS Glue, S3, Python, and Spark to orchestrate the ingestion, processing, and analysis of multi-source data. The system is designed to handle data from customer interactions on the website, mobile app accelerometer readings, and IoT-based Step Trainer records. The overarching goal is to provide a robust foundation for data scientists to glean insights by creating a structured and curated data environment.

This project comes from the Udacity Nanodegree: Data Engineering with AWS.

## Project Structure

### Scripts:

**custom_landing_to_trusted.py:** Python script using Spark to sanitize and store customer records in the Trusted Zone as a Glue Table called customer_trusted.
**accelerometer_landing_to_trusted.py:** Python script using Spark to sanitize and store accelerometer readings in the Trusted Zone as a Glue Table called accelerometer_trusted.
**customer_trusted_to_curated.py:** Python script using Spark to create a curated zone Glue Table named customers_curated that includes customers with accelerometer data who agreed to share their data for research.
**step_trainer_trusted.py:** Python script using Spark to read Step Trainer IoT data stream from S3 and populate a Trusted Zone Glue Table called step_trainer_trusted.
**machine_learning_curated.py:** Python script using Spark to create an aggregated table with Step Trainer and accelerometer readings for customers who agreed to share their data, stored in a Glue Table called machine_learning_curated.

### SQL Scripts:

**customer_landing.sql:** Glue SQL script for creating a Glue Table for customer landing zone.
**accelerometer_landing.sql:** Glue SQL script for creating a Glue Table for accelerometer landing zone.

### Screenshots:

**customer_landing.png:** Screenshot of Athena query result for the customer_landing Glue Table.
**accelerometer_landing.png:** Screenshot of Athena query result for the accelerometer_landing Glue Table.
**customer_trusted.png:** Screenshot of Athena query result for the customer_trusted Glue Table.