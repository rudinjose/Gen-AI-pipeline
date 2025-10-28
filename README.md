# Cars GenAI Pipeline (S3 -> Processed -> Report)

## Overview
This repository contains an end-to-end sample pipeline that reads raw CSV files from an S3 bucket (`cars-project1/raw/`),
performs a simple transformation (drop duplicates, group by manufacturer and compute average price & mileage), writes a
processed summary CSV to `cars-project1/processed/`, and then runs a GenAI step to produce short text reports into
`cars-project1/report/`.

The project is intentionally simple and follows the coding style provided by the user (Airflow DAG with params, bash execution,
safe helpers in scripts, boto3 usage, and clear logging).

## Files included
- `airflow/dags/s3_genai_pipeline_dag.py` : Airflow DAG (start -> transform -> genai -> end)
- `scripts/transform_data.py` : Transformation script (S3 read, pandas agg, S3 write)
- `scripts/genai_report.py` : GenAI report generation (uses Hugging Face transformers if installed)
- `jenkins/Jenkinsfile` : Example Jenkins pipeline to deploy files to an EC2-based Airflow instance and trigger the DAG
- `config/requirements.txt` : Python dependencies
- `README.md` : This file

## Deployment notes (free-tier guidance)
- Use an AWS Free Tier EC2 instance (t2.micro) to host Jenkins and Airflow. Note: t2.micro has very limited CPU/RAM — running transformers on CPU will be slow.
- Create an IAM user/role for the instance with permissions to S3 (GetObject, PutObject, ListBucket) and optionally SES if you add email features.
- Set AWS credentials in the EC2 environment or via an instance profile.
- Place your raw CSV files into `s3://cars-project1/raw/`.
- Ensure the airflow `SCRIPT_PATH` in the DAG matches where you copy scripts (default in Jenkinsfile: `/opt/airflow/scripts`).

## How to run locally (quick test)
1. Create a Python venv and install requirements.
2. Set AWS credentials via environment variables or AWS CLI.
3. Run: `python scripts/transform_data.py cars-project1 raw processed`
4. Run: `python scripts/genai_report.py cars-project1 processed report`

## Important
- The GenAI step uses `google/flan-t5-base` by default. CPU inference can be very slow on a free-tier instance. Consider using a smaller model or running generation on a machine with more resources.
- This repository **does not** include any credentials. Do not hardcode secrets.
