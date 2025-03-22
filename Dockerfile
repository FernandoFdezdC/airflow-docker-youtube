# Use the base image of Apache Airflow
FROM apache/airflow:2.10.4

# Install necessary system dependencies (optional)
# For example: if you need command-line tools or system libraries
USER root
RUN apt-get update && apt-get install -y \
    curl \
    vim \
    && apt-get clean

# Optional configuration: change to airflow user
USER airflow

# Copy the current directory contents into the container at /tmp
COPY requirements.txt /tmp

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt