# Use the official Airflow image
FROM apache/airflow:2.10.2

# Switch to airflow user to install python packages
USER airflow

# Copy requirements.txt into the image
COPY requirements.txt /requirements.txt

# Install the python dependencies
RUN pip install --no-cache-dir -r /requirements.txt