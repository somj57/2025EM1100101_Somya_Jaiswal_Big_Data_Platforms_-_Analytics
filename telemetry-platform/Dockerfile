# Use a lightweight Python base image
FROM python:3.10-slim

# PySpark requires Java to run the underlying Spark engine
# Update apt and install headless JRE to keep the image small
RUN apt-get update && \
    apt-get install -y default-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set the Java Home environment variable
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the source code into the container
COPY src/ ./src/

# Command to execute when the container starts:
# 1. Generate the data
# 2. Run the PySpark process
CMD ["sh", "-c", "python src/generate.py && python src/spark_process.py"]