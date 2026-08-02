# Architecting and Implementing a Resilient Global Telemetry Platform

**Name:** Somya Jaiswal
**Roll Number:** 2025EM1100101

## Project Overview
This project focuses on building a scalable big data platform for a global logistics company to process 24/7 telemetry data (engine heat, speed, location, battery efficiency) streaming from a fleet of 500,000 vehicles. The objective is to design a system capable of handling high-velocity data ingestion for both real-time monitoring and historical predictive maintenance.

--- 

## Steps to run and Terminal output 

1. docker build -t telemetry-app .
```text
(base) somyajaiswal@somyas-MacBook-Air telemetry-platform % docker build -t telemetry-app .
[+] Building 1.5s (12/12) FINISHED                                                                                                          docker:desktop-linux
 => [internal] load build definition from Dockerfile                                                                                                        0.0s
 => => transferring dockerfile: 858B                                                                                                                        0.0s
 => [internal] load metadata for docker.io/library/python:3.10-slim                                                                                         1.4s
 => [auth] library/python:pull token for registry-1.docker.io                                                                                               0.0s
 => [internal] load .dockerignore                                                                                                                           0.0s
 => => transferring context: 2B                                                                                                                             0.0s
 => [1/6] FROM docker.io/library/python:3.10-slim@sha256:c1e4e6c01eb489c422288b2de34b0761ca316f7a2d98e2c33f47659a73ed108a                                   0.0s
 => => resolve docker.io/library/python:3.10-slim@sha256:c1e4e6c01eb489c422288b2de34b0761ca316f7a2d98e2c33f47659a73ed108a                                   0.0s
 => [internal] load build context                                                                                                                           0.0s
 => => transferring context: 2.67kB                                                                                                                         0.0s
 => CACHED [2/6] RUN apt-get update &&     apt-get install -y default-jre-headless &&     apt-get clean &&     rm -rf /var/lib/apt/lists/*                  0.0s
 => CACHED [3/6] WORKDIR /app                                                                                                                               0.0s
 => CACHED [4/6] COPY requirements.txt .                                                                                                                    0.0s
 => CACHED [5/6] RUN pip install --no-cache-dir -r requirements.txt                                                                                         0.0s
 => [6/6] COPY src/ ./src/                                                                                                                                  0.0s
 => exporting to image                                                                                                                                      0.0s
 => => exporting layers                                                                                                                                     0.0s
 => => exporting manifest sha256:8f27de7ec0240322d1f735b52fdf73e3281d50a0ddabd7e1a7f64299dfcb7b10                                                           0.0s
 => => exporting config sha256:3e1c52242dd3ba00bc5f0302ff38886139096ad8140e3e7c1771ebf22d44f87e                                                             0.0s
 => => exporting attestation manifest sha256:198c4153f7c5ce3befc5d9cf66683d8a157516053921596d3dd805633462308e                                               0.0s
 => => exporting manifest list sha256:982d22629b4030fe569ca8a9e16a466abe3ef660640b26ecadfbd88b0464bc98                                                      0.0s
 => => naming to docker.io/library/telemetry-app:latest                                                                                                     0.0s
 => => unpacking to docker.io/library/telemetry-app:latest 
```

2. docker run --rm telemetry-app
```text
(base) somyajaiswal@somyas-MacBook-Air telemetry-platform % docker run --rm telemetry-app  
Data generated successfully!
/usr/local/lib/python3.10/site-packages/pyspark/bin/load-spark-env.sh: line 68: ps: command not found
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
26/08/02 07:30:17 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
+-------------+------------------+
|vehicle_model|   avg_engine_heat|
+-------------+------------------+
|   TransitVan|189.97977953260911|
|     Sprinter|189.90143479825645|
|  SemiTractor| 189.8778111724302|
| FreightLiner|189.99088236912132|
+-------------+------------------+

(base) somyajaiswal@somyas-MacBook-Air telemetry-platform % 
```



