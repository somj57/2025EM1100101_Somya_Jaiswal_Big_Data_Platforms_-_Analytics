# Architecting and Implementing a Resilient Global Telemetry Platform

**Name:** Somya Jaiswal
**Roll Number:** 2025EM1100101

## Project Overview
This project focuses on building a scalable big data platform for a global logistics company to process 24/7 telemetry data (engine heat, speed, location, battery efficiency) streaming from a fleet of 500,000 vehicles. The objective is to design a system capable of handling high-velocity data ingestion for both real-time monitoring and historical predictive maintenance.

--- 
## Steps to run and Terminal output 

1. docker build -t telemetry-spark-env .
```text
(base) somyajaiswal@somyas-MacBook-Air telemetry-platform % docker build -t telemetry-spark-env .
[+] Building 179.3s (12/12) FINISHED                                                                        docker:desktop-linux
 => [internal] load build definition from Dockerfile                                                                        0.0s
 => => transferring dockerfile: 749B                                                                                        0.0s
 => [internal] load metadata for docker.io/library/python:3.10-slim                                                         5.4s
 => [auth] library/python:pull token for registry-1.docker.io                                                               0.0s
 => [internal] load .dockerignore                                                                                           0.0s
 => => transferring context: 2B                                                                                             0.0s
 => [1/6] FROM docker.io/library/python:3.10-slim@sha256:c1e4e6c01eb489c422288b2de34b0761ca316f7a2d98e2c33f47659a73ed108a  13.6s
 => => resolve docker.io/library/python:3.10-slim@sha256:c1e4e6c01eb489c422288b2de34b0761ca316f7a2d98e2c33f47659a73ed108a   0.0s
 => => sha256:57bf3492848ac60149e44b169cd0c31a1fc53b7bec02e991c9b0fa71c79c4f1c 249B / 249B                                  0.3s
 => => sha256:59761324ffa24baf777953d8c6d386a99078c7b21877c050e6cb7331736d1076 13.83MB / 13.83MB                           13.4s
 => => sha256:6e7c7a17d8378f8f2e82927029141db51b8c33a78e9487006a7ff9164442398f 1.27MB / 1.27MB                              2.7s
 => => sha256:59f54fbcd984beca03fd8b78569fa57268ecf78d291c0b6fe1623c2467f5a075 30.14MB / 30.14MB                           11.9s
 => => extracting sha256:59f54fbcd984beca03fd8b78569fa57268ecf78d291c0b6fe1623c2467f5a075                                   0.3s
 => => extracting sha256:6e7c7a17d8378f8f2e82927029141db51b8c33a78e9487006a7ff9164442398f                                   0.0s
 => => extracting sha256:59761324ffa24baf777953d8c6d386a99078c7b21877c050e6cb7331736d1076                                   0.2s
 => => extracting sha256:57bf3492848ac60149e44b169cd0c31a1fc53b7bec02e991c9b0fa71c79c4f1c                                   0.0s
 => [internal] load build context                                                                                           0.0s
 => => transferring context: 5.00kB                                                                                         0.0s
 => [2/6] RUN apt-get update &&     apt-get install -y default-jre-headless &&     apt-get clean &&     rm -rf /var/lib/a  21.8s
 => [3/6] WORKDIR /app                                                                                                      0.0s 
 => [4/6] COPY requirements.txt .                                                                                           0.0s 
 => [5/6] RUN pip install --no-cache-dir -r requirements.txt                                                              128.8s 
 => [6/6] COPY . .                                                                                                          0.1s 
 => exporting to image                                                                                                      9.5s 
 => => exporting layers                                                                                                     7.2s 
 => => exporting manifest sha256:e95e1487db78279d2ad101e676a9900c5d6036013a6790f94736a4ed7ecf2f0c                           0.0s 
 => => exporting config sha256:ccc22d02c9c01e72fbb34e36fe9e640ae6ed27b9e6be41cd374c253068b93a45                             0.0s 
 => => exporting attestation manifest sha256:f72eee782a9eacf1ab657d4d4f68094759a7b5408ce4d0973f15db7d3235c790               0.0s 
 => => exporting manifest list sha256:e578dd966961850d562d8d628e3728501b776cb32052bb0bd0e70eec3be8603b                      0.0s
 => => naming to docker.io/library/telemetry-spark-env:latest                                                               0.0s
 => => unpacking to docker.io/library/telemetry-spark-env:latest
```

2. docker run -it -v "$(pwd):/app" telemetry-spark-env
```text 
(base) somyajaiswal@somyas-MacBook-Air telemetry-platform % docker run -it -v "$(pwd):/app" telemetry-spark-env
root@819c60c13514:/app#
```

3. spark-submit src/process.py
```text
root@819c60c13514:/app# spark-submit src/process.py
/usr/local/lib/python3.10/site-packages/pyspark/bin/load-spark-env.sh: line 68: ps: command not found
26/08/02 07:08:01 INFO SparkContext: Running Spark version 3.5.0
26/08/02 07:08:01 INFO SparkContext: OS info Linux, 6.12.76-linuxkit, aarch64
26/08/02 07:08:01 INFO SparkContext: Java version 21.0.11
26/08/02 07:08:01 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
26/08/02 07:08:01 INFO ResourceUtils: ==============================================================
26/08/02 07:08:01 INFO ResourceUtils: No custom resources configured for spark.driver.
26/08/02 07:08:01 INFO ResourceUtils: ==============================================================
26/08/02 07:08:01 INFO SparkContext: Submitted application: Resilient_Global_Telemetry_Platform
26/08/02 07:08:01 INFO ResourceProfile: Default ResourceProfile created, executor resources: Map(cores -> name: cores, amount: 1, script: , vendor: , memory -> name: memory, amount: 1024, script: , vendor: , offHeap -> name: offHeap, amount: 0, script: , vendor: ), task resources: Map(cpus -> name: cpus, amount: 1.0)
26/08/02 07:08:01 INFO ResourceProfile: Limiting resource is cpu
26/08/02 07:08:01 INFO ResourceProfileManager: Added ResourceProfile id: 0
26/08/02 07:08:01 INFO SecurityManager: Changing view acls to: root
26/08/02 07:08:01 INFO SecurityManager: Changing modify acls to: root
26/08/02 07:08:01 INFO SecurityManager: Changing view acls groups to: 
26/08/02 07:08:01 INFO SecurityManager: Changing modify acls groups to: 
26/08/02 07:08:01 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users with view permissions: root; groups with view permissions: EMPTY; users with modify permissions: root; groups with modify permissions: EMPTY
26/08/02 07:08:01 INFO Utils: Successfully started service 'sparkDriver' on port 43389.
26/08/02 07:08:01 INFO SparkEnv: Registering MapOutputTracker
26/08/02 07:08:01 INFO SparkEnv: Registering BlockManagerMaster
26/08/02 07:08:01 INFO BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
26/08/02 07:08:01 INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
26/08/02 07:08:01 INFO SparkEnv: Registering BlockManagerMasterHeartbeat
26/08/02 07:08:01 INFO DiskBlockManager: Created local directory at /tmp/blockmgr-4ea7d6a8-da34-496d-b795-099d14f6cdf1
26/08/02 07:08:01 INFO MemoryStore: MemoryStore started with capacity 434.4 MiB
26/08/02 07:08:01 INFO SparkEnv: Registering OutputCommitCoordinator
26/08/02 07:08:01 INFO JettyUtils: Start Jetty 0.0.0.0:4040 for SparkUI
26/08/02 07:08:01 INFO Utils: Successfully started service 'SparkUI' on port 4040.
26/08/02 07:08:01 INFO Executor: Starting executor ID driver on host 819c60c13514
26/08/02 07:08:01 INFO Executor: OS info Linux, 6.12.76-linuxkit, aarch64
26/08/02 07:08:01 INFO Executor: Java version 21.0.11
26/08/02 07:08:01 INFO Executor: Starting executor with user classpath (userClassPathFirst = false): ''
26/08/02 07:08:01 INFO Executor: Created or updated repl class loader org.apache.spark.util.MutableURLClassLoader@28f33c0b for default.
26/08/02 07:08:01 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 44573.
26/08/02 07:08:01 INFO NettyBlockTransferService: Server created on 819c60c13514:44573
26/08/02 07:08:01 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
26/08/02 07:08:01 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, 819c60c13514, 44573, None)
26/08/02 07:08:01 INFO BlockManagerMasterEndpoint: Registering block manager 819c60c13514:44573 with 434.4 MiB RAM, BlockManagerId(driver, 819c60c13514, 44573, None)
26/08/02 07:08:01 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, 819c60c13514, 44573, None)
26/08/02 07:08:01 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, 819c60c13514, 44573, None)
--- Starting Telemetry Processing Pipeline ---
Generating mock telemetry data...
26/08/02 07:08:01 INFO SharedState: Setting hive.metastore.warehouse.dir ('null') to the value of spark.sql.warehouse.dir.
26/08/02 07:08:01 INFO SharedState: Warehouse path is 'file:/app/spark-warehouse'.
Final Aggregated Data (Action triggered):
26/08/02 07:08:02 INFO CodeGenerator: Code generated in 79.574916 ms
26/08/02 07:08:02 INFO DAGScheduler: Registering RDD 6 (showString at NativeMethodAccessorImpl.java:0) as input to shuffle 0
26/08/02 07:08:02 INFO DAGScheduler: Got map stage job 0 (showString at NativeMethodAccessorImpl.java:0) with 10 output partitions
26/08/02 07:08:02 INFO DAGScheduler: Final stage: ShuffleMapStage 0 (showString at NativeMethodAccessorImpl.java:0)
26/08/02 07:08:02 INFO DAGScheduler: Parents of final stage: List()
26/08/02 07:08:02 INFO DAGScheduler: Missing parents: List()
26/08/02 07:08:02 INFO DAGScheduler: Submitting ShuffleMapStage 0 (MapPartitionsRDD[6] at showString at NativeMethodAccessorImpl.java:0), which has no missing parents
26/08/02 07:08:02 INFO MemoryStore: Block broadcast_0 stored as values in memory (estimated size 44.4 KiB, free 434.4 MiB)
26/08/02 07:08:02 INFO MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 20.4 KiB, free 434.3 MiB)
26/08/02 07:08:02 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on 819c60c13514:44573 (size: 20.4 KiB, free: 434.4 MiB)
26/08/02 07:08:02 INFO SparkContext: Created broadcast 0 from broadcast at DAGScheduler.scala:1580
26/08/02 07:08:02 INFO DAGScheduler: Submitting 10 missing tasks from ShuffleMapStage 0 (MapPartitionsRDD[6] at showString at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
26/08/02 07:08:02 INFO TaskSchedulerImpl: Adding task set 0.0 with 10 tasks resource profile 0
26/08/02 07:08:02 INFO TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0) (819c60c13514, executor driver, partition 0, PROCESS_LOCAL, 8548 bytes) 
26/08/02 07:08:02 INFO TaskSetManager: Starting task 1.0 in stage 0.0 (TID 1) (819c60c13514, executor driver, partition 1, PROCESS_LOCAL, 8548 bytes) 
26/08/02 07:08:02 INFO TaskSetManager: Starting task 2.0 in stage 0.0 (TID 2) (819c60c13514, executor driver, partition 2, PROCESS_LOCAL, 8548 bytes) 
26/08/02 07:08:02 INFO TaskSetManager: Starting task 3.0 in stage 0.0 (TID 3) (819c60c13514, executor driver, partition 3, PROCESS_LOCAL, 8548 bytes) 
26/08/02 07:08:02 INFO TaskSetManager: Starting task 4.0 in stage 0.0 (TID 4) (819c60c13514, executor driver, partition 4, PROCESS_LOCAL, 8548 bytes) 
26/08/02 07:08:02 INFO TaskSetManager: Starting task 5.0 in stage 0.0 (TID 5) (819c60c13514, executor driver, partition 5, PROCESS_LOCAL, 8548 bytes) 
26/08/02 07:08:02 INFO TaskSetManager: Starting task 6.0 in stage 0.0 (TID 6) (819c60c13514, executor driver, partition 6, PROCESS_LOCAL, 8548 bytes) 
26/08/02 07:08:02 INFO TaskSetManager: Starting task 7.0 in stage 0.0 (TID 7) (819c60c13514, executor driver, partition 7, PROCESS_LOCAL, 8548 bytes) 
26/08/02 07:08:02 INFO TaskSetManager: Starting task 8.0 in stage 0.0 (TID 8) (819c60c13514, executor driver, partition 8, PROCESS_LOCAL, 8548 bytes) 
26/08/02 07:08:02 INFO TaskSetManager: Starting task 9.0 in stage 0.0 (TID 9) (819c60c13514, executor driver, partition 9, PROCESS_LOCAL, 8548 bytes) 
26/08/02 07:08:02 INFO Executor: Running task 0.0 in stage 0.0 (TID 0)
26/08/02 07:08:02 INFO Executor: Running task 5.0 in stage 0.0 (TID 5)
26/08/02 07:08:02 INFO Executor: Running task 7.0 in stage 0.0 (TID 7)
26/08/02 07:08:02 INFO Executor: Running task 1.0 in stage 0.0 (TID 1)
26/08/02 07:08:02 INFO Executor: Running task 2.0 in stage 0.0 (TID 2)
26/08/02 07:08:02 INFO Executor: Running task 9.0 in stage 0.0 (TID 9)
26/08/02 07:08:02 INFO Executor: Running task 8.0 in stage 0.0 (TID 8)
26/08/02 07:08:02 INFO Executor: Running task 4.0 in stage 0.0 (TID 4)
26/08/02 07:08:02 INFO Executor: Running task 3.0 in stage 0.0 (TID 3)
26/08/02 07:08:02 INFO Executor: Running task 6.0 in stage 0.0 (TID 6)
26/08/02 07:08:03 INFO CodeGenerator: Code generated in 67.715875 ms
26/08/02 07:08:03 INFO CodeGenerator: Code generated in 18.640958 ms
26/08/02 07:08:03 INFO CodeGenerator: Code generated in 14.963417 ms
26/08/02 07:08:03 INFO CodeGenerator: Code generated in 9.4665 ms
26/08/02 07:08:03 INFO CodeGenerator: Code generated in 12.6975 ms
26/08/02 07:08:03 INFO PythonRunner: Times: total = 399, boot = 263, init = 136, finish = 0
26/08/02 07:08:03 INFO PythonRunner: Times: total = 399, boot = 278, init = 120, finish = 1
26/08/02 07:08:03 INFO PythonRunner: Times: total = 426, boot = 275, init = 148, finish = 3
26/08/02 07:08:03 INFO PythonRunner: Times: total = 433, boot = 294, init = 139, finish = 0
26/08/02 07:08:03 INFO PythonRunner: Times: total = 422, boot = 310, init = 112, finish = 0
26/08/02 07:08:03 INFO PythonRunner: Times: total = 355, boot = 257, init = 98, finish = 0
26/08/02 07:08:03 INFO PythonRunner: Times: total = 346, boot = 259, init = 86, finish = 1
26/08/02 07:08:03 INFO PythonRunner: Times: total = 381, boot = 285, init = 96, finish = 0
26/08/02 07:08:03 INFO PythonRunner: Times: total = 383, boot = 267, init = 116, finish = 0
26/08/02 07:08:03 INFO PythonRunner: Times: total = 369, boot = 257, init = 112, finish = 0
26/08/02 07:08:03 INFO Executor: Finished task 3.0 in stage 0.0 (TID 3). 2785 bytes result sent to driver
26/08/02 07:08:03 INFO Executor: Finished task 2.0 in stage 0.0 (TID 2). 2785 bytes result sent to driver
26/08/02 07:08:03 INFO Executor: Finished task 5.0 in stage 0.0 (TID 5). 2828 bytes result sent to driver
26/08/02 07:08:03 INFO Executor: Finished task 0.0 in stage 0.0 (TID 0). 2785 bytes result sent to driver
26/08/02 07:08:03 INFO Executor: Finished task 4.0 in stage 0.0 (TID 4). 2785 bytes result sent to driver
26/08/02 07:08:03 INFO Executor: Finished task 8.0 in stage 0.0 (TID 8). 2785 bytes result sent to driver
26/08/02 07:08:03 INFO Executor: Finished task 6.0 in stage 0.0 (TID 6). 2785 bytes result sent to driver
26/08/02 07:08:03 INFO Executor: Finished task 1.0 in stage 0.0 (TID 1). 2785 bytes result sent to driver
26/08/02 07:08:03 INFO Executor: Finished task 7.0 in stage 0.0 (TID 7). 2828 bytes result sent to driver
26/08/02 07:08:03 INFO Executor: Finished task 9.0 in stage 0.0 (TID 9). 2828 bytes result sent to driver
26/08/02 07:08:03 INFO TaskSetManager: Finished task 3.0 in stage 0.0 (TID 3) in 592 ms on 819c60c13514 (executor driver) (1/10)
26/08/02 07:08:03 INFO TaskSetManager: Finished task 2.0 in stage 0.0 (TID 2) in 593 ms on 819c60c13514 (executor driver) (2/10)
26/08/02 07:08:03 INFO TaskSetManager: Finished task 6.0 in stage 0.0 (TID 6) in 592 ms on 819c60c13514 (executor driver) (3/10)
26/08/02 07:08:03 INFO TaskSetManager: Finished task 8.0 in stage 0.0 (TID 8) in 592 ms on 819c60c13514 (executor driver) (4/10)
26/08/02 07:08:03 INFO PythonAccumulatorV2: Connected to AccumulatorServer at host: 127.0.0.1 port: 36267
26/08/02 07:08:03 INFO TaskSetManager: Finished task 1.0 in stage 0.0 (TID 1) in 595 ms on 819c60c13514 (executor driver) (5/10)
26/08/02 07:08:03 INFO TaskSetManager: Finished task 5.0 in stage 0.0 (TID 5) in 595 ms on 819c60c13514 (executor driver) (6/10)
26/08/02 07:08:03 INFO TaskSetManager: Finished task 7.0 in stage 0.0 (TID 7) in 594 ms on 819c60c13514 (executor driver) (7/10)
26/08/02 07:08:03 INFO TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 600 ms on 819c60c13514 (executor driver) (8/10)
26/08/02 07:08:03 INFO TaskSetManager: Finished task 4.0 in stage 0.0 (TID 4) in 597 ms on 819c60c13514 (executor driver) (9/10)
26/08/02 07:08:03 INFO TaskSetManager: Finished task 9.0 in stage 0.0 (TID 9) in 596 ms on 819c60c13514 (executor driver) (10/10)
26/08/02 07:08:03 INFO TaskSchedulerImpl: Removed TaskSet 0.0, whose tasks have all completed, from pool 
26/08/02 07:08:03 INFO DAGScheduler: ShuffleMapStage 0 (showString at NativeMethodAccessorImpl.java:0) finished in 0.642 s
26/08/02 07:08:03 INFO DAGScheduler: looking for newly runnable stages
26/08/02 07:08:03 INFO DAGScheduler: running: Set()
26/08/02 07:08:03 INFO DAGScheduler: waiting: Set()
26/08/02 07:08:03 INFO DAGScheduler: failed: Set()
26/08/02 07:08:03 INFO ShufflePartitionsUtil: For shuffle(0), advisory target size: 67108864, actual target size 1048576, minimum partition size: 1048576
26/08/02 07:08:03 INFO HashAggregateExec: spark.sql.codegen.aggregate.map.twolevel.enabled is set to true, but current version of codegened fast hashmap does not support this aggregate.
26/08/02 07:08:03 INFO CodeGenerator: Code generated in 8.663791 ms
26/08/02 07:08:03 INFO DAGScheduler: Registering RDD 9 (showString at NativeMethodAccessorImpl.java:0) as input to shuffle 1
26/08/02 07:08:03 INFO DAGScheduler: Got map stage job 1 (showString at NativeMethodAccessorImpl.java:0) with 1 output partitions
26/08/02 07:08:03 INFO DAGScheduler: Final stage: ShuffleMapStage 2 (showString at NativeMethodAccessorImpl.java:0)
26/08/02 07:08:03 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 1)
26/08/02 07:08:03 INFO DAGScheduler: Missing parents: List()
26/08/02 07:08:03 INFO DAGScheduler: Submitting ShuffleMapStage 2 (MapPartitionsRDD[9] at showString at NativeMethodAccessorImpl.java:0), which has no missing parents
26/08/02 07:08:03 INFO MemoryStore: Block broadcast_1 stored as values in memory (estimated size 46.4 KiB, free 434.3 MiB)
26/08/02 07:08:03 INFO MemoryStore: Block broadcast_1_piece0 stored as bytes in memory (estimated size 21.7 KiB, free 434.3 MiB)
26/08/02 07:08:03 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on 819c60c13514:44573 (size: 21.7 KiB, free: 434.4 MiB)
26/08/02 07:08:03 INFO SparkContext: Created broadcast 1 from broadcast at DAGScheduler.scala:1580
26/08/02 07:08:03 INFO DAGScheduler: Submitting 1 missing tasks from ShuffleMapStage 2 (MapPartitionsRDD[9] at showString at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0))
26/08/02 07:08:03 INFO TaskSchedulerImpl: Adding task set 2.0 with 1 tasks resource profile 0
26/08/02 07:08:03 INFO TaskSetManager: Starting task 0.0 in stage 2.0 (TID 10) (819c60c13514, executor driver, partition 0, NODE_LOCAL, 7604 bytes) 
26/08/02 07:08:03 INFO Executor: Running task 0.0 in stage 2.0 (TID 10)
26/08/02 07:08:03 INFO ShuffleBlockFetcherIterator: Getting 10 (15.1 KiB) non-empty blocks including 10 (15.1 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/08/02 07:08:03 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 3 ms
26/08/02 07:08:03 INFO CodeGenerator: Code generated in 6.213625 ms
26/08/02 07:08:03 INFO CodeGenerator: Code generated in 3.110167 ms
26/08/02 07:08:03 INFO Executor: Finished task 0.0 in stage 2.0 (TID 10). 5441 bytes result sent to driver
26/08/02 07:08:03 INFO TaskSetManager: Finished task 0.0 in stage 2.0 (TID 10) in 35 ms on 819c60c13514 (executor driver) (1/1)
26/08/02 07:08:03 INFO TaskSchedulerImpl: Removed TaskSet 2.0, whose tasks have all completed, from pool 
26/08/02 07:08:03 INFO DAGScheduler: ShuffleMapStage 2 (showString at NativeMethodAccessorImpl.java:0) finished in 0.040 s
26/08/02 07:08:03 INFO DAGScheduler: looking for newly runnable stages
26/08/02 07:08:03 INFO DAGScheduler: running: Set()
26/08/02 07:08:03 INFO DAGScheduler: waiting: Set()
26/08/02 07:08:03 INFO DAGScheduler: failed: Set()
26/08/02 07:08:03 INFO HashAggregateExec: spark.sql.codegen.aggregate.map.twolevel.enabled is set to true, but current version of codegened fast hashmap does not support this aggregate.
26/08/02 07:08:03 INFO BlockManagerInfo: Removed broadcast_1_piece0 on 819c60c13514:44573 in memory (size: 21.7 KiB, free: 434.4 MiB)
26/08/02 07:08:03 INFO CodeGenerator: Code generated in 13.581542 ms
26/08/02 07:08:03 INFO SparkContext: Starting job: showString at NativeMethodAccessorImpl.java:0
26/08/02 07:08:03 INFO DAGScheduler: Got job 2 (showString at NativeMethodAccessorImpl.java:0) with 10 output partitions
26/08/02 07:08:03 INFO DAGScheduler: Final stage: ResultStage 5 (showString at NativeMethodAccessorImpl.java:0)
26/08/02 07:08:03 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 4)
26/08/02 07:08:03 INFO DAGScheduler: Missing parents: List()
26/08/02 07:08:03 INFO DAGScheduler: Submitting ResultStage 5 (MapPartitionsRDD[12] at showString at NativeMethodAccessorImpl.java:0), which has no missing parents
26/08/02 07:08:03 INFO MemoryStore: Block broadcast_2 stored as values in memory (estimated size 67.6 KiB, free 434.3 MiB)
26/08/02 07:08:03 INFO MemoryStore: Block broadcast_2_piece0 stored as bytes in memory (estimated size 28.4 KiB, free 434.2 MiB)
26/08/02 07:08:03 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on 819c60c13514:44573 (size: 28.4 KiB, free: 434.4 MiB)
26/08/02 07:08:03 INFO SparkContext: Created broadcast 2 from broadcast at DAGScheduler.scala:1580
26/08/02 07:08:03 INFO DAGScheduler: Submitting 10 missing tasks from ResultStage 5 (MapPartitionsRDD[12] at showString at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
26/08/02 07:08:03 INFO TaskSchedulerImpl: Adding task set 5.0 with 10 tasks resource profile 0
26/08/02 07:08:03 INFO TaskSetManager: Starting task 6.0 in stage 5.0 (TID 11) (819c60c13514, executor driver, partition 6, NODE_LOCAL, 7615 bytes) 
26/08/02 07:08:03 INFO TaskSetManager: Starting task 9.0 in stage 5.0 (TID 12) (819c60c13514, executor driver, partition 9, NODE_LOCAL, 7615 bytes) 
26/08/02 07:08:03 INFO TaskSetManager: Starting task 0.0 in stage 5.0 (TID 13) (819c60c13514, executor driver, partition 0, PROCESS_LOCAL, 7615 bytes) 
26/08/02 07:08:03 INFO TaskSetManager: Starting task 1.0 in stage 5.0 (TID 14) (819c60c13514, executor driver, partition 1, PROCESS_LOCAL, 7615 bytes) 
26/08/02 07:08:03 INFO TaskSetManager: Starting task 2.0 in stage 5.0 (TID 15) (819c60c13514, executor driver, partition 2, PROCESS_LOCAL, 7615 bytes) 
26/08/02 07:08:03 INFO TaskSetManager: Starting task 3.0 in stage 5.0 (TID 16) (819c60c13514, executor driver, partition 3, PROCESS_LOCAL, 7615 bytes) 
26/08/02 07:08:03 INFO TaskSetManager: Starting task 4.0 in stage 5.0 (TID 17) (819c60c13514, executor driver, partition 4, PROCESS_LOCAL, 7615 bytes) 
26/08/02 07:08:03 INFO TaskSetManager: Starting task 5.0 in stage 5.0 (TID 18) (819c60c13514, executor driver, partition 5, PROCESS_LOCAL, 7615 bytes) 
26/08/02 07:08:03 INFO TaskSetManager: Starting task 7.0 in stage 5.0 (TID 19) (819c60c13514, executor driver, partition 7, PROCESS_LOCAL, 7615 bytes) 
26/08/02 07:08:03 INFO TaskSetManager: Starting task 8.0 in stage 5.0 (TID 20) (819c60c13514, executor driver, partition 8, PROCESS_LOCAL, 7615 bytes) 
26/08/02 07:08:03 INFO Executor: Running task 4.0 in stage 5.0 (TID 17)
26/08/02 07:08:03 INFO Executor: Running task 3.0 in stage 5.0 (TID 16)
26/08/02 07:08:03 INFO Executor: Running task 0.0 in stage 5.0 (TID 13)
26/08/02 07:08:03 INFO Executor: Running task 6.0 in stage 5.0 (TID 11)
26/08/02 07:08:03 INFO Executor: Running task 9.0 in stage 5.0 (TID 12)
26/08/02 07:08:03 INFO Executor: Running task 1.0 in stage 5.0 (TID 14)
26/08/02 07:08:03 INFO Executor: Running task 8.0 in stage 5.0 (TID 20)
26/08/02 07:08:03 INFO Executor: Running task 5.0 in stage 5.0 (TID 18)
26/08/02 07:08:03 INFO Executor: Running task 7.0 in stage 5.0 (TID 19)
26/08/02 07:08:03 INFO Executor: Running task 2.0 in stage 5.0 (TID 15)
26/08/02 07:08:03 INFO ShuffleBlockFetcherIterator: Getting 1 (207.0 B) non-empty blocks including 1 (207.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/08/02 07:08:03 INFO ShuffleBlockFetcherIterator: Getting 0 (0.0 B) non-empty blocks including 0 (0.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/08/02 07:08:03 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/08/02 07:08:03 INFO ShuffleBlockFetcherIterator: Getting 0 (0.0 B) non-empty blocks including 0 (0.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/08/02 07:08:03 INFO ShuffleBlockFetcherIterator: Getting 0 (0.0 B) non-empty blocks including 0 (0.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/08/02 07:08:03 INFO ShuffleBlockFetcherIterator: Getting 1 (207.0 B) non-empty blocks including 1 (207.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/08/02 07:08:03 INFO ShuffleBlockFetcherIterator: Getting 0 (0.0 B) non-empty blocks including 0 (0.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/08/02 07:08:03 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/08/02 07:08:03 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/08/02 07:08:03 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/08/02 07:08:03 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/08/02 07:08:03 INFO ShuffleBlockFetcherIterator: Getting 0 (0.0 B) non-empty blocks including 0 (0.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/08/02 07:08:03 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
26/08/02 07:08:03 INFO ShuffleBlockFetcherIterator: Getting 0 (0.0 B) non-empty blocks including 0 (0.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/08/02 07:08:03 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/08/02 07:08:03 INFO ShuffleBlockFetcherIterator: Getting 0 (0.0 B) non-empty blocks including 0 (0.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/08/02 07:08:03 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/08/02 07:08:03 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/08/02 07:08:03 INFO ShuffleBlockFetcherIterator: Getting 0 (0.0 B) non-empty blocks including 0 (0.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/08/02 07:08:03 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/08/02 07:08:03 INFO CodeGenerator: Code generated in 12.211083 ms
26/08/02 07:08:03 INFO CodeGenerator: Code generated in 2.809875 ms
26/08/02 07:08:03 INFO CodeGenerator: Code generated in 1.729666 ms
26/08/02 07:08:03 INFO Executor: Finished task 1.0 in stage 5.0 (TID 14). 7231 bytes result sent to driver
26/08/02 07:08:03 INFO Executor: Finished task 0.0 in stage 5.0 (TID 13). 7274 bytes result sent to driver
26/08/02 07:08:03 INFO Executor: Finished task 5.0 in stage 5.0 (TID 18). 7231 bytes result sent to driver
26/08/02 07:08:03 INFO Executor: Finished task 6.0 in stage 5.0 (TID 11). 7273 bytes result sent to driver
26/08/02 07:08:03 INFO Executor: Finished task 2.0 in stage 5.0 (TID 15). 7274 bytes result sent to driver
26/08/02 07:08:03 INFO Executor: Finished task 4.0 in stage 5.0 (TID 17). 7231 bytes result sent to driver
26/08/02 07:08:03 INFO TaskSetManager: Finished task 1.0 in stage 5.0 (TID 14) in 50 ms on 819c60c13514 (executor driver) (1/10)
26/08/02 07:08:03 INFO TaskSetManager: Finished task 6.0 in stage 5.0 (TID 11) in 52 ms on 819c60c13514 (executor driver) (2/10)
26/08/02 07:08:03 INFO TaskSetManager: Finished task 4.0 in stage 5.0 (TID 17) in 50 ms on 819c60c13514 (executor driver) (3/10)
26/08/02 07:08:03 INFO Executor: Finished task 3.0 in stage 5.0 (TID 16). 7231 bytes result sent to driver
26/08/02 07:08:03 INFO Executor: Finished task 9.0 in stage 5.0 (TID 12). 7273 bytes result sent to driver
26/08/02 07:08:03 INFO Executor: Finished task 8.0 in stage 5.0 (TID 20). 7231 bytes result sent to driver
26/08/02 07:08:03 INFO Executor: Finished task 7.0 in stage 5.0 (TID 19). 7231 bytes result sent to driver
26/08/02 07:08:03 INFO TaskSetManager: Finished task 5.0 in stage 5.0 (TID 18) in 52 ms on 819c60c13514 (executor driver) (4/10)
26/08/02 07:08:03 INFO TaskSetManager: Finished task 0.0 in stage 5.0 (TID 13) in 54 ms on 819c60c13514 (executor driver) (5/10)
26/08/02 07:08:03 INFO TaskSetManager: Finished task 9.0 in stage 5.0 (TID 12) in 55 ms on 819c60c13514 (executor driver) (6/10)
26/08/02 07:08:03 INFO TaskSetManager: Finished task 3.0 in stage 5.0 (TID 16) in 54 ms on 819c60c13514 (executor driver) (7/10)
26/08/02 07:08:03 INFO TaskSetManager: Finished task 8.0 in stage 5.0 (TID 20) in 53 ms on 819c60c13514 (executor driver) (8/10)
26/08/02 07:08:03 INFO TaskSetManager: Finished task 2.0 in stage 5.0 (TID 15) in 55 ms on 819c60c13514 (executor driver) (9/10)
26/08/02 07:08:03 INFO TaskSetManager: Finished task 7.0 in stage 5.0 (TID 19) in 54 ms on 819c60c13514 (executor driver) (10/10)
26/08/02 07:08:03 INFO TaskSchedulerImpl: Removed TaskSet 5.0, whose tasks have all completed, from pool 
26/08/02 07:08:03 INFO DAGScheduler: ResultStage 5 (showString at NativeMethodAccessorImpl.java:0) finished in 0.061 s
26/08/02 07:08:03 INFO DAGScheduler: Job 2 is finished. Cancelling potential speculative or zombie tasks for this job
26/08/02 07:08:03 INFO TaskSchedulerImpl: Killing all running tasks in stage 5: Stage finished
26/08/02 07:08:03 INFO DAGScheduler: Job 2 finished: showString at NativeMethodAccessorImpl.java:0, took 0.068374 s
26/08/02 07:08:03 INFO BlockManagerInfo: Removed broadcast_2_piece0 on 819c60c13514:44573 in memory (size: 28.4 KiB, free: 434.4 MiB)
26/08/02 07:08:03 INFO CodeGenerator: Code generated in 2.2455 ms
+-------------+---------------+
|vehicle_model|avg_engine_temp|
+-------------+---------------+
|      Model_A|          95.75|
|      Model_B|          88.75|
+-------------+---------------+

Simulating deep iterative processing...
Iteration 5: Checkpointing RDD to truncate DAG liability...
26/08/02 07:08:03 INFO DAGScheduler: Registering RDD 14 (checkpoint at NativeMethodAccessorImpl.java:0) as input to shuffle 2
26/08/02 07:08:03 INFO DAGScheduler: Got map stage job 3 (checkpoint at NativeMethodAccessorImpl.java:0) with 10 output partitions
26/08/02 07:08:03 INFO DAGScheduler: Final stage: ShuffleMapStage 6 (checkpoint at NativeMethodAccessorImpl.java:0)
26/08/02 07:08:03 INFO DAGScheduler: Parents of final stage: List()
26/08/02 07:08:03 INFO DAGScheduler: Missing parents: List()
26/08/02 07:08:03 INFO DAGScheduler: Submitting ShuffleMapStage 6 (MapPartitionsRDD[14] at checkpoint at NativeMethodAccessorImpl.java:0), which has no missing parents
26/08/02 07:08:03 INFO MemoryStore: Block broadcast_3 stored as values in memory (estimated size 44.6 KiB, free 434.3 MiB)
26/08/02 07:08:03 INFO MemoryStore: Block broadcast_3_piece0 stored as bytes in memory (estimated size 20.5 KiB, free 434.3 MiB)
26/08/02 07:08:03 INFO BlockManagerInfo: Added broadcast_3_piece0 in memory on 819c60c13514:44573 (size: 20.5 KiB, free: 434.4 MiB)
26/08/02 07:08:03 INFO SparkContext: Created broadcast 3 from broadcast at DAGScheduler.scala:1580
26/08/02 07:08:03 INFO DAGScheduler: Submitting 10 missing tasks from ShuffleMapStage 6 (MapPartitionsRDD[14] at checkpoint at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
26/08/02 07:08:03 INFO TaskSchedulerImpl: Adding task set 6.0 with 10 tasks resource profile 0
26/08/02 07:08:03 INFO TaskSetManager: Starting task 0.0 in stage 6.0 (TID 21) (819c60c13514, executor driver, partition 0, PROCESS_LOCAL, 8548 bytes) 
26/08/02 07:08:03 INFO TaskSetManager: Starting task 1.0 in stage 6.0 (TID 22) (819c60c13514, executor driver, partition 1, PROCESS_LOCAL, 8548 bytes) 
26/08/02 07:08:03 INFO TaskSetManager: Starting task 2.0 in stage 6.0 (TID 23) (819c60c13514, executor driver, partition 2, PROCESS_LOCAL, 8548 bytes) 
26/08/02 07:08:03 INFO TaskSetManager: Starting task 3.0 in stage 6.0 (TID 24) (819c60c13514, executor driver, partition 3, PROCESS_LOCAL, 8548 bytes) 
26/08/02 07:08:03 INFO TaskSetManager: Starting task 4.0 in stage 6.0 (TID 25) (819c60c13514, executor driver, partition 4, PROCESS_LOCAL, 8548 bytes) 
26/08/02 07:08:03 INFO TaskSetManager: Starting task 5.0 in stage 6.0 (TID 26) (819c60c13514, executor driver, partition 5, PROCESS_LOCAL, 8548 bytes) 
26/08/02 07:08:03 INFO TaskSetManager: Starting task 6.0 in stage 6.0 (TID 27) (819c60c13514, executor driver, partition 6, PROCESS_LOCAL, 8548 bytes) 
26/08/02 07:08:03 INFO TaskSetManager: Starting task 7.0 in stage 6.0 (TID 28) (819c60c13514, executor driver, partition 7, PROCESS_LOCAL, 8548 bytes) 
26/08/02 07:08:03 INFO TaskSetManager: Starting task 8.0 in stage 6.0 (TID 29) (819c60c13514, executor driver, partition 8, PROCESS_LOCAL, 8548 bytes) 
26/08/02 07:08:03 INFO TaskSetManager: Starting task 9.0 in stage 6.0 (TID 30) (819c60c13514, executor driver, partition 9, PROCESS_LOCAL, 8548 bytes) 
26/08/02 07:08:03 INFO Executor: Running task 5.0 in stage 6.0 (TID 26)
26/08/02 07:08:03 INFO Executor: Running task 0.0 in stage 6.0 (TID 21)
26/08/02 07:08:03 INFO Executor: Running task 2.0 in stage 6.0 (TID 23)
26/08/02 07:08:03 INFO Executor: Running task 4.0 in stage 6.0 (TID 25)
26/08/02 07:08:03 INFO Executor: Running task 3.0 in stage 6.0 (TID 24)
26/08/02 07:08:03 INFO Executor: Running task 1.0 in stage 6.0 (TID 22)
26/08/02 07:08:03 INFO Executor: Running task 9.0 in stage 6.0 (TID 30)
26/08/02 07:08:03 INFO Executor: Running task 7.0 in stage 6.0 (TID 28)
26/08/02 07:08:03 INFO Executor: Running task 6.0 in stage 6.0 (TID 27)
26/08/02 07:08:03 INFO Executor: Running task 8.0 in stage 6.0 (TID 29)
26/08/02 07:08:04 INFO BlockManagerInfo: Removed broadcast_0_piece0 on 819c60c13514:44573 in memory (size: 20.4 KiB, free: 434.4 MiB)
26/08/02 07:08:04 INFO PythonRunner: Times: total = 62, boot = -704, init = 765, finish = 1
26/08/02 07:08:04 INFO Executor: Finished task 7.0 in stage 6.0 (TID 28). 2785 bytes result sent to driver
26/08/02 07:08:04 INFO TaskSetManager: Finished task 7.0 in stage 6.0 (TID 28) in 97 ms on 819c60c13514 (executor driver) (1/10)
26/08/02 07:08:04 INFO PythonRunner: Times: total = 88, boot = -664, init = 751, finish = 1
26/08/02 07:08:04 INFO PythonRunner: Times: total = 82, boot = -689, init = 771, finish = 0
26/08/02 07:08:04 INFO Executor: Finished task 3.0 in stage 6.0 (TID 24). 2785 bytes result sent to driver
26/08/02 07:08:04 INFO TaskSetManager: Finished task 3.0 in stage 6.0 (TID 24) in 110 ms on 819c60c13514 (executor driver) (2/10)
26/08/02 07:08:04 INFO PythonRunner: Times: total = 95, boot = -636, init = 731, finish = 0
26/08/02 07:08:04 INFO Executor: Finished task 2.0 in stage 6.0 (TID 23). 2828 bytes result sent to driver
26/08/02 07:08:04 INFO TaskSetManager: Finished task 2.0 in stage 6.0 (TID 23) in 116 ms on 819c60c13514 (executor driver) (3/10)
26/08/02 07:08:04 INFO PythonRunner: Times: total = 99, boot = -647, init = 746, finish = 0
26/08/02 07:08:04 INFO PythonRunner: Times: total = 107, boot = -627, init = 734, finish = 0
26/08/02 07:08:04 INFO PythonRunner: Times: total = 103, boot = -685, init = 788, finish = 0
26/08/02 07:08:04 INFO Executor: Finished task 1.0 in stage 6.0 (TID 22). 2785 bytes result sent to driver
26/08/02 07:08:04 INFO TaskSetManager: Finished task 1.0 in stage 6.0 (TID 22) in 130 ms on 819c60c13514 (executor driver) (4/10)
26/08/02 07:08:04 INFO Executor: Finished task 4.0 in stage 6.0 (TID 25). 2785 bytes result sent to driver
26/08/02 07:08:04 INFO Executor: Finished task 8.0 in stage 6.0 (TID 29). 2785 bytes result sent to driver
26/08/02 07:08:04 INFO Executor: Finished task 0.0 in stage 6.0 (TID 21). 2785 bytes result sent to driver
26/08/02 07:08:04 INFO TaskSetManager: Finished task 4.0 in stage 6.0 (TID 25) in 136 ms on 819c60c13514 (executor driver) (5/10)
26/08/02 07:08:04 INFO TaskSetManager: Finished task 0.0 in stage 6.0 (TID 21) in 137 ms on 819c60c13514 (executor driver) (6/10)
26/08/02 07:08:04 INFO PythonRunner: Times: total = 123, boot = -666, init = 789, finish = 0
26/08/02 07:08:04 INFO TaskSetManager: Finished task 8.0 in stage 6.0 (TID 29) in 137 ms on 819c60c13514 (executor driver) (7/10)
26/08/02 07:08:04 INFO Executor: Finished task 9.0 in stage 6.0 (TID 30). 2785 bytes result sent to driver
26/08/02 07:08:04 INFO TaskSetManager: Finished task 9.0 in stage 6.0 (TID 30) in 142 ms on 819c60c13514 (executor driver) (8/10)
26/08/02 07:08:04 INFO PythonRunner: Times: total = 131, boot = -713, init = 844, finish = 0
26/08/02 07:08:04 INFO Executor: Finished task 5.0 in stage 6.0 (TID 26). 2785 bytes result sent to driver
26/08/02 07:08:04 INFO TaskSetManager: Finished task 5.0 in stage 6.0 (TID 26) in 152 ms on 819c60c13514 (executor driver) (9/10)
26/08/02 07:08:04 INFO PythonRunner: Times: total = 135, boot = -715, init = 850, finish = 0
26/08/02 07:08:04 INFO Executor: Finished task 6.0 in stage 6.0 (TID 27). 2785 bytes result sent to driver
26/08/02 07:08:04 INFO TaskSetManager: Finished task 6.0 in stage 6.0 (TID 27) in 155 ms on 819c60c13514 (executor driver) (10/10)
26/08/02 07:08:04 INFO TaskSchedulerImpl: Removed TaskSet 6.0, whose tasks have all completed, from pool 
26/08/02 07:08:04 INFO DAGScheduler: ShuffleMapStage 6 (checkpoint at NativeMethodAccessorImpl.java:0) finished in 0.164 s
26/08/02 07:08:04 INFO DAGScheduler: looking for newly runnable stages
26/08/02 07:08:04 INFO DAGScheduler: running: Set()
26/08/02 07:08:04 INFO DAGScheduler: waiting: Set()
26/08/02 07:08:04 INFO DAGScheduler: failed: Set()
26/08/02 07:08:04 INFO ShufflePartitionsUtil: For shuffle(2), advisory target size: 67108864, actual target size 1048576, minimum partition size: 1048576
26/08/02 07:08:04 INFO HashAggregateExec: spark.sql.codegen.aggregate.map.twolevel.enabled is set to true, but current version of codegened fast hashmap does not support this aggregate.
26/08/02 07:08:04 INFO DAGScheduler: Registering RDD 17 (checkpoint at NativeMethodAccessorImpl.java:0) as input to shuffle 3
26/08/02 07:08:04 INFO DAGScheduler: Got map stage job 4 (checkpoint at NativeMethodAccessorImpl.java:0) with 1 output partitions
26/08/02 07:08:04 INFO DAGScheduler: Final stage: ShuffleMapStage 8 (checkpoint at NativeMethodAccessorImpl.java:0)
26/08/02 07:08:04 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 7)
26/08/02 07:08:04 INFO DAGScheduler: Missing parents: List()
26/08/02 07:08:04 INFO DAGScheduler: Submitting ShuffleMapStage 8 (MapPartitionsRDD[17] at checkpoint at NativeMethodAccessorImpl.java:0), which has no missing parents
26/08/02 07:08:04 INFO MemoryStore: Block broadcast_4 stored as values in memory (estimated size 46.4 KiB, free 434.3 MiB)
26/08/02 07:08:04 INFO MemoryStore: Block broadcast_4_piece0 stored as bytes in memory (estimated size 21.7 KiB, free 434.3 MiB)
26/08/02 07:08:04 INFO BlockManagerInfo: Added broadcast_4_piece0 in memory on 819c60c13514:44573 (size: 21.7 KiB, free: 434.4 MiB)
26/08/02 07:08:04 INFO SparkContext: Created broadcast 4 from broadcast at DAGScheduler.scala:1580
26/08/02 07:08:04 INFO DAGScheduler: Submitting 1 missing tasks from ShuffleMapStage 8 (MapPartitionsRDD[17] at checkpoint at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0))
26/08/02 07:08:04 INFO TaskSchedulerImpl: Adding task set 8.0 with 1 tasks resource profile 0
26/08/02 07:08:04 INFO TaskSetManager: Starting task 0.0 in stage 8.0 (TID 31) (819c60c13514, executor driver, partition 0, NODE_LOCAL, 7604 bytes) 
26/08/02 07:08:04 INFO Executor: Running task 0.0 in stage 8.0 (TID 31)
26/08/02 07:08:04 INFO ShuffleBlockFetcherIterator: Getting 10 (15.1 KiB) non-empty blocks including 10 (15.1 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/08/02 07:08:04 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/08/02 07:08:04 INFO Executor: Finished task 0.0 in stage 8.0 (TID 31). 5441 bytes result sent to driver
26/08/02 07:08:04 INFO TaskSetManager: Finished task 0.0 in stage 8.0 (TID 31) in 8 ms on 819c60c13514 (executor driver) (1/1)
26/08/02 07:08:04 INFO TaskSchedulerImpl: Removed TaskSet 8.0, whose tasks have all completed, from pool 
26/08/02 07:08:04 INFO DAGScheduler: ShuffleMapStage 8 (checkpoint at NativeMethodAccessorImpl.java:0) finished in 0.011 s
26/08/02 07:08:04 INFO DAGScheduler: looking for newly runnable stages
26/08/02 07:08:04 INFO DAGScheduler: running: Set()
26/08/02 07:08:04 INFO DAGScheduler: waiting: Set()
26/08/02 07:08:04 INFO DAGScheduler: failed: Set()
26/08/02 07:08:04 INFO HashAggregateExec: spark.sql.codegen.aggregate.map.twolevel.enabled is set to true, but current version of codegened fast hashmap does not support this aggregate.
26/08/02 07:08:04 INFO CodeGenerator: Code generated in 10.543 ms
26/08/02 07:08:04 INFO MemoryStore: Block broadcast_5 stored as values in memory (estimated size 221.7 KiB, free 434.1 MiB)
26/08/02 07:08:04 INFO MemoryStore: Block broadcast_5_piece0 stored as bytes in memory (estimated size 32.6 KiB, free 434.0 MiB)
26/08/02 07:08:04 INFO BlockManagerInfo: Added broadcast_5_piece0 in memory on 819c60c13514:44573 (size: 32.6 KiB, free: 434.3 MiB)
26/08/02 07:08:04 INFO SparkContext: Created broadcast 5 from checkpoint at NativeMethodAccessorImpl.java:0
26/08/02 07:08:04 INFO SparkContext: Starting job: checkpoint at NativeMethodAccessorImpl.java:0
26/08/02 07:08:04 INFO DAGScheduler: Got job 5 (checkpoint at NativeMethodAccessorImpl.java:0) with 10 output partitions
26/08/02 07:08:04 INFO DAGScheduler: Final stage: ResultStage 11 (checkpoint at NativeMethodAccessorImpl.java:0)
26/08/02 07:08:04 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 10)
26/08/02 07:08:04 INFO DAGScheduler: Missing parents: List()
26/08/02 07:08:04 INFO DAGScheduler: Submitting ResultStage 11 (MapPartitionsRDD[20] at checkpoint at NativeMethodAccessorImpl.java:0), which has no missing parents
26/08/02 07:08:04 INFO MemoryStore: Block broadcast_6 stored as values in memory (estimated size 71.2 KiB, free 434.0 MiB)
26/08/02 07:08:04 INFO MemoryStore: Block broadcast_6_piece0 stored as bytes in memory (estimated size 29.6 KiB, free 433.9 MiB)
26/08/02 07:08:04 INFO BlockManagerInfo: Added broadcast_6_piece0 in memory on 819c60c13514:44573 (size: 29.6 KiB, free: 434.3 MiB)
26/08/02 07:08:04 INFO SparkContext: Created broadcast 6 from broadcast at DAGScheduler.scala:1580
26/08/02 07:08:04 INFO DAGScheduler: Submitting 10 missing tasks from ResultStage 11 (MapPartitionsRDD[20] at checkpoint at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
26/08/02 07:08:04 INFO TaskSchedulerImpl: Adding task set 11.0 with 10 tasks resource profile 0
26/08/02 07:08:04 INFO TaskSetManager: Starting task 6.0 in stage 11.0 (TID 32) (819c60c13514, executor driver, partition 6, NODE_LOCAL, 7615 bytes) 
26/08/02 07:08:04 INFO TaskSetManager: Starting task 9.0 in stage 11.0 (TID 33) (819c60c13514, executor driver, partition 9, NODE_LOCAL, 7615 bytes) 
26/08/02 07:08:04 INFO TaskSetManager: Starting task 0.0 in stage 11.0 (TID 34) (819c60c13514, executor driver, partition 0, PROCESS_LOCAL, 7615 bytes) 
26/08/02 07:08:04 INFO TaskSetManager: Starting task 1.0 in stage 11.0 (TID 35) (819c60c13514, executor driver, partition 1, PROCESS_LOCAL, 7615 bytes) 
26/08/02 07:08:04 INFO TaskSetManager: Starting task 2.0 in stage 11.0 (TID 36) (819c60c13514, executor driver, partition 2, PROCESS_LOCAL, 7615 bytes) 
26/08/02 07:08:04 INFO TaskSetManager: Starting task 3.0 in stage 11.0 (TID 37) (819c60c13514, executor driver, partition 3, PROCESS_LOCAL, 7615 bytes) 
26/08/02 07:08:04 INFO TaskSetManager: Starting task 4.0 in stage 11.0 (TID 38) (819c60c13514, executor driver, partition 4, PROCESS_LOCAL, 7615 bytes) 
26/08/02 07:08:04 INFO TaskSetManager: Starting task 5.0 in stage 11.0 (TID 39) (819c60c13514, executor driver, partition 5, PROCESS_LOCAL, 7615 bytes) 
26/08/02 07:08:04 INFO TaskSetManager: Starting task 7.0 in stage 11.0 (TID 40) (819c60c13514, executor driver, partition 7, PROCESS_LOCAL, 7615 bytes) 
26/08/02 07:08:04 INFO TaskSetManager: Starting task 8.0 in stage 11.0 (TID 41) (819c60c13514, executor driver, partition 8, PROCESS_LOCAL, 7615 bytes) 
26/08/02 07:08:04 INFO BlockManagerInfo: Removed broadcast_3_piece0 on 819c60c13514:44573 in memory (size: 20.5 KiB, free: 434.3 MiB)
26/08/02 07:08:04 INFO Executor: Running task 6.0 in stage 11.0 (TID 32)
26/08/02 07:08:04 INFO Executor: Running task 3.0 in stage 11.0 (TID 37)
26/08/02 07:08:04 INFO Executor: Running task 1.0 in stage 11.0 (TID 35)
26/08/02 07:08:04 INFO Executor: Running task 9.0 in stage 11.0 (TID 33)
26/08/02 07:08:04 INFO Executor: Running task 8.0 in stage 11.0 (TID 41)
26/08/02 07:08:04 INFO Executor: Running task 4.0 in stage 11.0 (TID 38)
26/08/02 07:08:04 INFO Executor: Running task 0.0 in stage 11.0 (TID 34)
26/08/02 07:08:04 INFO Executor: Running task 2.0 in stage 11.0 (TID 36)
26/08/02 07:08:04 INFO Executor: Running task 7.0 in stage 11.0 (TID 40)
26/08/02 07:08:04 INFO BlockManagerInfo: Removed broadcast_4_piece0 on 819c60c13514:44573 in memory (size: 21.7 KiB, free: 434.3 MiB)
26/08/02 07:08:04 INFO Executor: Running task 5.0 in stage 11.0 (TID 39)
26/08/02 07:08:04 INFO ShuffleBlockFetcherIterator: Getting 0 (0.0 B) non-empty blocks including 0 (0.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/08/02 07:08:04 INFO ShuffleBlockFetcherIterator: Getting 1 (207.0 B) non-empty blocks including 1 (207.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/08/02 07:08:04 INFO ShuffleBlockFetcherIterator: Getting 0 (0.0 B) non-empty blocks including 0 (0.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/08/02 07:08:04 INFO ShuffleBlockFetcherIterator: Getting 0 (0.0 B) non-empty blocks including 0 (0.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/08/02 07:08:04 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/08/02 07:08:04 INFO ShuffleBlockFetcherIterator: Getting 1 (207.0 B) non-empty blocks including 1 (207.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/08/02 07:08:04 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/08/02 07:08:04 INFO ShuffleBlockFetcherIterator: Getting 0 (0.0 B) non-empty blocks including 0 (0.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/08/02 07:08:04 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/08/02 07:08:04 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
26/08/02 07:08:04 INFO ShuffleBlockFetcherIterator: Getting 0 (0.0 B) non-empty blocks including 0 (0.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/08/02 07:08:04 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
26/08/02 07:08:04 INFO ShuffleBlockFetcherIterator: Getting 0 (0.0 B) non-empty blocks including 0 (0.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/08/02 07:08:04 INFO ShuffleBlockFetcherIterator: Getting 0 (0.0 B) non-empty blocks including 0 (0.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/08/02 07:08:04 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
26/08/02 07:08:04 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/08/02 07:08:04 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/08/02 07:08:04 INFO ShuffleBlockFetcherIterator: Getting 0 (0.0 B) non-empty blocks including 0 (0.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
26/08/02 07:08:04 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
26/08/02 07:08:04 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
26/08/02 07:08:04 INFO CodeGenerator: Code generated in 10.992083 ms
26/08/02 07:08:04 INFO Executor: Finished task 7.0 in stage 11.0 (TID 40). 6973 bytes result sent to driver
26/08/02 07:08:04 INFO Executor: Finished task 9.0 in stage 11.0 (TID 33). 6973 bytes result sent to driver
26/08/02 07:08:04 INFO TaskSetManager: Finished task 9.0 in stage 11.0 (TID 33) in 57 ms on 819c60c13514 (executor driver) (1/10)
26/08/02 07:08:04 INFO TaskSetManager: Finished task 7.0 in stage 11.0 (TID 40) in 56 ms on 819c60c13514 (executor driver) (2/10)
26/08/02 07:08:04 INFO Executor: Finished task 4.0 in stage 11.0 (TID 38). 6973 bytes result sent to driver
26/08/02 07:08:04 INFO TaskSetManager: Finished task 4.0 in stage 11.0 (TID 38) in 57 ms on 819c60c13514 (executor driver) (3/10)
26/08/02 07:08:04 INFO Executor: Finished task 0.0 in stage 11.0 (TID 34). 6973 bytes result sent to driver
26/08/02 07:08:04 INFO Executor: Finished task 6.0 in stage 11.0 (TID 32). 6973 bytes result sent to driver
26/08/02 07:08:04 INFO Executor: Finished task 8.0 in stage 11.0 (TID 41). 6973 bytes result sent to driver
26/08/02 07:08:04 INFO TaskSetManager: Finished task 0.0 in stage 11.0 (TID 34) in 62 ms on 819c60c13514 (executor driver) (4/10)
26/08/02 07:08:04 INFO Executor: Finished task 2.0 in stage 11.0 (TID 36). 6973 bytes result sent to driver
26/08/02 07:08:04 INFO TaskSetManager: Finished task 6.0 in stage 11.0 (TID 32) in 63 ms on 819c60c13514 (executor driver) (5/10)
26/08/02 07:08:04 INFO TaskSetManager: Finished task 2.0 in stage 11.0 (TID 36) in 63 ms on 819c60c13514 (executor driver) (6/10)
26/08/02 07:08:04 INFO TaskSetManager: Finished task 8.0 in stage 11.0 (TID 41) in 63 ms on 819c60c13514 (executor driver) (7/10)
26/08/02 07:08:04 INFO Executor: Finished task 3.0 in stage 11.0 (TID 37). 6973 bytes result sent to driver
26/08/02 07:08:04 INFO TaskSetManager: Finished task 3.0 in stage 11.0 (TID 37) in 64 ms on 819c60c13514 (executor driver) (8/10)
26/08/02 07:08:04 INFO Executor: Finished task 5.0 in stage 11.0 (TID 39). 6973 bytes result sent to driver
26/08/02 07:08:04 INFO TaskSetManager: Finished task 5.0 in stage 11.0 (TID 39) in 66 ms on 819c60c13514 (executor driver) (9/10)
26/08/02 07:08:04 INFO Executor: Finished task 1.0 in stage 11.0 (TID 35). 6973 bytes result sent to driver
26/08/02 07:08:04 INFO TaskSetManager: Finished task 1.0 in stage 11.0 (TID 35) in 68 ms on 819c60c13514 (executor driver) (10/10)
26/08/02 07:08:04 INFO TaskSchedulerImpl: Removed TaskSet 11.0, whose tasks have all completed, from pool 
26/08/02 07:08:04 INFO DAGScheduler: ResultStage 11 (checkpoint at NativeMethodAccessorImpl.java:0) finished in 0.077 s
26/08/02 07:08:04 INFO DAGScheduler: Job 5 is finished. Cancelling potential speculative or zombie tasks for this job
26/08/02 07:08:04 INFO TaskSchedulerImpl: Killing all running tasks in stage 11: Stage finished
26/08/02 07:08:04 INFO DAGScheduler: Job 5 finished: checkpoint at NativeMethodAccessorImpl.java:0, took 0.078575 s
26/08/02 07:08:04 INFO ReliableCheckpointRDD: Checkpointing took 96 ms.
26/08/02 07:08:04 INFO MemoryStore: Block broadcast_7 stored as values in memory (estimated size 221.7 KiB, free 433.8 MiB)
26/08/02 07:08:04 INFO MemoryStore: Block broadcast_7_piece0 stored as bytes in memory (estimated size 32.6 KiB, free 433.8 MiB)
26/08/02 07:08:04 INFO BlockManagerInfo: Added broadcast_7_piece0 in memory on 819c60c13514:44573 (size: 32.6 KiB, free: 434.3 MiB)
26/08/02 07:08:04 INFO SparkContext: Created broadcast 7 from checkpoint at NativeMethodAccessorImpl.java:0
26/08/02 07:08:04 INFO ReliableRDDCheckpointData: Done checkpointing RDD 20 to file:/app/data/checkpoints/d2653d35-6801-4a1d-bef5-159020f1139d/rdd-20, new parent is RDD 21
Iteration 10: Checkpointing RDD to truncate DAG liability...
26/08/02 07:08:04 INFO CodeGenerator: Code generated in 3.022667 ms
26/08/02 07:08:04 INFO MemoryStore: Block broadcast_8 stored as values in memory (estimated size 221.7 KiB, free 433.6 MiB)
26/08/02 07:08:04 INFO MemoryStore: Block broadcast_8_piece0 stored as bytes in memory (estimated size 32.6 KiB, free 433.6 MiB)
26/08/02 07:08:04 INFO BlockManagerInfo: Added broadcast_8_piece0 in memory on 819c60c13514:44573 (size: 32.6 KiB, free: 434.3 MiB)
26/08/02 07:08:04 INFO SparkContext: Created broadcast 8 from checkpoint at NativeMethodAccessorImpl.java:0
26/08/02 07:08:04 INFO SparkContext: Starting job: checkpoint at NativeMethodAccessorImpl.java:0
26/08/02 07:08:04 INFO DAGScheduler: Got job 6 (checkpoint at NativeMethodAccessorImpl.java:0) with 10 output partitions
26/08/02 07:08:04 INFO DAGScheduler: Final stage: ResultStage 12 (checkpoint at NativeMethodAccessorImpl.java:0)
26/08/02 07:08:04 INFO DAGScheduler: Parents of final stage: List()
26/08/02 07:08:04 INFO DAGScheduler: Missing parents: List()
26/08/02 07:08:04 INFO DAGScheduler: Submitting ResultStage 12 (MapPartitionsRDD[23] at checkpoint at NativeMethodAccessorImpl.java:0), which has no missing parents
26/08/02 07:08:04 INFO MemoryStore: Block broadcast_9 stored as values in memory (estimated size 12.3 KiB, free 433.5 MiB)
26/08/02 07:08:04 INFO MemoryStore: Block broadcast_9_piece0 stored as bytes in memory (estimated size 5.4 KiB, free 433.5 MiB)
26/08/02 07:08:04 INFO BlockManagerInfo: Added broadcast_9_piece0 in memory on 819c60c13514:44573 (size: 5.4 KiB, free: 434.3 MiB)
26/08/02 07:08:04 INFO SparkContext: Created broadcast 9 from broadcast at DAGScheduler.scala:1580
26/08/02 07:08:04 INFO DAGScheduler: Submitting 10 missing tasks from ResultStage 12 (MapPartitionsRDD[23] at checkpoint at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
26/08/02 07:08:04 INFO TaskSchedulerImpl: Adding task set 12.0 with 10 tasks resource profile 0
26/08/02 07:08:04 INFO TaskSetManager: Starting task 0.0 in stage 12.0 (TID 42) (819c60c13514, executor driver, partition 0, PROCESS_LOCAL, 7425 bytes) 
26/08/02 07:08:04 INFO TaskSetManager: Starting task 1.0 in stage 12.0 (TID 43) (819c60c13514, executor driver, partition 1, PROCESS_LOCAL, 7425 bytes) 
26/08/02 07:08:04 INFO TaskSetManager: Starting task 2.0 in stage 12.0 (TID 44) (819c60c13514, executor driver, partition 2, PROCESS_LOCAL, 7425 bytes) 
26/08/02 07:08:04 INFO TaskSetManager: Starting task 3.0 in stage 12.0 (TID 45) (819c60c13514, executor driver, partition 3, PROCESS_LOCAL, 7425 bytes) 
26/08/02 07:08:04 INFO TaskSetManager: Starting task 4.0 in stage 12.0 (TID 46) (819c60c13514, executor driver, partition 4, PROCESS_LOCAL, 7425 bytes) 
26/08/02 07:08:04 INFO TaskSetManager: Starting task 5.0 in stage 12.0 (TID 47) (819c60c13514, executor driver, partition 5, PROCESS_LOCAL, 7425 bytes) 
26/08/02 07:08:04 INFO TaskSetManager: Starting task 6.0 in stage 12.0 (TID 48) (819c60c13514, executor driver, partition 6, PROCESS_LOCAL, 7425 bytes) 
26/08/02 07:08:04 INFO TaskSetManager: Starting task 7.0 in stage 12.0 (TID 49) (819c60c13514, executor driver, partition 7, PROCESS_LOCAL, 7425 bytes) 
26/08/02 07:08:04 INFO TaskSetManager: Starting task 8.0 in stage 12.0 (TID 50) (819c60c13514, executor driver, partition 8, PROCESS_LOCAL, 7425 bytes) 
26/08/02 07:08:04 INFO TaskSetManager: Starting task 9.0 in stage 12.0 (TID 51) (819c60c13514, executor driver, partition 9, PROCESS_LOCAL, 7425 bytes) 
26/08/02 07:08:04 INFO Executor: Running task 4.0 in stage 12.0 (TID 46)
26/08/02 07:08:04 INFO Executor: Running task 8.0 in stage 12.0 (TID 50)
26/08/02 07:08:04 INFO Executor: Running task 3.0 in stage 12.0 (TID 45)
26/08/02 07:08:04 INFO Executor: Running task 2.0 in stage 12.0 (TID 44)
26/08/02 07:08:04 INFO Executor: Running task 7.0 in stage 12.0 (TID 49)
26/08/02 07:08:04 INFO Executor: Running task 5.0 in stage 12.0 (TID 47)
26/08/02 07:08:04 INFO Executor: Running task 1.0 in stage 12.0 (TID 43)
26/08/02 07:08:04 INFO Executor: Running task 0.0 in stage 12.0 (TID 42)
26/08/02 07:08:04 INFO Executor: Running task 9.0 in stage 12.0 (TID 51)
26/08/02 07:08:04 INFO Executor: Running task 6.0 in stage 12.0 (TID 48)
26/08/02 07:08:04 INFO CodeGenerator: Code generated in 3.602958 ms
26/08/02 07:08:04 INFO Executor: Finished task 6.0 in stage 12.0 (TID 48). 1187 bytes result sent to driver
26/08/02 07:08:04 INFO Executor: Finished task 0.0 in stage 12.0 (TID 42). 1187 bytes result sent to driver
26/08/02 07:08:04 INFO TaskSetManager: Finished task 6.0 in stage 12.0 (TID 48) in 35 ms on 819c60c13514 (executor driver) (1/10)
26/08/02 07:08:04 INFO TaskSetManager: Finished task 0.0 in stage 12.0 (TID 42) in 37 ms on 819c60c13514 (executor driver) (2/10)
26/08/02 07:08:04 INFO Executor: Finished task 7.0 in stage 12.0 (TID 49). 1187 bytes result sent to driver
26/08/02 07:08:04 INFO TaskSetManager: Finished task 7.0 in stage 12.0 (TID 49) in 38 ms on 819c60c13514 (executor driver) (3/10)
26/08/02 07:08:04 INFO Executor: Finished task 3.0 in stage 12.0 (TID 45). 1187 bytes result sent to driver
26/08/02 07:08:04 INFO TaskSetManager: Finished task 3.0 in stage 12.0 (TID 45) in 39 ms on 819c60c13514 (executor driver) (4/10)
26/08/02 07:08:04 INFO Executor: Finished task 4.0 in stage 12.0 (TID 46). 1187 bytes result sent to driver
26/08/02 07:08:04 INFO TaskSetManager: Finished task 4.0 in stage 12.0 (TID 46) in 41 ms on 819c60c13514 (executor driver) (5/10)
26/08/02 07:08:04 INFO Executor: Finished task 9.0 in stage 12.0 (TID 51). 1187 bytes result sent to driver
26/08/02 07:08:04 INFO Executor: Finished task 5.0 in stage 12.0 (TID 47). 1187 bytes result sent to driver
26/08/02 07:08:04 INFO TaskSetManager: Finished task 9.0 in stage 12.0 (TID 51) in 42 ms on 819c60c13514 (executor driver) (6/10)
26/08/02 07:08:04 INFO TaskSetManager: Finished task 5.0 in stage 12.0 (TID 47) in 43 ms on 819c60c13514 (executor driver) (7/10)
26/08/02 07:08:04 INFO Executor: Finished task 2.0 in stage 12.0 (TID 44). 1187 bytes result sent to driver
26/08/02 07:08:04 INFO TaskSetManager: Finished task 2.0 in stage 12.0 (TID 44) in 47 ms on 819c60c13514 (executor driver) (8/10)
26/08/02 07:08:04 INFO Executor: Finished task 8.0 in stage 12.0 (TID 50). 1230 bytes result sent to driver
26/08/02 07:08:04 INFO Executor: Finished task 1.0 in stage 12.0 (TID 43). 1187 bytes result sent to driver
26/08/02 07:08:04 INFO TaskSetManager: Finished task 8.0 in stage 12.0 (TID 50) in 48 ms on 819c60c13514 (executor driver) (9/10)
26/08/02 07:08:04 INFO TaskSetManager: Finished task 1.0 in stage 12.0 (TID 43) in 50 ms on 819c60c13514 (executor driver) (10/10)
26/08/02 07:08:04 INFO TaskSchedulerImpl: Removed TaskSet 12.0, whose tasks have all completed, from pool 
26/08/02 07:08:04 INFO DAGScheduler: ResultStage 12 (checkpoint at NativeMethodAccessorImpl.java:0) finished in 0.054 s
26/08/02 07:08:04 INFO DAGScheduler: Job 6 is finished. Cancelling potential speculative or zombie tasks for this job
26/08/02 07:08:04 INFO TaskSchedulerImpl: Killing all running tasks in stage 12: Stage finished
26/08/02 07:08:04 INFO DAGScheduler: Job 6 finished: checkpoint at NativeMethodAccessorImpl.java:0, took 0.057043 s
26/08/02 07:08:04 INFO ReliableCheckpointRDD: Checkpointing took 64 ms.
26/08/02 07:08:04 INFO MemoryStore: Block broadcast_10 stored as values in memory (estimated size 221.7 KiB, free 433.3 MiB)
26/08/02 07:08:04 INFO BlockManagerInfo: Removed broadcast_5_piece0 on 819c60c13514:44573 in memory (size: 32.6 KiB, free: 434.3 MiB)
26/08/02 07:08:04 INFO MemoryStore: Block broadcast_10_piece0 stored as bytes in memory (estimated size 32.6 KiB, free 433.5 MiB)
26/08/02 07:08:04 INFO BlockManagerInfo: Added broadcast_10_piece0 in memory on 819c60c13514:44573 (size: 32.6 KiB, free: 434.3 MiB)
26/08/02 07:08:04 INFO SparkContext: Created broadcast 10 from checkpoint at NativeMethodAccessorImpl.java:0
26/08/02 07:08:04 INFO BlockManagerInfo: Removed broadcast_9_piece0 on 819c60c13514:44573 in memory (size: 5.4 KiB, free: 434.3 MiB)
26/08/02 07:08:04 INFO BlockManagerInfo: Removed broadcast_6_piece0 on 819c60c13514:44573 in memory (size: 29.6 KiB, free: 434.3 MiB)
26/08/02 07:08:04 INFO ReliableRDDCheckpointData: Done checkpointing RDD 23 to file:/app/data/checkpoints/d2653d35-6801-4a1d-bef5-159020f1139d/rdd-23, new parent is RDD 24
Iteration 15: Checkpointing RDD to truncate DAG liability...
26/08/02 07:08:04 INFO CodeGenerator: Code generated in 4.790917 ms
26/08/02 07:08:04 INFO MemoryStore: Block broadcast_11 stored as values in memory (estimated size 221.7 KiB, free 433.4 MiB)
26/08/02 07:08:04 INFO MemoryStore: Block broadcast_11_piece0 stored as bytes in memory (estimated size 32.6 KiB, free 433.4 MiB)
26/08/02 07:08:04 INFO BlockManagerInfo: Added broadcast_11_piece0 in memory on 819c60c13514:44573 (size: 32.6 KiB, free: 434.3 MiB)
26/08/02 07:08:04 INFO SparkContext: Created broadcast 11 from checkpoint at NativeMethodAccessorImpl.java:0
26/08/02 07:08:04 INFO SparkContext: Starting job: checkpoint at NativeMethodAccessorImpl.java:0
26/08/02 07:08:04 INFO DAGScheduler: Got job 7 (checkpoint at NativeMethodAccessorImpl.java:0) with 10 output partitions
26/08/02 07:08:04 INFO DAGScheduler: Final stage: ResultStage 13 (checkpoint at NativeMethodAccessorImpl.java:0)
26/08/02 07:08:04 INFO DAGScheduler: Parents of final stage: List()
26/08/02 07:08:04 INFO DAGScheduler: Missing parents: List()
26/08/02 07:08:04 INFO DAGScheduler: Submitting ResultStage 13 (MapPartitionsRDD[26] at checkpoint at NativeMethodAccessorImpl.java:0), which has no missing parents
26/08/02 07:08:04 INFO MemoryStore: Block broadcast_12 stored as values in memory (estimated size 13.5 KiB, free 433.4 MiB)
26/08/02 07:08:04 INFO MemoryStore: Block broadcast_12_piece0 stored as bytes in memory (estimated size 5.6 KiB, free 433.4 MiB)
26/08/02 07:08:04 INFO BlockManagerInfo: Added broadcast_12_piece0 in memory on 819c60c13514:44573 (size: 5.6 KiB, free: 434.3 MiB)
26/08/02 07:08:04 INFO SparkContext: Created broadcast 12 from broadcast at DAGScheduler.scala:1580
26/08/02 07:08:04 INFO DAGScheduler: Submitting 10 missing tasks from ResultStage 13 (MapPartitionsRDD[26] at checkpoint at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
26/08/02 07:08:04 INFO TaskSchedulerImpl: Adding task set 13.0 with 10 tasks resource profile 0
26/08/02 07:08:04 INFO TaskSetManager: Starting task 0.0 in stage 13.0 (TID 52) (819c60c13514, executor driver, partition 0, PROCESS_LOCAL, 7425 bytes) 
26/08/02 07:08:04 INFO TaskSetManager: Starting task 1.0 in stage 13.0 (TID 53) (819c60c13514, executor driver, partition 1, PROCESS_LOCAL, 7425 bytes) 
26/08/02 07:08:04 INFO TaskSetManager: Starting task 2.0 in stage 13.0 (TID 54) (819c60c13514, executor driver, partition 2, PROCESS_LOCAL, 7425 bytes) 
26/08/02 07:08:04 INFO TaskSetManager: Starting task 3.0 in stage 13.0 (TID 55) (819c60c13514, executor driver, partition 3, PROCESS_LOCAL, 7425 bytes) 
26/08/02 07:08:04 INFO TaskSetManager: Starting task 4.0 in stage 13.0 (TID 56) (819c60c13514, executor driver, partition 4, PROCESS_LOCAL, 7425 bytes) 
26/08/02 07:08:04 INFO TaskSetManager: Starting task 5.0 in stage 13.0 (TID 57) (819c60c13514, executor driver, partition 5, PROCESS_LOCAL, 7425 bytes) 
26/08/02 07:08:04 INFO TaskSetManager: Starting task 6.0 in stage 13.0 (TID 58) (819c60c13514, executor driver, partition 6, PROCESS_LOCAL, 7425 bytes) 
26/08/02 07:08:04 INFO TaskSetManager: Starting task 7.0 in stage 13.0 (TID 59) (819c60c13514, executor driver, partition 7, PROCESS_LOCAL, 7425 bytes) 
26/08/02 07:08:04 INFO TaskSetManager: Starting task 8.0 in stage 13.0 (TID 60) (819c60c13514, executor driver, partition 8, PROCESS_LOCAL, 7425 bytes) 
26/08/02 07:08:04 INFO TaskSetManager: Starting task 9.0 in stage 13.0 (TID 61) (819c60c13514, executor driver, partition 9, PROCESS_LOCAL, 7425 bytes) 
26/08/02 07:08:04 INFO Executor: Running task 6.0 in stage 13.0 (TID 58)
26/08/02 07:08:04 INFO Executor: Running task 4.0 in stage 13.0 (TID 56)
26/08/02 07:08:04 INFO Executor: Running task 8.0 in stage 13.0 (TID 60)
26/08/02 07:08:04 INFO Executor: Running task 5.0 in stage 13.0 (TID 57)
26/08/02 07:08:04 INFO Executor: Running task 9.0 in stage 13.0 (TID 61)
26/08/02 07:08:04 INFO Executor: Running task 0.0 in stage 13.0 (TID 52)
26/08/02 07:08:04 INFO Executor: Running task 1.0 in stage 13.0 (TID 53)
26/08/02 07:08:04 INFO Executor: Running task 3.0 in stage 13.0 (TID 55)
26/08/02 07:08:04 INFO Executor: Running task 2.0 in stage 13.0 (TID 54)
26/08/02 07:08:04 INFO Executor: Running task 7.0 in stage 13.0 (TID 59)
26/08/02 07:08:04 INFO CodeGenerator: Code generated in 4.134959 ms
26/08/02 07:08:04 INFO Executor: Finished task 4.0 in stage 13.0 (TID 56). 1187 bytes result sent to driver
26/08/02 07:08:04 INFO TaskSetManager: Finished task 4.0 in stage 13.0 (TID 56) in 31 ms on 819c60c13514 (executor driver) (1/10)
26/08/02 07:08:04 INFO Executor: Finished task 3.0 in stage 13.0 (TID 55). 1187 bytes result sent to driver
26/08/02 07:08:04 INFO TaskSetManager: Finished task 3.0 in stage 13.0 (TID 55) in 32 ms on 819c60c13514 (executor driver) (2/10)
26/08/02 07:08:04 INFO Executor: Finished task 6.0 in stage 13.0 (TID 58). 1187 bytes result sent to driver
26/08/02 07:08:04 INFO Executor: Finished task 7.0 in stage 13.0 (TID 59). 1144 bytes result sent to driver
26/08/02 07:08:04 INFO TaskSetManager: Finished task 7.0 in stage 13.0 (TID 59) in 35 ms on 819c60c13514 (executor driver) (3/10)
26/08/02 07:08:04 INFO TaskSetManager: Finished task 6.0 in stage 13.0 (TID 58) in 35 ms on 819c60c13514 (executor driver) (4/10)
26/08/02 07:08:04 INFO Executor: Finished task 1.0 in stage 13.0 (TID 53). 1187 bytes result sent to driver
26/08/02 07:08:04 INFO Executor: Finished task 8.0 in stage 13.0 (TID 60). 1187 bytes result sent to driver
26/08/02 07:08:04 INFO TaskSetManager: Finished task 8.0 in stage 13.0 (TID 60) in 38 ms on 819c60c13514 (executor driver) (5/10)
26/08/02 07:08:04 INFO TaskSetManager: Finished task 1.0 in stage 13.0 (TID 53) in 39 ms on 819c60c13514 (executor driver) (6/10)
26/08/02 07:08:04 INFO Executor: Finished task 2.0 in stage 13.0 (TID 54). 1187 bytes result sent to driver
26/08/02 07:08:04 INFO TaskSetManager: Finished task 2.0 in stage 13.0 (TID 54) in 40 ms on 819c60c13514 (executor driver) (7/10)
26/08/02 07:08:04 INFO Executor: Finished task 9.0 in stage 13.0 (TID 61). 1187 bytes result sent to driver
26/08/02 07:08:04 INFO Executor: Finished task 5.0 in stage 13.0 (TID 57). 1187 bytes result sent to driver
26/08/02 07:08:04 INFO TaskSetManager: Finished task 9.0 in stage 13.0 (TID 61) in 41 ms on 819c60c13514 (executor driver) (8/10)
26/08/02 07:08:04 INFO Executor: Finished task 0.0 in stage 13.0 (TID 52). 1187 bytes result sent to driver
26/08/02 07:08:04 INFO TaskSetManager: Finished task 0.0 in stage 13.0 (TID 52) in 42 ms on 819c60c13514 (executor driver) (9/10)
26/08/02 07:08:04 INFO TaskSetManager: Finished task 5.0 in stage 13.0 (TID 57) in 41 ms on 819c60c13514 (executor driver) (10/10)
26/08/02 07:08:04 INFO TaskSchedulerImpl: Removed TaskSet 13.0, whose tasks have all completed, from pool 
26/08/02 07:08:04 INFO DAGScheduler: ResultStage 13 (checkpoint at NativeMethodAccessorImpl.java:0) finished in 0.046 s
26/08/02 07:08:04 INFO DAGScheduler: Job 7 is finished. Cancelling potential speculative or zombie tasks for this job
26/08/02 07:08:04 INFO TaskSchedulerImpl: Killing all running tasks in stage 13: Stage finished
26/08/02 07:08:04 INFO DAGScheduler: Job 7 finished: checkpoint at NativeMethodAccessorImpl.java:0, took 0.048202 s
26/08/02 07:08:04 INFO ReliableCheckpointRDD: Checkpointing took 55 ms.
26/08/02 07:08:04 INFO MemoryStore: Block broadcast_13 stored as values in memory (estimated size 221.7 KiB, free 433.2 MiB)
26/08/02 07:08:04 INFO MemoryStore: Block broadcast_13_piece0 stored as bytes in memory (estimated size 32.6 KiB, free 433.1 MiB)
26/08/02 07:08:04 INFO BlockManagerInfo: Added broadcast_13_piece0 in memory on 819c60c13514:44573 (size: 32.6 KiB, free: 434.2 MiB)
26/08/02 07:08:04 INFO SparkContext: Created broadcast 13 from checkpoint at NativeMethodAccessorImpl.java:0
26/08/02 07:08:04 INFO ReliableRDDCheckpointData: Done checkpointing RDD 26 to file:/app/data/checkpoints/d2653d35-6801-4a1d-bef5-159020f1139d/rdd-26, new parent is RDD 27
Iteration 20: Checkpointing RDD to truncate DAG liability...
26/08/02 07:08:04 INFO CodeGenerator: Code generated in 4.436166 ms
26/08/02 07:08:04 INFO MemoryStore: Block broadcast_14 stored as values in memory (estimated size 221.7 KiB, free 432.9 MiB)
26/08/02 07:08:04 INFO MemoryStore: Block broadcast_14_piece0 stored as bytes in memory (estimated size 32.6 KiB, free 432.9 MiB)
26/08/02 07:08:04 INFO BlockManagerInfo: Added broadcast_14_piece0 in memory on 819c60c13514:44573 (size: 32.6 KiB, free: 434.2 MiB)
26/08/02 07:08:04 INFO SparkContext: Created broadcast 14 from checkpoint at NativeMethodAccessorImpl.java:0
26/08/02 07:08:04 INFO SparkContext: Starting job: checkpoint at NativeMethodAccessorImpl.java:0
26/08/02 07:08:04 INFO DAGScheduler: Got job 8 (checkpoint at NativeMethodAccessorImpl.java:0) with 10 output partitions
26/08/02 07:08:04 INFO DAGScheduler: Final stage: ResultStage 14 (checkpoint at NativeMethodAccessorImpl.java:0)
26/08/02 07:08:04 INFO DAGScheduler: Parents of final stage: List()
26/08/02 07:08:04 INFO DAGScheduler: Missing parents: List()
26/08/02 07:08:04 INFO DAGScheduler: Submitting ResultStage 14 (MapPartitionsRDD[29] at checkpoint at NativeMethodAccessorImpl.java:0), which has no missing parents
26/08/02 07:08:04 INFO MemoryStore: Block broadcast_15 stored as values in memory (estimated size 14.6 KiB, free 432.9 MiB)
26/08/02 07:08:04 INFO MemoryStore: Block broadcast_15_piece0 stored as bytes in memory (estimated size 5.9 KiB, free 432.9 MiB)
26/08/02 07:08:04 INFO BlockManagerInfo: Added broadcast_15_piece0 in memory on 819c60c13514:44573 (size: 5.9 KiB, free: 434.2 MiB)
26/08/02 07:08:04 INFO SparkContext: Created broadcast 15 from broadcast at DAGScheduler.scala:1580
26/08/02 07:08:04 INFO DAGScheduler: Submitting 10 missing tasks from ResultStage 14 (MapPartitionsRDD[29] at checkpoint at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
26/08/02 07:08:04 INFO TaskSchedulerImpl: Adding task set 14.0 with 10 tasks resource profile 0
26/08/02 07:08:04 INFO TaskSetManager: Starting task 0.0 in stage 14.0 (TID 62) (819c60c13514, executor driver, partition 0, PROCESS_LOCAL, 7425 bytes) 
26/08/02 07:08:04 INFO TaskSetManager: Starting task 1.0 in stage 14.0 (TID 63) (819c60c13514, executor driver, partition 1, PROCESS_LOCAL, 7425 bytes) 
26/08/02 07:08:04 INFO TaskSetManager: Starting task 2.0 in stage 14.0 (TID 64) (819c60c13514, executor driver, partition 2, PROCESS_LOCAL, 7425 bytes) 
26/08/02 07:08:04 INFO TaskSetManager: Starting task 3.0 in stage 14.0 (TID 65) (819c60c13514, executor driver, partition 3, PROCESS_LOCAL, 7425 bytes) 
26/08/02 07:08:04 INFO TaskSetManager: Starting task 4.0 in stage 14.0 (TID 66) (819c60c13514, executor driver, partition 4, PROCESS_LOCAL, 7425 bytes) 
26/08/02 07:08:04 INFO TaskSetManager: Starting task 5.0 in stage 14.0 (TID 67) (819c60c13514, executor driver, partition 5, PROCESS_LOCAL, 7425 bytes) 
26/08/02 07:08:04 INFO TaskSetManager: Starting task 6.0 in stage 14.0 (TID 68) (819c60c13514, executor driver, partition 6, PROCESS_LOCAL, 7425 bytes) 
26/08/02 07:08:04 INFO TaskSetManager: Starting task 7.0 in stage 14.0 (TID 69) (819c60c13514, executor driver, partition 7, PROCESS_LOCAL, 7425 bytes) 
26/08/02 07:08:04 INFO TaskSetManager: Starting task 8.0 in stage 14.0 (TID 70) (819c60c13514, executor driver, partition 8, PROCESS_LOCAL, 7425 bytes) 
26/08/02 07:08:04 INFO TaskSetManager: Starting task 9.0 in stage 14.0 (TID 71) (819c60c13514, executor driver, partition 9, PROCESS_LOCAL, 7425 bytes) 
26/08/02 07:08:04 INFO Executor: Running task 4.0 in stage 14.0 (TID 66)
26/08/02 07:08:04 INFO Executor: Running task 3.0 in stage 14.0 (TID 65)
26/08/02 07:08:04 INFO Executor: Running task 1.0 in stage 14.0 (TID 63)
26/08/02 07:08:04 INFO Executor: Running task 2.0 in stage 14.0 (TID 64)
26/08/02 07:08:04 INFO Executor: Running task 0.0 in stage 14.0 (TID 62)
26/08/02 07:08:04 INFO Executor: Running task 8.0 in stage 14.0 (TID 70)
26/08/02 07:08:04 INFO Executor: Running task 9.0 in stage 14.0 (TID 71)
26/08/02 07:08:04 INFO Executor: Running task 6.0 in stage 14.0 (TID 68)
26/08/02 07:08:04 INFO Executor: Running task 7.0 in stage 14.0 (TID 69)
26/08/02 07:08:04 INFO Executor: Running task 5.0 in stage 14.0 (TID 67)
26/08/02 07:08:04 INFO CodeGenerator: Code generated in 3.519416 ms
26/08/02 07:08:04 INFO Executor: Finished task 4.0 in stage 14.0 (TID 66). 1187 bytes result sent to driver
26/08/02 07:08:04 INFO Executor: Finished task 3.0 in stage 14.0 (TID 65). 1187 bytes result sent to driver
26/08/02 07:08:04 INFO TaskSetManager: Finished task 4.0 in stage 14.0 (TID 66) in 28 ms on 819c60c13514 (executor driver) (1/10)
26/08/02 07:08:04 INFO TaskSetManager: Finished task 3.0 in stage 14.0 (TID 65) in 28 ms on 819c60c13514 (executor driver) (2/10)
26/08/02 07:08:04 INFO Executor: Finished task 1.0 in stage 14.0 (TID 63). 1187 bytes result sent to driver
26/08/02 07:08:04 INFO TaskSetManager: Finished task 1.0 in stage 14.0 (TID 63) in 29 ms on 819c60c13514 (executor driver) (3/10)
26/08/02 07:08:04 INFO Executor: Finished task 5.0 in stage 14.0 (TID 67). 1187 bytes result sent to driver
26/08/02 07:08:04 INFO TaskSetManager: Finished task 5.0 in stage 14.0 (TID 67) in 33 ms on 819c60c13514 (executor driver) (4/10)
26/08/02 07:08:04 INFO Executor: Finished task 7.0 in stage 14.0 (TID 69). 1187 bytes result sent to driver
26/08/02 07:08:04 INFO Executor: Finished task 0.0 in stage 14.0 (TID 62). 1187 bytes result sent to driver
26/08/02 07:08:04 INFO Executor: Finished task 2.0 in stage 14.0 (TID 64). 1187 bytes result sent to driver
26/08/02 07:08:04 INFO TaskSetManager: Finished task 0.0 in stage 14.0 (TID 62) in 36 ms on 819c60c13514 (executor driver) (5/10)
26/08/02 07:08:04 INFO TaskSetManager: Finished task 7.0 in stage 14.0 (TID 69) in 35 ms on 819c60c13514 (executor driver) (6/10)
26/08/02 07:08:04 INFO TaskSetManager: Finished task 2.0 in stage 14.0 (TID 64) in 36 ms on 819c60c13514 (executor driver) (7/10)
26/08/02 07:08:04 INFO Executor: Finished task 8.0 in stage 14.0 (TID 70). 1144 bytes result sent to driver
26/08/02 07:08:04 INFO Executor: Finished task 9.0 in stage 14.0 (TID 71). 1144 bytes result sent to driver
26/08/02 07:08:04 INFO Executor: Finished task 6.0 in stage 14.0 (TID 68). 1187 bytes result sent to driver
26/08/02 07:08:04 INFO TaskSetManager: Finished task 9.0 in stage 14.0 (TID 71) in 37 ms on 819c60c13514 (executor driver) (8/10)
26/08/02 07:08:04 INFO TaskSetManager: Finished task 8.0 in stage 14.0 (TID 70) in 37 ms on 819c60c13514 (executor driver) (9/10)
26/08/02 07:08:04 INFO TaskSetManager: Finished task 6.0 in stage 14.0 (TID 68) in 38 ms on 819c60c13514 (executor driver) (10/10)
26/08/02 07:08:04 INFO TaskSchedulerImpl: Removed TaskSet 14.0, whose tasks have all completed, from pool 
26/08/02 07:08:04 INFO DAGScheduler: ResultStage 14 (checkpoint at NativeMethodAccessorImpl.java:0) finished in 0.041 s
26/08/02 07:08:04 INFO DAGScheduler: Job 8 is finished. Cancelling potential speculative or zombie tasks for this job
26/08/02 07:08:04 INFO TaskSchedulerImpl: Killing all running tasks in stage 14: Stage finished
26/08/02 07:08:04 INFO DAGScheduler: Job 8 finished: checkpoint at NativeMethodAccessorImpl.java:0, took 0.043401 s
26/08/02 07:08:04 INFO ReliableCheckpointRDD: Checkpointing took 50 ms.
26/08/02 07:08:04 INFO MemoryStore: Block broadcast_16 stored as values in memory (estimated size 221.7 KiB, free 432.7 MiB)
26/08/02 07:08:04 INFO MemoryStore: Block broadcast_16_piece0 stored as bytes in memory (estimated size 32.6 KiB, free 432.6 MiB)
26/08/02 07:08:04 INFO BlockManagerInfo: Added broadcast_16_piece0 in memory on 819c60c13514:44573 (size: 32.6 KiB, free: 434.2 MiB)
26/08/02 07:08:04 INFO SparkContext: Created broadcast 16 from checkpoint at NativeMethodAccessorImpl.java:0
26/08/02 07:08:04 INFO ReliableRDDCheckpointData: Done checkpointing RDD 29 to file:/app/data/checkpoints/d2653d35-6801-4a1d-bef5-159020f1139d/rdd-29, new parent is RDD 30
Iterative processing complete. Final schema:
root
 |-- vehicle_model: string (nullable = true)
 |-- avg_engine_temp: double (nullable = true)
 |-- simulated_weight_1: double (nullable = true)
 |-- simulated_weight_2: double (nullable = true)
 |-- simulated_weight_3: double (nullable = true)
 |-- simulated_weight_4: double (nullable = true)
 |-- simulated_weight_5: double (nullable = true)
 |-- simulated_weight_6: double (nullable = true)
 |-- simulated_weight_7: double (nullable = true)
 |-- simulated_weight_8: double (nullable = true)
 |-- simulated_weight_9: double (nullable = true)
 |-- simulated_weight_10: double (nullable = true)
 |-- simulated_weight_11: double (nullable = true)
 |-- simulated_weight_12: double (nullable = true)
 |-- simulated_weight_13: double (nullable = true)
 |-- simulated_weight_14: double (nullable = true)
 |-- simulated_weight_15: double (nullable = true)
 |-- simulated_weight_16: double (nullable = true)
 |-- simulated_weight_17: double (nullable = true)
 |-- simulated_weight_18: double (nullable = true)
 |-- simulated_weight_19: double (nullable = true)
 |-- simulated_weight_20: double (nullable = true)

26/08/02 07:08:04 INFO ParquetUtils: Using default output committer for Parquet: org.apache.parquet.hadoop.ParquetOutputCommitter
26/08/02 07:08:04 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
26/08/02 07:08:04 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
26/08/02 07:08:04 INFO SQLHadoopMapReduceCommitProtocol: Using user defined output committer class org.apache.parquet.hadoop.ParquetOutputCommitter
26/08/02 07:08:04 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
26/08/02 07:08:04 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
26/08/02 07:08:04 INFO SQLHadoopMapReduceCommitProtocol: Using output committer class org.apache.parquet.hadoop.ParquetOutputCommitter
26/08/02 07:08:04 INFO CodeGenerator: Code generated in 3.164458 ms
26/08/02 07:08:04 INFO SparkContext: Starting job: parquet at NativeMethodAccessorImpl.java:0
26/08/02 07:08:04 INFO DAGScheduler: Got job 9 (parquet at NativeMethodAccessorImpl.java:0) with 10 output partitions
26/08/02 07:08:04 INFO DAGScheduler: Final stage: ResultStage 15 (parquet at NativeMethodAccessorImpl.java:0)
26/08/02 07:08:04 INFO DAGScheduler: Parents of final stage: List()
26/08/02 07:08:04 INFO DAGScheduler: Missing parents: List()
26/08/02 07:08:04 INFO DAGScheduler: Submitting ResultStage 15 (MapPartitionsRDD[32] at parquet at NativeMethodAccessorImpl.java:0), which has no missing parents
26/08/02 07:08:04 INFO MemoryStore: Block broadcast_17 stored as values in memory (estimated size 218.7 KiB, free 432.4 MiB)
26/08/02 07:08:04 INFO MemoryStore: Block broadcast_17_piece0 stored as bytes in memory (estimated size 77.3 KiB, free 432.3 MiB)
26/08/02 07:08:04 INFO BlockManagerInfo: Added broadcast_17_piece0 in memory on 819c60c13514:44573 (size: 77.3 KiB, free: 434.1 MiB)
26/08/02 07:08:04 INFO SparkContext: Created broadcast 17 from broadcast at DAGScheduler.scala:1580
26/08/02 07:08:04 INFO DAGScheduler: Submitting 10 missing tasks from ResultStage 15 (MapPartitionsRDD[32] at parquet at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
26/08/02 07:08:04 INFO TaskSchedulerImpl: Adding task set 15.0 with 10 tasks resource profile 0
26/08/02 07:08:04 INFO TaskSetManager: Starting task 0.0 in stage 15.0 (TID 72) (819c60c13514, executor driver, partition 0, PROCESS_LOCAL, 7425 bytes) 
26/08/02 07:08:04 INFO TaskSetManager: Starting task 1.0 in stage 15.0 (TID 73) (819c60c13514, executor driver, partition 1, PROCESS_LOCAL, 7425 bytes) 
26/08/02 07:08:04 INFO TaskSetManager: Starting task 2.0 in stage 15.0 (TID 74) (819c60c13514, executor driver, partition 2, PROCESS_LOCAL, 7425 bytes) 
26/08/02 07:08:04 INFO TaskSetManager: Starting task 3.0 in stage 15.0 (TID 75) (819c60c13514, executor driver, partition 3, PROCESS_LOCAL, 7425 bytes) 
26/08/02 07:08:04 INFO TaskSetManager: Starting task 4.0 in stage 15.0 (TID 76) (819c60c13514, executor driver, partition 4, PROCESS_LOCAL, 7425 bytes) 
26/08/02 07:08:04 INFO TaskSetManager: Starting task 5.0 in stage 15.0 (TID 77) (819c60c13514, executor driver, partition 5, PROCESS_LOCAL, 7425 bytes) 
26/08/02 07:08:04 INFO TaskSetManager: Starting task 6.0 in stage 15.0 (TID 78) (819c60c13514, executor driver, partition 6, PROCESS_LOCAL, 7425 bytes) 
26/08/02 07:08:04 INFO TaskSetManager: Starting task 7.0 in stage 15.0 (TID 79) (819c60c13514, executor driver, partition 7, PROCESS_LOCAL, 7425 bytes) 
26/08/02 07:08:04 INFO TaskSetManager: Starting task 8.0 in stage 15.0 (TID 80) (819c60c13514, executor driver, partition 8, PROCESS_LOCAL, 7425 bytes) 
26/08/02 07:08:04 INFO BlockManagerInfo: Removed broadcast_15_piece0 on 819c60c13514:44573 in memory (size: 5.9 KiB, free: 434.1 MiB)
26/08/02 07:08:04 INFO TaskSetManager: Starting task 9.0 in stage 15.0 (TID 81) (819c60c13514, executor driver, partition 9, PROCESS_LOCAL, 7425 bytes) 
26/08/02 07:08:04 INFO Executor: Running task 6.0 in stage 15.0 (TID 78)
26/08/02 07:08:04 INFO Executor: Running task 9.0 in stage 15.0 (TID 81)
26/08/02 07:08:04 INFO Executor: Running task 2.0 in stage 15.0 (TID 74)
26/08/02 07:08:04 INFO Executor: Running task 8.0 in stage 15.0 (TID 80)
26/08/02 07:08:04 INFO Executor: Running task 0.0 in stage 15.0 (TID 72)
26/08/02 07:08:04 INFO Executor: Running task 1.0 in stage 15.0 (TID 73)
26/08/02 07:08:04 INFO Executor: Running task 3.0 in stage 15.0 (TID 75)
26/08/02 07:08:04 INFO Executor: Running task 4.0 in stage 15.0 (TID 76)
26/08/02 07:08:04 INFO Executor: Running task 5.0 in stage 15.0 (TID 77)
26/08/02 07:08:04 INFO Executor: Running task 7.0 in stage 15.0 (TID 79)
26/08/02 07:08:04 INFO BlockManagerInfo: Removed broadcast_8_piece0 on 819c60c13514:44573 in memory (size: 32.6 KiB, free: 434.1 MiB)
26/08/02 07:08:04 INFO BlockManagerInfo: Removed broadcast_12_piece0 on 819c60c13514:44573 in memory (size: 5.6 KiB, free: 434.1 MiB)
26/08/02 07:08:04 INFO BlockManagerInfo: Removed broadcast_14_piece0 on 819c60c13514:44573 in memory (size: 32.6 KiB, free: 434.2 MiB)
26/08/02 07:08:04 INFO BlockManagerInfo: Removed broadcast_11_piece0 on 819c60c13514:44573 in memory (size: 32.6 KiB, free: 434.2 MiB)
26/08/02 07:08:04 INFO CodeGenerator: Code generated in 3.997375 ms
26/08/02 07:08:04 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
26/08/02 07:08:04 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
26/08/02 07:08:04 INFO SQLHadoopMapReduceCommitProtocol: Using user defined output committer class org.apache.parquet.hadoop.ParquetOutputCommitter
26/08/02 07:08:04 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
26/08/02 07:08:04 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
26/08/02 07:08:04 INFO SQLHadoopMapReduceCommitProtocol: Using output committer class org.apache.parquet.hadoop.ParquetOutputCommitter
26/08/02 07:08:04 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
26/08/02 07:08:04 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
26/08/02 07:08:04 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
26/08/02 07:08:04 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
26/08/02 07:08:04 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
26/08/02 07:08:04 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
26/08/02 07:08:04 INFO SQLHadoopMapReduceCommitProtocol: Using user defined output committer class org.apache.parquet.hadoop.ParquetOutputCommitter
26/08/02 07:08:04 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
26/08/02 07:08:04 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
26/08/02 07:08:04 INFO SQLHadoopMapReduceCommitProtocol: Using user defined output committer class org.apache.parquet.hadoop.ParquetOutputCommitter
26/08/02 07:08:04 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
26/08/02 07:08:04 INFO SQLHadoopMapReduceCommitProtocol: Using output committer class org.apache.parquet.hadoop.ParquetOutputCommitter
26/08/02 07:08:04 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
26/08/02 07:08:04 INFO SQLHadoopMapReduceCommitProtocol: Using user defined output committer class org.apache.parquet.hadoop.ParquetOutputCommitter
26/08/02 07:08:04 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
26/08/02 07:08:04 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
26/08/02 07:08:04 INFO SQLHadoopMapReduceCommitProtocol: Using output committer class org.apache.parquet.hadoop.ParquetOutputCommitter
26/08/02 07:08:04 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
26/08/02 07:08:04 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
26/08/02 07:08:04 INFO SQLHadoopMapReduceCommitProtocol: Using user defined output committer class org.apache.parquet.hadoop.ParquetOutputCommitter
26/08/02 07:08:04 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
26/08/02 07:08:04 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
26/08/02 07:08:04 INFO SQLHadoopMapReduceCommitProtocol: Using output committer class org.apache.parquet.hadoop.ParquetOutputCommitter
26/08/02 07:08:04 INFO SQLHadoopMapReduceCommitProtocol: Using output committer class org.apache.parquet.hadoop.ParquetOutputCommitter
26/08/02 07:08:04 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
26/08/02 07:08:04 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
26/08/02 07:08:04 INFO SQLHadoopMapReduceCommitProtocol: Using user defined output committer class org.apache.parquet.hadoop.ParquetOutputCommitter
26/08/02 07:08:04 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
26/08/02 07:08:04 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
26/08/02 07:08:04 INFO SQLHadoopMapReduceCommitProtocol: Using output committer class org.apache.parquet.hadoop.ParquetOutputCommitter
26/08/02 07:08:04 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
26/08/02 07:08:04 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
26/08/02 07:08:04 INFO SQLHadoopMapReduceCommitProtocol: Using user defined output committer class org.apache.parquet.hadoop.ParquetOutputCommitter
26/08/02 07:08:04 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
26/08/02 07:08:04 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
26/08/02 07:08:04 INFO SQLHadoopMapReduceCommitProtocol: Using output committer class org.apache.parquet.hadoop.ParquetOutputCommitter
26/08/02 07:08:04 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
26/08/02 07:08:04 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
26/08/02 07:08:04 INFO SQLHadoopMapReduceCommitProtocol: Using user defined output committer class org.apache.parquet.hadoop.ParquetOutputCommitter
26/08/02 07:08:04 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
26/08/02 07:08:04 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
26/08/02 07:08:04 INFO SQLHadoopMapReduceCommitProtocol: Using output committer class org.apache.parquet.hadoop.ParquetOutputCommitter
26/08/02 07:08:04 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
26/08/02 07:08:04 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
26/08/02 07:08:04 INFO SQLHadoopMapReduceCommitProtocol: Using user defined output committer class org.apache.parquet.hadoop.ParquetOutputCommitter
26/08/02 07:08:04 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
26/08/02 07:08:04 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
26/08/02 07:08:04 INFO SQLHadoopMapReduceCommitProtocol: Using output committer class org.apache.parquet.hadoop.ParquetOutputCommitter
26/08/02 07:08:04 INFO CodecConfig: Compression: SNAPPY
26/08/02 07:08:04 INFO SparkHadoopMapRedUtil: No need to commit output of task because needsTaskCommit=false: attempt_202608020708048655190825848705669_0015_m_000003_75
26/08/02 07:08:04 INFO CodecConfig: Compression: SNAPPY
26/08/02 07:08:04 INFO CodecConfig: Compression: SNAPPY
26/08/02 07:08:04 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
26/08/02 07:08:04 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
26/08/02 07:08:04 INFO SQLHadoopMapReduceCommitProtocol: Using user defined output committer class org.apache.parquet.hadoop.ParquetOutputCommitter
26/08/02 07:08:04 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
26/08/02 07:08:04 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
26/08/02 07:08:04 INFO SparkHadoopMapRedUtil: No need to commit output of task because needsTaskCommit=false: attempt_202608020708048655190825848705669_0015_m_000008_80
26/08/02 07:08:04 INFO SQLHadoopMapReduceCommitProtocol: Using output committer class org.apache.parquet.hadoop.ParquetOutputCommitter
26/08/02 07:08:04 INFO SparkHadoopMapRedUtil: No need to commit output of task because needsTaskCommit=false: attempt_202608020708048655190825848705669_0015_m_000007_79
26/08/02 07:08:04 INFO SparkHadoopMapRedUtil: No need to commit output of task because needsTaskCommit=false: attempt_202608020708048655190825848705669_0015_m_000005_77
26/08/02 07:08:04 INFO SparkHadoopMapRedUtil: No need to commit output of task because needsTaskCommit=false: attempt_202608020708048655190825848705669_0015_m_000002_74
26/08/02 07:08:04 INFO SparkHadoopMapRedUtil: No need to commit output of task because needsTaskCommit=false: attempt_202608020708048655190825848705669_0015_m_000001_73
26/08/02 07:08:04 INFO CodecConfig: Compression: SNAPPY
26/08/02 07:08:04 INFO CodecConfig: Compression: SNAPPY
26/08/02 07:08:04 INFO CodecConfig: Compression: SNAPPY
26/08/02 07:08:04 INFO SparkHadoopMapRedUtil: No need to commit output of task because needsTaskCommit=false: attempt_202608020708048655190825848705669_0015_m_000004_76
26/08/02 07:08:04 INFO Executor: Finished task 2.0 in stage 15.0 (TID 74). 2398 bytes result sent to driver
26/08/02 07:08:04 INFO Executor: Finished task 3.0 in stage 15.0 (TID 75). 2398 bytes result sent to driver
26/08/02 07:08:04 INFO Executor: Finished task 5.0 in stage 15.0 (TID 77). 2398 bytes result sent to driver
26/08/02 07:08:04 INFO Executor: Finished task 7.0 in stage 15.0 (TID 79). 2398 bytes result sent to driver
26/08/02 07:08:04 INFO Executor: Finished task 1.0 in stage 15.0 (TID 73). 2398 bytes result sent to driver
26/08/02 07:08:04 INFO Executor: Finished task 4.0 in stage 15.0 (TID 76). 2398 bytes result sent to driver
26/08/02 07:08:04 INFO TaskSetManager: Finished task 2.0 in stage 15.0 (TID 74) in 101 ms on 819c60c13514 (executor driver) (1/10)
26/08/02 07:08:04 INFO Executor: Finished task 8.0 in stage 15.0 (TID 80). 2398 bytes result sent to driver
26/08/02 07:08:04 INFO TaskSetManager: Finished task 4.0 in stage 15.0 (TID 76) in 102 ms on 819c60c13514 (executor driver) (2/10)
26/08/02 07:08:04 INFO TaskSetManager: Finished task 8.0 in stage 15.0 (TID 80) in 102 ms on 819c60c13514 (executor driver) (3/10)
26/08/02 07:08:04 INFO TaskSetManager: Finished task 3.0 in stage 15.0 (TID 75) in 103 ms on 819c60c13514 (executor driver) (4/10)
26/08/02 07:08:04 INFO TaskSetManager: Finished task 5.0 in stage 15.0 (TID 77) in 103 ms on 819c60c13514 (executor driver) (5/10)
26/08/02 07:08:04 INFO TaskSetManager: Finished task 1.0 in stage 15.0 (TID 73) in 109 ms on 819c60c13514 (executor driver) (6/10)
26/08/02 07:08:04 INFO TaskSetManager: Finished task 7.0 in stage 15.0 (TID 79) in 103 ms on 819c60c13514 (executor driver) (7/10)
26/08/02 07:08:04 INFO ParquetOutputFormat: ParquetRecordWriter [block size: 134217728b, row group padding size: 8388608b, validating: false]
26/08/02 07:08:04 INFO ParquetOutputFormat: ParquetRecordWriter [block size: 134217728b, row group padding size: 8388608b, validating: false]
26/08/02 07:08:04 INFO ParquetOutputFormat: ParquetRecordWriter [block size: 134217728b, row group padding size: 8388608b, validating: false]
26/08/02 07:08:04 INFO ParquetWriteSupport: Initialized Parquet WriteSupport with Catalyst schema:
{
  "type" : "struct",
  "fields" : [ {
    "name" : "vehicle_model",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "avg_engine_temp",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_1",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_2",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_3",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_4",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_5",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_6",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_7",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_8",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_9",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_10",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_11",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_12",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_13",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_14",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_15",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_16",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_17",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_18",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_19",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_20",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  } ]
}
and corresponding Parquet message type:
message spark_schema {
  optional binary vehicle_model (STRING);
  optional double avg_engine_temp;
  optional double simulated_weight_1;
  optional double simulated_weight_2;
  optional double simulated_weight_3;
  optional double simulated_weight_4;
  optional double simulated_weight_5;
  optional double simulated_weight_6;
  optional double simulated_weight_7;
  optional double simulated_weight_8;
  optional double simulated_weight_9;
  optional double simulated_weight_10;
  optional double simulated_weight_11;
  optional double simulated_weight_12;
  optional double simulated_weight_13;
  optional double simulated_weight_14;
  optional double simulated_weight_15;
  optional double simulated_weight_16;
  optional double simulated_weight_17;
  optional double simulated_weight_18;
  optional double simulated_weight_19;
  optional double simulated_weight_20;
}

       
26/08/02 07:08:04 INFO ParquetWriteSupport: Initialized Parquet WriteSupport with Catalyst schema:
{
  "type" : "struct",
  "fields" : [ {
    "name" : "vehicle_model",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "avg_engine_temp",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_1",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_2",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_3",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_4",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_5",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_6",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_7",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_8",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_9",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_10",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_11",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_12",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_13",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_14",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_15",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_16",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_17",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_18",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_19",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_20",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  } ]
}
and corresponding Parquet message type:
message spark_schema {
  optional binary vehicle_model (STRING);
  optional double avg_engine_temp;
  optional double simulated_weight_1;
  optional double simulated_weight_2;
  optional double simulated_weight_3;
  optional double simulated_weight_4;
  optional double simulated_weight_5;
  optional double simulated_weight_6;
  optional double simulated_weight_7;
  optional double simulated_weight_8;
  optional double simulated_weight_9;
  optional double simulated_weight_10;
  optional double simulated_weight_11;
  optional double simulated_weight_12;
  optional double simulated_weight_13;
  optional double simulated_weight_14;
  optional double simulated_weight_15;
  optional double simulated_weight_16;
  optional double simulated_weight_17;
  optional double simulated_weight_18;
  optional double simulated_weight_19;
  optional double simulated_weight_20;
}

       
26/08/02 07:08:04 INFO ParquetWriteSupport: Initialized Parquet WriteSupport with Catalyst schema:
{
  "type" : "struct",
  "fields" : [ {
    "name" : "vehicle_model",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "avg_engine_temp",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_1",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_2",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_3",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_4",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_5",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_6",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_7",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_8",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_9",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_10",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_11",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_12",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_13",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_14",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_15",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_16",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_17",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_18",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_19",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "simulated_weight_20",
    "type" : "double",
    "nullable" : true,
    "metadata" : { }
  } ]
}
and corresponding Parquet message type:
message spark_schema {
  optional binary vehicle_model (STRING);
  optional double avg_engine_temp;
  optional double simulated_weight_1;
  optional double simulated_weight_2;
  optional double simulated_weight_3;
  optional double simulated_weight_4;
  optional double simulated_weight_5;
  optional double simulated_weight_6;
  optional double simulated_weight_7;
  optional double simulated_weight_8;
  optional double simulated_weight_9;
  optional double simulated_weight_10;
  optional double simulated_weight_11;
  optional double simulated_weight_12;
  optional double simulated_weight_13;
  optional double simulated_weight_14;
  optional double simulated_weight_15;
  optional double simulated_weight_16;
  optional double simulated_weight_17;
  optional double simulated_weight_18;
  optional double simulated_weight_19;
  optional double simulated_weight_20;
}

       
26/08/02 07:08:04 INFO CodecPool: Got brand-new compressor [.snappy]
26/08/02 07:08:04 INFO CodecPool: Got brand-new compressor [.snappy]
26/08/02 07:08:04 INFO CodecPool: Got brand-new compressor [.snappy]
26/08/02 07:08:04 INFO FileOutputCommitter: Saved output of task 'attempt_202608020708048655190825848705669_0015_m_000000_72' to file:/app/data/processed/telemetry_avg_temp/_temporary/0/task_202608020708048655190825848705669_0015_m_000000
26/08/02 07:08:04 INFO SparkHadoopMapRedUtil: attempt_202608020708048655190825848705669_0015_m_000000_72: Committed. Elapsed time: 0 ms.
26/08/02 07:08:04 INFO FileOutputCommitter: Saved output of task 'attempt_202608020708048655190825848705669_0015_m_000009_81' to file:/app/data/processed/telemetry_avg_temp/_temporary/0/task_202608020708048655190825848705669_0015_m_000009
26/08/02 07:08:04 INFO SparkHadoopMapRedUtil: attempt_202608020708048655190825848705669_0015_m_000009_81: Committed. Elapsed time: 0 ms.
26/08/02 07:08:04 INFO Executor: Finished task 0.0 in stage 15.0 (TID 72). 2398 bytes result sent to driver
26/08/02 07:08:04 INFO FileOutputCommitter: Saved output of task 'attempt_202608020708048655190825848705669_0015_m_000006_78' to file:/app/data/processed/telemetry_avg_temp/_temporary/0/task_202608020708048655190825848705669_0015_m_000006
26/08/02 07:08:04 INFO Executor: Finished task 9.0 in stage 15.0 (TID 81). 2441 bytes result sent to driver
26/08/02 07:08:04 INFO SparkHadoopMapRedUtil: attempt_202608020708048655190825848705669_0015_m_000006_78: Committed. Elapsed time: 1 ms.
26/08/02 07:08:04 INFO TaskSetManager: Finished task 0.0 in stage 15.0 (TID 72) in 272 ms on 819c60c13514 (executor driver) (8/10)
26/08/02 07:08:04 INFO Executor: Finished task 6.0 in stage 15.0 (TID 78). 2441 bytes result sent to driver
26/08/02 07:08:04 INFO TaskSetManager: Finished task 9.0 in stage 15.0 (TID 81) in 266 ms on 819c60c13514 (executor driver) (9/10)
26/08/02 07:08:04 INFO TaskSetManager: Finished task 6.0 in stage 15.0 (TID 78) in 266 ms on 819c60c13514 (executor driver) (10/10)
26/08/02 07:08:04 INFO TaskSchedulerImpl: Removed TaskSet 15.0, whose tasks have all completed, from pool 
26/08/02 07:08:04 INFO DAGScheduler: ResultStage 15 (parquet at NativeMethodAccessorImpl.java:0) finished in 0.284 s
26/08/02 07:08:04 INFO DAGScheduler: Job 9 is finished. Cancelling potential speculative or zombie tasks for this job
26/08/02 07:08:04 INFO TaskSchedulerImpl: Killing all running tasks in stage 15: Stage finished
26/08/02 07:08:04 INFO DAGScheduler: Job 9 finished: parquet at NativeMethodAccessorImpl.java:0, took 0.285444 s
26/08/02 07:08:04 INFO FileFormatWriter: Start to commit write Job b29bf551-e75f-4293-89d3-2fdaf7490668.
26/08/02 07:08:05 INFO FileFormatWriter: Write Job b29bf551-e75f-4293-89d3-2fdaf7490668 committed. Elapsed time: 8 ms.
26/08/02 07:08:05 INFO FileFormatWriter: Finished processing stats for write job b29bf551-e75f-4293-89d3-2fdaf7490668.
Data successfully written to /app/data/processed/telemetry_avg_temp
26/08/02 07:08:05 INFO SparkContext: SparkContext is stopping with exitCode 0.
26/08/02 07:08:05 INFO SparkUI: Stopped Spark web UI at http://819c60c13514:4040
26/08/02 07:08:05 INFO MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
26/08/02 07:08:05 INFO MemoryStore: MemoryStore cleared
26/08/02 07:08:05 INFO BlockManager: BlockManager stopped
26/08/02 07:08:05 INFO BlockManagerMaster: BlockManagerMaster stopped
26/08/02 07:08:05 INFO OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
26/08/02 07:08:05 INFO SparkContext: Successfully stopped SparkContext
26/08/02 07:08:05 INFO ShutdownHookManager: Shutdown hook called
26/08/02 07:08:05 INFO ShutdownHookManager: Deleting directory /tmp/spark-0b821265-103d-4c51-8c7b-a1d7ff32a695
26/08/02 07:08:05 INFO ShutdownHookManager: Deleting directory /tmp/spark-eaba8ca9-45bd-4253-82a8-ea4b1a593612/pyspark-f9460243-d67c-4f29-9932-719eb1002ebb
26/08/02 07:08:05 INFO ShutdownHookManager: Deleting directory /tmp/spark-eaba8ca9-45bd-4253-82a8-ea4b1a593612
root@819c60c13514:/app# 
```


