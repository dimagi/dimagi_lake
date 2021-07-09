# Dimagi Lake
Repository contains the code for:
 - Forms, Cases and Location ingestion into the Datalake
 - Data aggregation Code for Data warehouse


## Menu
1. How It Works
2. Dependencies
3. Scope
4. How to setup Dimagi Lake
5. Spark Thrift Server
6. BI Tool
6. FAQ



## How It Works
This is an Spark Application Which is used for two purposes:
- To ingest Form and Case Data into the Datalake. It runs as a services and starts a Kafka Stream to periodically Pull ChangeMeta from kafka. Following Points explains how it works:
    1. It starts a Kafka stream using Spark Structure Steaming
    2. Pulls `<=MAX_RECORDS_TO_PROCESS` number of messages from Kafka in a single Batch. Constant `MAX_RECORDS_TO_PROCESS` in settings.py file can be set to virtually any value depending on the resources available.
    3. After Pulling ChangeMeta from Kafka. Split the Changes into different `domain` and `subtype`.
    4. Pull The real records from HQ Postgres using `CaseAccessors` and `FormAccessors` of HQ Code.
    5. Location Information is added to the records for better downstream analysis.
    6. Records are flattened. Flattening is required becaused of a potential [bug](https://github.com/delta-io/delta/issues/170#issuecomment-784156234) present in a library we use.
    7. If its a new type of form/case, then a new corresponding table is created in the metastore and it's data is directly written to the storage on a new location `\commcare\<domain>\<kafka_topic>\<case_type\form_xmlns>`
    8. For records whose corresponding table is already present in the metastore, the records are then Merged with the existing Data in the Datalake. Merging happens using Upsert Operation Using a spark library called [Delta Lake](https://docs.delta.io/latest/delta-intro.html). This makes the Data merging Possible. 
    9. We are only keeping the latest copy of records in the Datalake. When a case is udpated for example, Only the latest copy of case is maintained in the Datalake. Older versions can also be accessed if we want. But, since the use case is only to have the latest copy, we can remove the Older version periodically to free up space.
    10. To resolve [Small Files Problem](https://www.upsolver.com/blog/small-file-problem-hdfs-s3) in Datalake, Data would be periodically be re-partitioned to reduce the number of files. It would make the data query faster.
- To aggregate the Data for Data warehouse(In Progress). Each of the table aggregation would start as a separate spark application with `spark-submit` command based on different arguments you pass while invoking it. General flow of aggregation for a table is as follows:
    1. It reads data from input tables in Datalake and load it as spark Dataframe.
    2. Perform the aggregation on the dataframe based on the business logic.
    3. Write the data back to Datalake storage.
    4. Step 1 to 3 repeats for different tables. At last when all the steps of aggregation are complete, load the data from Datalake Storage to Data warehouse storage which is Postgres. Dashboard will use this postgres to seek its Data.


## Dependencies
1. **Python3**: Its written in Python3
2. **Java8**: Used by Spark.
3. **Scala 1.12**: Used by Spark.
2. **Spark 3.0.2**: Spark is used as a processing engine to process the incoming data.
3. **Object Storage/HDFS(3.2)**: Used to store the Data. Right now using S3. The repository can also be used with Local Filesystem for local setup
4. **Commcare Hq**: It uses CommcareHq code to pull Forms/cases records, Locations, user info from HQ Storage
5. **Postgres**: Postgres is used as a hive metastore to store hive table tables.


## Scope:

- [x] Capture New Forms and Cases(has to be added to the `ALLOWED_LIST`)
- [x] Capture Case updates
- [x] Capture republish of Forms and Cases
- [x] Allows on-the-fly custom transformation before saving records.
- [x] Allows field addition and deletion in cases/forms
- [x] Capture Form deletes
- [x] Capture case deletes
- [x] Capture Location Changes
- [x] Allow Data aggregation(In progress, Half way through)
- [ ] Allows Field datatype change in cases/forms during ingestion


## How to Setup

### Install the prerequisite
1. Python3
2. Java8
9. Scala 1.12

### Download Spark
Install `Spark 3.0.2, pre-built for apache hadoop 3.2 and later` version from the following link. This specific version is important.

https://spark.apache.org/downloads.html

### Setup Spark

**On Mac using Brew (recommended if you have a Mac)**
1. Run:
    ```
    brew cask install java;
    brew install scala;
    brew install apache-spark
    ```

**On others using Binary package**
1. Download
    ```
    https://mirrors.estointernet.in/apache/spark/spark-3.0.2/spark-3.0.2-bin-hadoop3.2.tgz
    ```

2. Extract
    ```
    tar -xvf spark-3.0.2-bin-hadoop3.2.tgz
    ```

3. shift
    ```
    sudo mv spark-3.0.2-bin-hadoop3.2/ /usr/local/spark/
    ```

4. Set environment variable
    ```
    export SPARK_HOME=path/to/where/spark/is/installed #/usr/local/Cellar/apache-spark/3.0.1/libexec for mac
    export PATH=<path/to/where/spark/is/installed>/bin #/usr/local/Cellar/apache-spark/3.0.1/bin:$PATH for mac
    ```

5. Confirm everything went fine
    ```
    pyspark --version
    ```

6. Download required repository to connect to S3(Dont need for local setup)
    ```
    cd /usr/local/spark/jars;
    wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.0/hadoop-aws-3.2.0.jar;
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.375/aws-java-sdk-bundle-1.11.375.jar;
    
    ```
7. Add the following line in `/usr/local/spark/conf/spark-env.sh`
    ```
    # Its for Ubuntu, for other platforms JavaHome could be different
    export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/  
    ```

### Setup Hive Metastore
Metastore is used to store the metatables for the data you are writing to the datalake using Spark. Hive metastore also speed ups the queries it stores all the meta information about the data like Columns, Partitions etc. By default spark stores the meta info into the local disk and in the directory from where you are running the spark application. But it has two problems:
1. It would be difficult to share the meta information between spark applications. Eg: If Aggregation wants to access the tables created by Datalake ingestion. It can directly access from storage but not through Metastore.
2. Storing the data directly into local disk might not be safe and we would have to separately manage its appropriate availability and security.

So, we will setup a central hive metastore using postgres and tell Spark to use that as a metastore.

1. Install Postgres. You can also use HQ postgres for your local setup.
2. Create a Database named `metastore_db`
3. Download following Migration files:
    ```
    wget https://raw.githubusercontent.com/apache/hive/master/metastore/scripts/upgrade/postgres/hive-schema-2.3.0.postgres.sql;

    wget https://raw.githubusercontent.com/apache/hive/master/metastore/scripts/upgrade/postgres/hive-txn-schema-2.3.0.postgres.sql
    ```
4. Change `\i hive-txn-schema-2.3.0.postgres.sql;` to `\i absolute/path/to/hive-txn-schema-2.3.0.postgres.sql;` in hive-schema-2.3.0.postgres.sql.

5. Login into Postgres and switch to `metastore_db`.
6. Run `\i absolute/path/to/hive-schema-2.3.0.postgres.sql`.
7. Create a file `hive-site.xml` in `$SPARK_HOME/conf/` and add the following content and fill the appropriate creds.
    ```
    <?xml version="1.0" encoding="UTF-8"?>
    <configuration>
       <property>
          <name>javax.jdo.option.ConnectionURL</name>
          <value>jdbc:postgresql://{host}:{ip}/metastore_db?ceateDatabaseIfNotExist=true</value>
          <description>JDBC connect string for a JDBC metastore</description>
       </property>
       <property>
          <name>javax.jdo.option.ConnectionDriverName</name>
          <value>org.postgresql.Driver</value>
          <description>Driver class name for a JDBC metastore</description>
       </property>
       <property>
          <name>javax.jdo.option.ConnectionUserName</name>
          <value>{username}</value>
       </property>
       <property>
          <name>javax.jdo.option.ConnectionPassword</name>
          <value>{password}</value>
       </property>
       <property>
      <name>datanucleus.autoCreateSchema</name>
      <value>false</value>
    </property>
    <property>
    <name>hive.metastore.schema.verification</name>
    <value>true</value>
    </property>
    </configuration>
    ```
5. Download Postgres driver Jar from here https://jdbc.postgresql.org/download.html and put it in $SPARK_HOME/jars directory.

Your Hive Metastore is now ready.

### Integration with HQ Codebase
At the moment Dimagi lake reuses HQ code to pull out the records for the given ids. Therefore, it depends on the HQ code base and hence it uses same python environment as HQ and should also know where the HQ code is residing in the system.

1. Setup the HQ code on the machine. this machine does not need to run any HQ service but HQ codebase should point to the right place where these services are running.
2. Add the following line in `/usr/local/spark/conf/spark-env.sh`
    ```
    export PYTHONPATH=path/to/commcare-hq_code:path/to/dimagi_lake/home_directory
    ```
3. Add the following in the `~/.bash_profile` or `~/.bashrc` file so that pyspark can use same python as hq.
    ```
    export PYSPARK_DRIVER_PYTHON='/Users/dsi/projects/commcare-hq/venv/bin/python3'
    export PYSPARK_PYTHON=/Users/dsi/projects/commcare-hq/venv/bin/python3
    ```

### Update Spark Default Setting(No need for Local setup)
Open `/usr/local/spark/conf/spark-defaults.conf` and add the following settings
```
# For S3. Not required if S3 is not used 
spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version 2
spark.speculation false
spark.hadoop.fs.s3a.fast.upload true
spark.com.amazonaws.services.s3.enableV4 true
spark.hadoop.fs.s3a.impl    org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.access.key {AWS_Access_Key}
spark.hadoop.fs.s3a.secret.key {AWS_Secret_key}

# For Dynamic Resource Allocation
spark.shuffle.service.enabled true

```
### Update Dimagi lake Settings
You will have to add the environment varilable to update the config for:
1. Kafka creds
2. Metastore DB Creds : where Meta tables will be stored
3. Dashboard DB Creds : Where Aggregation output will be pushed.
4. Storage Location directory: Where Actuall data will be stored. 


Following are the environment variables:
```
export KAFKA_BOOTSTRAP_SERVER_HOST=localhost
export KAFKA_BOOTSTRAP_SERVER_PORT=9092

export METASTORE_HOST=localhost
export METASTORE_PORT=5432
export METASTORE_DB=metastore_db
export METASTORE_USERNAME=commcarehq
export METASTORE_PASSWORD=commcarehq

export DASHBOARD_DB_HOST=localhost
export DASHBOARD_DB_PORT=5432
export DASHBOARD_DB_NAME=metastore_db
export DASHBOARD_DB_USERNAME=commcarehq
export DASHBOARD_DB_PASSWORD=commcarehq

#Below locations should be good for local setup. Change below values appropriately based on what location and what storage you want to use on server.

export CHECKPOINT_BASE_DIR=file:///tmp/dimagi-lake/kafka_offsets
export HQ_DATA_PATH=file:///tmp/dimagi-lake/kafka_offsets
export AGG_DATA_PATH=file:///tmp/dimagi-lake/kafka_offsets
```

### Update Allowed List in consts.py
1. Ensure the domain you are submitting forms from is added to the `DATA_LAKE_DOMAIN` list
2. Ensure the Forms and cases you are submitting are added to the `ALLOWED_FORMS` and `ALLOWED_CASES` list

### Running dimagi lake

1. For ingestion
```
spark-submit --packages "io.delta:delta-core_2.12:0.8.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --files application_config.yaml <path/to/dimagi_lake>/main.py ingestion <kafka-topic>
```

2. For aggregation
```
spark-submit --packages "io.delta:delta-core_2.12:0.8.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --files application_config.yaml <path/to/dimagi_lake>/main.py aggregation <table_name(check table_mapping.py for this)> <domain_name> <month>
```

## Spark Thrift Server(STS)
Its a spark application comes with the spark package. When you run it, it allows external applications to connect to it as a JDBC Client and query the data stored in Datalake using SQL queries same as quering any other SQL database like Postgres. You can read about it more [here](https://spark.apache.org/docs/latest/sql-distributed-sql-engine.html). We need this because we want to allow internal/external users to query the data using BI tool and BI tool needs to connect to SPARK cluster as a JDBC client. Run the following command to start the Spark Thrift Server for our setup:

```
$SPARK_HOME/sbin/start-thriftserver.sh --packages "io.delta:delta-core_2.12:0.8.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"

```

Now to test it as a client from cli. Spark Package comes with a CLI tool called `beeline`.  Use it to connect to STS:
```
    $SPARK_HOME/bin/spark-beeline
    beeline> !connect jdbc:hive2://localhost:10000
```

## BI tool
BI tools are great to see the quick visualization of the data and to query data fast through a web UI. For this System We are using Metabase as our BI tool. Setting up Metabase is pretty simple. We are using Jar method to run the Metabase([here](https://www.metabase.com/docs/latest/operations-guide/installing-metabase.html)). 
After Downloading Jar you just need to run:
```
java -jar metabase.jar
```
Above page linked shows the setup steps in detail.
After Metabase if setup, you can add a database and select the database type as Spark-SQL and put the credentials.

Note: Metabase's technique to find the columns is not as perfect(perhaps just for spark-sql). It may also see some extra things as columns when they are not because of which you can go to the admin and see the table and hide all the invalid columns. It should work fine after that.
## FAQ

1. Why are we processing records in Chunks and not as per records bases like a true stream?
    - Because When you are handling Data with Spark. You data is actually written to files in Disk/Object Storage. If you will process data per record bases, it will have to read and write more files which will increase the IOps. It will further slow down the system. In short, We can process larger data much faster than probably writing it.
    - You should also try to achieve the big chunk processing and lesser writing. If the type of forms increases, you can runn multiple instances of ingestion process and split the form types among them.

2. Why Do we store the aggergated Data back to Dashboard database? Or why can't dashboard directly pull data from Datalake storage if we are storing aggregated data into datalake storage as well.
    - Spark is good for large size data processing it is not very efficient for making index like queries. Eg: fetching a single records based on indexes say get me the launched flwcs in this state. In Such queries Postgres performs much better. Whereas, If you want to process/Aggregate huge data, spark is better.
    - Its a standard technique to separate out storages for user end(dashboard) and analytics(BI tool/aggregation) end of the product. So if someone is doing complex data pulls or if aggregation is running, it should not impact the performance of dashbaord.

3. How do I allow the BI tool to connect as a client using JDBC connection to query the data?
    - Start Spark thrift Server and connect your BI tool to it. Command to start STS(Spark Thrift Server) is mentioned above.
    - Detailed information about STS is [here](https://spark.apache.org/docs/latest/sql-distributed-sql-engine.html)

4. Is there a way to connect to STS from CLI for quick testing?
    - Yes, you can use the inbuilt CLI tool, beeline to connect to Spark Thrift Server. Command mentioned above in STS section.
    
5. What is the use of DeltaLake?
    - When we write any data using Spark, it is written in form of files. Imagine, if your data is to be written in 10 files but some error happened in middle and only 6 files could get written. In this case, you will endup in an inconsistent state.
    - Imagine you want to update a record out of all the stored records based on some id. You can't really directly do that in Spark because spark only allows you to create or append the data. It does not allows to update the existing files. Reading and writing the entire data collection would be very inefficient and we would need to deal with all the issues of inconsistency by ourself.
    - Delta lake helps to solve above issues. It mimics the ACID properties of a SQL database by using Delta Logs. It also provides clean way to perform DDL queries(UPDATE, DELETE, MERGE)
    - Read more about it [here](https://docs.delta.io/latest/index.html)
6. What happens when we perform update/delete/merge using Delta Lake?
    - Deltalake holds your data in form of versions. When you perform above queries:
        - It find the appropriate partition file needs to be updated.
        - Creates a new copy of that file with updated data.
        - Marks this new file to be the part of the latest Version.
        - Whenever a table is then queried, By default only the latest version data is pulled.
        - One can delete old versions using `vaccum` provided by Deltalake.
7. What are important things to know about DeltaLake?
    - Its Merge Query feature: https://docs.delta.io/latest/delta-update.html#upsert-into-a-table-using-merge
    - Its Concurrency Control during DDL: https://docs.delta.io/latest/concurrency-control.html
    - Schema Evolution: https://databricks.com/blog/2019/09/24/diving-into-delta-lake-schema-enforcement-evolution.html
8. Which Version of DeltaLake was used:
    - `0.8.0`
9. How to setup Multi node spark Cluster?
    - I used the following Blogs. These are pretty good:
        - https://medium.com/ymedialabs-innovation/apache-spark-on-a-multi-node-cluster-b75967c8cb2b
        - https://medium.com/@jootorres_11979/how-to-install-and-set-up-an-apache-spark-cluster-on-hadoop-18-04-b4d70650ed42
10. What are the resource/cluster managers spark can use?
    - Actually Three:
        - Yarn: It comes with hadoop package. We dont use it because we dont use hadoop. But if for any project we choose to use hadoop, this should be ours prefered choice.
        - Mesos: Its a generic cluster manager. Some people also call it as Operating System of a cluster. Because of its generic nature its relatively difficult to maintain and its a bit overkill for our need.
        - Kubernetes(Experimental): It does not really make sense to learn a total new thing. Kub8 is itself a huge concept to gain mastery in. So, that is why we did not choose Kub8.
        - Spark Standalone: Spark comes with a cluster manager with its package. This manager is customised for spark only and provide all the needed features like custom resource allocation, dynamic resource allocation, tracking UI etc. So, we use it for our purpose.
    - Here is the link you can read about these cluster managers for spark:
        - https://data-flair.training/blogs/apache-spark-cluster-managers-tutorial/
        - https://dzone.com/articles/deep-dive-into-spark-cluster-management
    
11. How to ensure spark for production?
    - For production spark can be run in high availability mode. Below if the link you can read about it.
        - https://spark.apache.org/docs/latest/spark-standalone.html#high-availability

12. What is the storage layer we are using?
    - Right now we have decided to move forward with AWS S3. But, the decision depends on the cloud we are using and the object storage it is providing. You can use following benchmarks to see if you want to use hadoop or CSP provided Storage:
        - Performance of data reading, writing, quering.
        - Data Consistency
        - Effort of using
        - Reliability

13. What if we have acquired a large group of customer who wants to analyse the data themselves using the BI tool(Metabase)? What are the things we need to take care accordingly?
    - Spark Cluster Size: Will the existing number of nodes in your spark cluster be able to handle that load? If not, you can add more worker nodes to your spark cluster and it would work like charm.
    -  Will your metabase worker be able to handle that many requests? If not, You can [horizontally scale](https://www.metabase.com/learn/data-diet/analytics/metabase-at-scale#horizontal-scaling) Metabase by putting a load balancer in front of it and spawning up multiple instances of it. 
        