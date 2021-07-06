# Dimagi Lake
Repository contains the code which is used for Form and cases ingestion into the Datalake.


## Menu
1. How It Works
2. Dependencies
3. Scope
4. How to setup



## How It Works
This is an Spark Application Which is used to ingest Form and Case Data into the Datalake. It runs as a services and starts a Kafka Stream to periodically Pull ChangeMeta from kafka. Following Points explains how it works:
1. It starts a Kafka stream using Spark Structure Steaming
2. Pulls `<=MAX_RECORDS_TO_PROCESS` number of messages from Kafka in a single Batch. Constant `MAX_RECORDS_TO_PROCESS` in settings.py file can be set to virtually any value depending on the resources available.
3. After Pulling ChangeMeta from Kafka. Split the Changes into different `domain` and `subtype`.
4. Pull The real records from HQ Postgres using `CaseAccessors` and `FormAccessors` of HQ Code.
5. Location Information is added to the records for better downstream analysis.
6. Records are flattened. Flattening is required becaused of a potential [bug](https://github.com/delta-io/delta/issues/170#issuecomment-784156234) present in a library we use.
7. If its a new type of form/case, then a new corresponding table is created in the metastore and it's data is directly written to the storage on a new location `\commcare\<domain>\<kafka_topic>\<case_type\form_xmlns>`
8. For records whose corresponding table is already present in the metastore, the records are then Merged with the existing Data in the Datalake. Merging happens using Upsert Operation Using a spark library called [Delta Lake](https://docs.delta.io/latest/delta-intro.html). This makes the Data merging Possible. 
9. We are only keeping the latest copy of records in the Datalake. When a case is udpated for example, Only the latest copy of case is maintained in the Datalake. Older versions can also be accessed if we want. But, since the use case is only to have the latest copy, we can remove the Older version periodically to free up space.
10. To resolve Small Files Problem in Datalake, Data would be periodically be re-partitioned to reduce the number of files. It would make the data query faster.


## Dependencies
1. **Python3**: Its written in Python3
2. **Java8**: Used by Spark.
3. **Scala 1.12**: Used by Spark.
2. **Spark 3.0.2**: Spark is used as a processing engine to process the incoming data.
3. **Object Storage/HDFS(3.2)**: Used to store the Data. Right now using S3. The repository can also be used with Local Filesystem
4. **Commcare Hq**: It uses Commcare Hq to pull Forms/cases records, Locations, user info from HQ Storage
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
- [ ] Allows Field datatype change in cases/forms

## How to Setup

### Install the prerequisite
1. Python3
2. Java8
9. Scala 1.12

### Download Spark
Install `Spark 3.0.2, pre-built for apache hadoop 3.2 and later` version from the following link

https://spark.apache.org/downloads.html

## Setup Spark

### On Mac using Brew (recommended if you have a Mac)
1. Run:
    ```
    brew cask install java;
    brew install scala;
    brew install apache-spark
    ```

### On others using Binary package
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
    export SPARK_HOME = /usr/loca/spark/
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

## Setup Hive Metastore
Metastore is used to store the metatables for the data you are writing to the datalake using Spark. By default it stores the meta info into the local dist and in the directory from where you are running the spark application. But it has two problems:
1. It would be difficult to share the metainformation between spark applications. Eg: If Aggregation wants to access the tables created by Datalake ingestion.
2. Storing the data directory into local disk might not be safe and we would have to separately manage its appropriate availability and security.

So, we will setup a central hive metastore using postgres and tell Spark to use that as a metastore.

1. Install Postgres. You can also use HQ postgres if you have that setup.
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
5. Your Hive Metastore is now ready.

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
5. 



### Running it
```
spark-submit --packages "io.delta:delta-core_2.12:0.8.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" <path/to/dimagi_lake>/main.py <kafka_topic_name>
```

