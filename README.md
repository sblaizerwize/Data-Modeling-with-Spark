# **README**

## **Introduction**

Sparkify is a digital music application that enables users to listen to music online. Currently, the application aims to offer new content to users based on their song preferences. However, the information collected from users, including songs and user logs (metadata), is stored in JSON files in a S3 bucket, which avoids querying data and therefore running analytics on it. As follows, you can find excerpts of the JSON files. 

JSON file containing song's information. 
```
{
    "num_songs": 1,
    "artist_id": "ARD7TVE1187B99BFB1",
    "artist_latitude": null,
    "artist_longitude": null,
    "artist_location": "California - LA",
    "artist_name": "Casual",
    "song_id": "SOMZWCG12A8C13C480",
    "title": "I Didn't Mean To",
    "duration": 218.93179,
    "year": 0
}
```


JSON file containing user logs information. 
```
{
    "artist": null,
    "auth": "Logged In",
    "firstName": "Walter",
    "gender": "M",
    "itemInSession": 0,
    "lastName": "Frye",
    "length": null,
    "level": "free",
    "location": "San Francisco-Oakland-Hayward, CA",
    "method": "GET",
    "page": "Home",
    "registration": 1540919166796.0,
    "sessionId": 38,
    "song": null,
    "status": 200,
    "ts": 1541105830796,
    "userAgent": "\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"",
    "userId": "39"
}
```
---

## **Proposed solution**

The proposed solution in this repository consists of implementing an ETL migration process. The ETL is orchestrated by a Spark job that runs in an AWS EMR cluster. The aim of the job is to pull source data from a S3 bucket, transform it following a star schema design, and save it in a distinct S3 bucket in parquet format, see **Figure 1**. 

![sparkify architecture](/images/sparkify_emr.png)
**Figure 1** Architecture of the Data Lake Project.
<br />

The following steps describe the main stages of the spark job:

  - Read source data and loads it into a dataframe.
  - Drop duplicated records.
  - Create new dataframes complying with the schema design described in **Figure 2**. 
  - Save the dataframes in a S3 bucket using a parquet format. \
  **Note:** To optimize analytics, data can be partitioned by year, month, or artist, if required.  

![sparkify schema](/images/sparkify_emr_schema.png)
**Figure 2** Schema of the Data Lake.
<br />

---
## **Explanation of the files in this repository**

The following table describes the content of this repository. 

<table>
  <tr>
   <td><strong>File</strong>
   </td>
   <td><strong>Description</strong>
   </td>
  </tr>
  <tr>
   <td>data
   </td>
   <td>Folder that includes a subset of songs and logs records to run locally your ETL.  
   </td>
  </tr>
  <tr>
   <td>etl_local.py
   </td>
   <td>Python script that implements the ETL process in a local cluster. It pulls source data from a local folder, transforms it, and loads it into an S3 bucket in parquet format.
   </td>
  </tr>
  <tr>
   <td>etl_emr.py
   </td>
   <td>Python script that implements the ETL process in a AWS EMR cluster. It pulls source data from a S3 bucket, transforms it, and loads it into a different S3 bucket in parquet format.
   </td>
  </tr>
  <tr>
   <td>dl.cfg
   </td>
   <td>File that contains information about your AWS credentials. 
   </td>
  </tr>
  <tr>
   <td>README.md
   </td>
   <td>File that contains the main information and instructions of how to use this repository.
   </td>
  </tr>
</table>

---
## **Prerequisites**

Before using this repository, you must comply with the following:

To run your Spark job in the Udacity environment:
1. Include your AWS credentials in the `dl.cfg file` without single quotes.
2. Proceed to the section **How to run the Python scripts**


To run your Spark job in an AWS EMR cluster
1. Create an AWS account.
2. Install [AWS CLI 2](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html) in your computer.
3. Ensure to setup your [AWS credentials](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html#cli-configure-quickstart-config): key ID, access Key, and region.
4. Create a [PEM key](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html#prepare-key-pair) pair in the AWS console to connect to your EMR cluster securely via SSH.
    1. Download your PEM key in your computer.
    2. Move your PEM key to the `/.ssh/` folder: \
    `mv <yourkey.pem> ~/.ssh/`
    3. Grant read and execution permissions to the `/.ssh/` folder: \
    `sudo chmod 755 ~/.ssh`
    4. Grant read permission to the file: \
    `chmod 400 <yourkey.pem>`
5. Create an AWS EMR cluster. \
  You can use this steps as a reference:   

    ```
    aws emr create-default-roles
    aws emr create-cluster \
      --name "sparkifycluster"
      --instance-type m5.xlarge \
      --instance-count 3 \
      --release-label emr-5.32.0 \
      --ec2-attributes KeyName=<yourKey> \
      --use-default-roles \
      --applications Name=Hadoop Name=Ganglia Name=Spark
      --region us-west-2
    ```
6. Connect to the EMR master cluster from your terminal: \
`ssh hadoop@<masterpublicDNS> -i ~/.ssh/<yourkey.pem>`

---
## **How to run the Python scripts**
To run your ETL process in the Udacity environment:
1. Open a terminal console
2. Run the following command:
    ```
    /opt/conda/bin/spark-submit --packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.7 --master local[*] etl_local.py
    ```

To run your ETL process in an AWS EMR cluster:
1. Open a terminal console in your computer
2. Copy the `etl_emr.py` file to your AWS EMR cluster:
    ```
    scp -i ~/.ssh/<yourkey.pem> <path_to/etl_emr.py> hadoop@<masterpublicDNS>:~/
    ```
3. Connect to your EMR master cluster: \
`ssh hadoop@<masterpublicDNS> -i ~/.ssh/<yourkey.pem>`

4. In the cluster's terminal, submit your Spark job: \
`/usr/bin/spark-submit --master yarn ./etl_emr.py`



