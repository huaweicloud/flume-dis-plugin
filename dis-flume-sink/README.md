# Welcome to DIS Sink Plugin for Apache Flume
    DIS Flume Sink is a Sink plug-in provided by the Data Ingestion Service (DIS) to upload data in Flume channels to the DIS.

## Getting Started
---

### Requirements

To get started using this plugin, you will need three things:

1. JDK 1.8 +
2. Flume-NG 1.4.0 +

### Installing DIS Flume Sink
1. Upload `dis-flume-sink-x.x.x.zip` to **`$(FLUME_HOME}`** directory. Such as `/opt/apache-flume-1.7.0/dis-flume-sink-x.x.x.zip`
2. Unzip package, and you will get new directory named `dis-flume-sink`

```
$ unzip dis-flume-sink-x.x.x.zip
```

3. Run the following commands to execute the Flume installation script **install.sh**:

```
$ cd dis-flume-sink
$ dos2unix install.sh
$ bash install.sh
```

4. If the following information appears, the DIS Flume Sink has been successfully installed in
   the ${FLUME_HOME}/plugin.d/dis-flume-sink directory:

```
Install dis-flume-sink successfully.
```

5. Package and directory can be delete now.

```
$ cd ${FLUME_HOME}
$ rm -rf dis-flume-sink-x.x.x.zip dis-flume-sink
```

The Dis Sink Plugin should be available for Flume now. The plugin file will be under the directory `${FLUME_HOME}/plugin.d/dis-flume-sink`.


DIS Sink Parameters.
---



| Name                     | Description                              | Default                                  |
| :----------------------- | :--------------------------------------- | :--------------------------------------- |
| channel                  | Flume channel name.                      | -                                        |
| type                     | Use DIS Sink, type name must be `com.huaweicloud.dis.adapter.flume.sink.DISSink` | com.huaweicloud.dis.adapter.flume.sink.DISSink |
| streamName               | Specifies the name of the Stream that is Created on DIS service | -                                        |
| ak                       | The Access Key ID for hwclouds, it can be obtained from **My Credential** Page | -                                        |
| sk                       | The Secret Key ID for hwclouds, it can be obtained from **My Credential** Page | -                                        |
| region                   | Specifies use which region of DIS, now DIS only support `cn-north-1` | cn-north-1                               |
| projectId                | The ProjectId of the specified region, it can be obtained from **My Credential** Page | -                                        |
| endpoint                 | DIS Gateway endpoint                     | https://dis.cn-north-1.myhuaweicloud.com |
| partitionNumber          | Number of stream partitions, note that this configuration only used to calculate batchSize. The transmitted data will be evenly distributed on each partition | 1                                        |
| batchSize                | The size of a dataset for batch processing within a Flume transaction | `partitionNumber`*250                    |
| sendingThreadSize        | The number of sending threads, the default value is one. Increase this value can improve sending performance, but need to pay attention that the use of multi-threading will lead to out of sequence and some data retransmission when flume resumes from abnormal stop (for example, get `batchSize` data from flume, but only part of data sent successfully, and flume stop abnormal, all data will be re-upload when flume restart because flume transaction did not submit completed) | 1                                        |
| sendingRecordSize        | The batch put size for a single call to DIS interface. `batchSize`represents a batch value (such as 1000) of a transaction, and `sendingRecordSize` represents a bulk value of a rest request (for example, 250 indicates that there will four times call). Flume transaction will be completed when all data sending successfully, otherwise, those data will be retransmission when Flume restart. This value is forced to be the same value as `batchSize` when `sendingThreadSize` is 1 (to reduce data retransmissions). | 250                                      |
| retrySize                | Specifies how many times should be retried when an exception occurs. The default value `2147483647 (Integer.MAX_VALUE)` is recommended, it's means infinite retry, but `exponential backoff algorithm` will be used to wait for a while | 2147483647                               |
| resultLogLevel           | Specifies the log level for the partition and latest sequenceNumber after DIS response, Valid values is `OFF`,`DEBUG`,` INFO`,`WARN`,`ERROR`. Note that it will not take effect if flume log4j's level is higher | OFF                                      |
| maxBufferAgeMillis       | This maximum waiting time when the size of buffered events does not reach `batchSize`. Buffered events will be sent immediately after this time. The unit is millisecond | 5000                                     |
| connectionTimeOutSeconds | The time of the connection timeout, the unit is second | 30                                       |
| socketTimeOutSeconds     | The time of the read timeout, the unit is second | 60                                       |
| dataEncryptEnabled       | Whether the data is be encrypted         | false                                    |
| dataPassword             | Data encryption password, it's useless when `dataEncryptEnabled` is `false` | -                                        |


DIS Sink Configuration Example
---
```properties
agent.sinks.dissink.channel=channel1
agent.sinks.dissink.type=com.huaweicloud.dis.adapter.flume.sink.DISSink
agent.sinks.dissink.streamName=YOU_DIS_STREAM_NAME
agent.sinks.dissink.ak=YOU_ACCESS_KEY_ID
agent.sinks.dissink.sk=YOU_SECRET_KEY_ID
agent.sinks.dissink.region=cn-north-1
agent.sinks.dissink.projectId=YOU_PROJECT_ID
agent.sinks.dissink.endpoint=https://dis.cn-north-1.myhuaweicloud.com
agent.sinks.dissink.batchSize=250
agent.sinks.dissink.resultLogLevel=INFO
```