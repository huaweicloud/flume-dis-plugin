# Welcome to DIS Source Plugin for Apache Flume
    DIS Flume Source is a Source plug-in provided by the Data Ingestion Service (DIS) to pull data in Flume channels from the DIS.

## Getting Started
---

### Requirements

To get started using this plugin, you will need three things:

1. JDK 1.8 +
2. Flume-NG 1.4.0 +

### Installing DIS Flume Source
1. Upload `dis-flume-source-x.x.x.zip` to **`$(FLUME_HOME}/plugin.d`** directory. Such as `/opt/apache-flume-1.7.0/dis-flume-source-x.x.x.zip`
2. Unzip package, and you will get new directory named `dis-flume-source`

```
$ unzip dis-flume-source-x.x.x.zip
```

3. Package can be delete now.

```
$ cd ${FLUME_HOME}/plugin.d
$ rm -rf dis-source-sink-x.x.x.zip
```

The Dis Sink Plugin should be available for Flume now. The plugin file will be under the directory `${FLUME_HOME}/plugin.d/dis-flume-source`.


DIS Source Parameters.
---



| Name                     | Description                              | Default                                  |
| :----------------------- | :--------------------------------------- | :--------------------------------------- |
| channel                  | Flume channel name.                      | -                                        |
| type                     | Use DIS Sink, type name must be `com.huaweicloud.dis.adapter.flume.sink.DISSink` | com.huaweicloud.dis.adapter.flume.sink.DISSink |
| streams                  | The DIS stream list separated by commas to consume messages from.     | -                                        |
| ak                       | The Access Key ID for hwclouds, it can be obtained from **My Credential** Page | -                                        |
| sk                       | The Secret Key ID for hwclouds, it can be obtained from **My Credential** Page | -                                        |
| region                   | Specifies use which region of DIS, now DIS only support `cn-north-1` | cn-north-1                               |
| projectId                | The ProjectId of the specified region, it can be obtained from **My Credential** Page | -                                        |
| endpoint                 | DIS Gateway endpoint                     | https://dis.cn-north-1.myhuaweicloud.com |
| group.id                 | The group ID of consumer group.          | 1                                        |
| enable.auto.commit       | Setting enable.auto.commit means that offsets are committed automatically with a frequency controlled by the config auto.commit.interval.ms.   | true                                     |
| auto.commit.interval.ms  | Frequency when offsets are committed automatically.             | 5000                    |


DIS Source Configuration Example
---
```properties
agent.sources.DISSource.channel=channel1
agent.sources.DISSource.type=com.huaweicloud.dis.adapter.flume.source.DISSource
agent.sources.DISSource.streams = YOUR_STREAM_NAME
agent.sources.DISSource.ak = YOUR_ACCESS_KEY_ID
agent.sources.DISSource.sk = YOUR_SECRET_KEY_ID
agent.sources.DISSource.region = cn-north-1
agent.sources.DISSource.projectId = YOUR_PROJECT_ID
agent.sources.DISSource.group.id = YOUR_APP_ID
agent.sources.DISSource.enable.auto.commit = true
agent.sources.DISSource.auto.commit.interval.ms = 50000
```
