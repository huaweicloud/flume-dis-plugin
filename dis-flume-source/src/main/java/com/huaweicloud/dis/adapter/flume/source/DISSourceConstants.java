package com.huaweicloud.dis.adapter.flume.source;

public class DISSourceConstants
{
    
    public static final String STREAMS = "streams";
    
    public static final String STREAMS_REGEX = STREAMS + "." + "regex";
    
    public static final String DEFAULT_KEY_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
    
    public static final String DEFAULT_VALUE_DESERIALIZER =
        "org.apache.kafka.common.serialization.ByteArrayDeserializer";
    
    public static final String DEFAULT_AUTO_COMMIT = "false";
    
    public static final String BATCH_SIZE = "batchSize";
    
    public static final String BATCH_DURATION_MS = "batchDurationMillis";
    
    public static final int DEFAULT_BATCH_SIZE = 1000;
    
    public static final int DEFAULT_BATCH_DURATION = 1000;
    
    public static final String DEFAULT_GROUP_ID = "flume";
    
    public static final String AVRO_EVENT = "useFlumeEventFormat";
    
    public static final boolean DEFAULT_AVRO_EVENT = false;
    
    /* Old Properties */
    public static final String ZOOKEEPER_CONNECT_FLUME_KEY = "zookeeperConnect";
    
    public static final String TOPIC = "topic";
    
    public static final String OLD_GROUP_ID = "groupId";
    
    // flume event headers
    public static final String DEFAULT_TOPIC_HEADER = "topic";
    
    public static final String KEY_HEADER = "key";
    
    public static final String TIMESTAMP_HEADER = "timestamp";
    
    public static final String PARTITION_HEADER = "partition";
    
    public static final String SET_TOPIC_HEADER = "setTopicHeader";
    
    public static final boolean DEFAULT_SET_TOPIC_HEADER = true;
    
    public static final String TOPIC_HEADER = "topicHeader";
    
}
