/*
 * Copyright 2002-2010 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huaweicloud.dis.adapter.flume.sink;

public class DISSinkConstants {

    public static final String STREAMS = "streams";

    public static final String DEFAULT_KEY_DESERIALIZER
        = "com.huaweicloud.dis.adapter.kafka.common.serialization.StringDeserializer";

    public static final String DEFAULT_VALUE_DESERIALIZER =
        "com.huaweicloud.dis.adapter.kafka.common.serialization.ByteArrayDeserializer";

    public static final String BATCH_SIZE = "batchSize";

    public static final int DEFAULT_BATCH_SIZE = 1000;

    public static final String AVRO_EVENT = "useFlumeEventFormat";

    public static final boolean DEFAULT_AVRO_EVENT = false;

    public static final String KEY_HEADER = "key";
    public static final String DEFAULT_TOPIC_OVERRIDE_HEADER = "stream";
    public static final String TOPIC_OVERRIDE_HEADER = "topicHeader";
    public static final String ALLOW_TOPIC_OVERRIDE_HEADER = "allowTopicOverride";
    public static final boolean DEFAULT_ALLOW_TOPIC_OVERRIDE_HEADER = true;

    public static final String DEFAULT_TOPIC = "default-flume-stream";

}
