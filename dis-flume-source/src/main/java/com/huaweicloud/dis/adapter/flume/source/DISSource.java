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

package com.huaweicloud.dis.adapter.flume.source;

import com.google.common.base.Optional;
import com.huaweicloud.dis.DISConfig;
import com.huaweicloud.dis.adapter.kafka.clients.consumer.*;
import com.huaweicloud.dis.adapter.kafka.common.TopicPartition;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.conf.LogPrivacyUtil;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.kafka.KafkaSourceCounter;
import org.apache.flume.source.AbstractPollableSource;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import static com.huaweicloud.dis.adapter.flume.source.DISSourceConstants.DEFAULT_GROUP_ID;

/**
 * A Source for DIS which reads messages from DIS streams.
 *
 * <tt>group.id: </tt> the group ID of consumer group. <b>Required</b>
 * <p>
 * <tt>streams: </tt> the stream list separated by commas to consume messages from. <b>Required</b>
 * <p>
 * <tt>maxBatchSize: </tt> Maximum number of messages written to Channel in one batch. Default: 1000
 * <p>
 * <tt>maxBatchDurationMillis: </tt> Maximum number of milliseconds before a batch (of any size) will be written to a
 * channel. Default: 1000
 * <p>
 */
public class DISSource extends AbstractPollableSource implements Configurable
{
    private static final Logger log = LoggerFactory.getLogger(DISSource.class);
    
    private Context context;
    
    private Properties disProps;
    
    private KafkaSourceCounter counter;
    
    private Consumer<String, byte[]> consumer;
    
    private Iterator<ConsumerRecord<String, byte[]>> it;
    
    private final List<Event> eventList = new ArrayList<Event>();
    
    private Map<TopicPartition, OffsetAndMetadata> tpAndOffsetMetadata;
    
    private AtomicBoolean rebalanceFlag;
    
    private Map<String, String> headers;
    
    private Optional<SpecificDatumReader<AvroFlumeEvent>> reader = Optional.absent();
    
    private BinaryDecoder decoder = null;
    
    private boolean useAvroEventFormat;
    
    private int batchUpperLimit;
    
    private int maxBatchDurationMillis;
    
    private Subscriber subscriber;
    
    private String groupId = DEFAULT_GROUP_ID;
    
    private String topicHeader = null;
    
    private boolean setTopicHeader;
    
    /**
     * This class is a helper to subscribe for topics by using different strategies
     */
    public abstract class Subscriber<T>
    {
        public abstract void subscribe(Consumer<?, ?> consumer, SourceRebalanceListener listener);
        
        public T get()
        {
            return null;
        }
    }
    
    private class TopicListSubscriber extends Subscriber<List<String>>
    {
        private List<String> topicList;
        
        public TopicListSubscriber(String commaSeparatedTopics)
        {
            this.topicList = Arrays.asList(commaSeparatedTopics.split("^\\s+|\\s*,\\s*|\\s+$"));
        }
        
        @Override
        public void subscribe(Consumer<?, ?> consumer, SourceRebalanceListener listener)
        {
            consumer.subscribe(topicList, listener);
        }
        
        @Override
        public List<String> get()
        {
            return topicList;
        }
    }
    
    private class PatternSubscriber extends Subscriber<Pattern>
    {
        private Pattern pattern;
        
        public PatternSubscriber(String regex)
        {
            this.pattern = Pattern.compile(regex);
        }
        
        @Override
        public void subscribe(Consumer<?, ?> consumer, SourceRebalanceListener listener)
        {
            consumer.subscribe(pattern, listener);
        }
        
        @Override
        public Pattern get()
        {
            return pattern;
        }
    }
    
    @Override
    protected Status doProcess()
        throws EventDeliveryException
    {
        final String batchUUID = UUID.randomUUID().toString();
        byte[] kafkaMessage;
        String kafkaKey;
        Event event;
        byte[] eventBody;
        
        try
        {
            // prepare time variables for new batch
            final long nanoBatchStartTime = System.nanoTime();
            final long batchStartTime = System.currentTimeMillis();
            final long maxBatchEndTime = System.currentTimeMillis() + maxBatchDurationMillis;
            
            while (eventList.size() < batchUpperLimit && System.currentTimeMillis() < maxBatchEndTime)
            {
                
                if (it == null || !it.hasNext())
                {
                    // Obtaining new records
                    // Poll time is remainder time for current batch.
                    ConsumerRecords<String, byte[]> records =
                        consumer.poll(Math.max(0, maxBatchEndTime - System.currentTimeMillis()));
                    it = records.iterator();
                    
                    // this flag is set to true in a callback when some partitions are revoked.
                    // If there are any records we commit them.
                    if (rebalanceFlag.get())
                    {
                        rebalanceFlag.set(false);
                        break;
                    }
                    // check records after poll
                    if (!it.hasNext())
                    {
                        if (log.isDebugEnabled())
                        {
                            counter.incrementKafkaEmptyCount();
                            log.debug("Returning with backoff. No more data to read");
                        }
                        // batch time exceeded
                        break;
                    }
                }
                
                // get next message
                ConsumerRecord<String, byte[]> message = it.next();
                kafkaKey = message.key();
                kafkaMessage = message.value();
                
                if (useAvroEventFormat)
                {
                    // Assume the event is in Avro format using the AvroFlumeEvent schema
                    // Will need to catch the exception if it is not
                    ByteArrayInputStream in = new ByteArrayInputStream(message.value());
                    decoder = DecoderFactory.get().directBinaryDecoder(in, decoder);
                    if (!reader.isPresent())
                    {
                        reader = Optional.of(new SpecificDatumReader<AvroFlumeEvent>(AvroFlumeEvent.class));
                    }
                    // This may throw an exception but it will be caught by the
                    // exception handler below and logged at error
                    AvroFlumeEvent avroevent = reader.get().read(null, decoder);
                    
                    eventBody = avroevent.getBody().array();
                    headers = toStringMap(avroevent.getHeaders());
                }
                else
                {
                    eventBody = message.value();
                    headers.clear();
                    headers = new HashMap<String, String>(4);
                }
                
                // Add headers to event (timestamp, topic, partition, key) only if they don't exist
                if (!headers.containsKey(DISSourceConstants.TIMESTAMP_HEADER))
                {
                    headers.put(DISSourceConstants.TIMESTAMP_HEADER, String.valueOf(System.currentTimeMillis()));
                }
                // Only set the topic header if setTopicHeader and it isn't already populated
                if (setTopicHeader && !headers.containsKey(topicHeader))
                {
                    headers.put(topicHeader, message.topic());
                }
                if (!headers.containsKey(DISSourceConstants.PARTITION_HEADER))
                {
                    headers.put(DISSourceConstants.PARTITION_HEADER, String.valueOf(message.partition()));
                }
                
                if (kafkaKey != null)
                {
                    headers.put(DISSourceConstants.KEY_HEADER, kafkaKey);
                }
                
                if (log.isTraceEnabled())
                {
                    if (LogPrivacyUtil.allowLogRawData())
                    {
                        log.trace("Topic: {} Partition: {} Message: {}",
                            new String[] {message.topic(), String.valueOf(message.partition()), new String(eventBody)});
                    }
                    else
                    {
                        log.trace("Topic: {} Partition: {} Message arrived.",
                            message.topic(),
                            String.valueOf(message.partition()));
                    }
                }
                
                event = EventBuilder.withBody(eventBody, headers);
                eventList.add(event);
                
                if (log.isDebugEnabled())
                {
                    log.debug("Waited: {} ", System.currentTimeMillis() - batchStartTime);
                    log.debug("Event #: {}", eventList.size());
                }
                
                // For each partition store next offset that is going to be read.
                tpAndOffsetMetadata.put(new TopicPartition(message.topic(), message.partition()),
                    new OffsetAndMetadata(message.offset() + 1, batchUUID));
            }
            
            if (eventList.size() > 0)
            {
                counter.addToKafkaEventGetTimer((System.nanoTime() - nanoBatchStartTime) / (1000 * 1000));
                counter.addToEventReceivedCount((long)eventList.size());
                getChannelProcessor().processEventBatch(eventList);
                counter.addToEventAcceptedCount(eventList.size());
                if (log.isDebugEnabled())
                {
                    log.debug("Wrote {} events to channel", eventList.size());
                }
                eventList.clear();
                
                if (!tpAndOffsetMetadata.isEmpty())
                {
                    long commitStartTime = System.nanoTime();
                    consumer.commitSync(tpAndOffsetMetadata);
                    long commitEndTime = System.nanoTime();
                    counter.addToKafkaCommitTimer((commitEndTime - commitStartTime) / (1000 * 1000));
                    tpAndOffsetMetadata.clear();
                }
                return Status.READY;
            }
            
            return Status.BACKOFF;
        }
        catch (Exception e)
        {
            log.error("DISSource EXCEPTION, {}", e);
            return Status.BACKOFF;
        }
    }
    
    /**
     * We configure the source and generate properties for the DIS Consumer
     *
     * @param context
     */
    @Override
    protected void doConfigure(Context context)
        throws FlumeException
    {
        this.context = context;
        headers = new HashMap<String, String>(4);
        tpAndOffsetMetadata = new HashMap<TopicPartition, OffsetAndMetadata>();
        rebalanceFlag = new AtomicBoolean(false);
        disProps = new Properties();
        
        String topicProperty = context.getString(DISSourceConstants.STREAMS_REGEX);
        if (topicProperty != null && !topicProperty.isEmpty())
        {
            // create subscriber that uses pattern-based subscription
            subscriber = new PatternSubscriber(topicProperty);
        }
        else if ((topicProperty = context.getString(DISSourceConstants.STREAMS)) != null && !topicProperty.isEmpty())
        {
            // create subscriber that uses topic list subscription
            log.info("Topic Property: ", topicProperty);
            subscriber = new TopicListSubscriber(topicProperty);
        }
        else if (subscriber == null)
        {
            throw new ConfigurationException("At least one DIS stream must be specified.");
        }
        
        batchUpperLimit = context.getInteger(DISSourceConstants.BATCH_SIZE, DISSourceConstants.DEFAULT_BATCH_SIZE);
        maxBatchDurationMillis =
            context.getInteger(DISSourceConstants.BATCH_DURATION_MS, DISSourceConstants.DEFAULT_BATCH_DURATION);
        
        useAvroEventFormat =
            context.getBoolean(DISSourceConstants.AVRO_EVENT, DISSourceConstants.DEFAULT_AVRO_EVENT);
        
        if (log.isDebugEnabled())
        {
            log.debug(DISSourceConstants.AVRO_EVENT + " set to: {}", useAvroEventFormat);
        }
        
        String groupIdProperty = context.getString(ConsumerConfig.GROUP_ID_CONFIG);
        if (groupIdProperty != null && !groupIdProperty.isEmpty())
        {
            groupId = groupIdProperty; // Use the new group id property
        }
        
        if (groupId == null || groupId.isEmpty())
        {
            groupId = DEFAULT_GROUP_ID;
            log.info("Group ID was not specified. Using {} as the group id.", groupId);
        }
        
        setTopicHeader =
            context.getBoolean(DISSourceConstants.SET_TOPIC_HEADER, DISSourceConstants.DEFAULT_SET_TOPIC_HEADER);
        
        topicHeader = context.getString(DISSourceConstants.TOPIC_HEADER, DISSourceConstants.DEFAULT_TOPIC_HEADER);
        
        setConsumerProps(context);
        
        if (log.isDebugEnabled() && LogPrivacyUtil.allowLogPrintConfig())
        {
            log.debug("DIS consumer properties: {}", disProps);
        }
        
        if (counter == null)
        {
            counter = new KafkaSourceCounter(getName());
        }
    }
    
    private void setConsumerProps(Context ctx)
    {
        disProps.clear();
        disProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, DISSourceConstants.DEFAULT_KEY_DESERIALIZER);
        disProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DISSourceConstants.DEFAULT_VALUE_DESERIALIZER);
        // Defaults overridden based on config
        disProps.putAll(ctx.getParameters());
        
        if (groupId != null)
        {
            disProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        }
        disProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, DISSourceConstants.DEFAULT_AUTO_COMMIT);
        // try to decrypt ak/sk
        tryDecryptDISCredentials();
    }
    
    Properties getConsumerProps()
    {
        return disProps;
    }
    
    /**
     * Helper function to convert a map of CharSequence to a map of String.
     */
    private static Map<String, String> toStringMap(Map<CharSequence, CharSequence> charSeqMap)
    {
        Map<String, String> stringMap = new HashMap<String, String>();
        for (Map.Entry<CharSequence, CharSequence> entry : charSeqMap.entrySet())
        {
            stringMap.put(entry.getKey().toString(), entry.getValue().toString());
        }
        return stringMap;
    }
    
    <T> Subscriber<T> getSubscriber()
    {
        return subscriber;
    }
    
    @Override
    protected void doStart()
        throws FlumeException
    {
        log.info("Starting {}...", this);
        
        // initialize a consumer.
        consumer = new DISKafkaConsumer<String, byte[]>(disProps);
        
        // Subscribe for topics by already specified strategy
        subscriber.subscribe(consumer, new SourceRebalanceListener(rebalanceFlag));
        
        // Connect to DIS. 1 second is optimal time.
        it = consumer.poll(1000).iterator();
        log.info("DIS source {} started.", getName());
        counter.start();
    }
    
    @Override
    protected void doStop()
        throws FlumeException
    {
        if (consumer != null)
        {
            consumer.wakeup();
            consumer.close();
        }
        counter.stop();
        log.info("DIS Source {} stopped. Metrics: {}", getName(), counter);
    }

    protected void tryDecryptDISCredentials()
    {
        if (disProps == null)
        {
            return;
        }
        disProps.put(DISConfig.PROPERTY_AK, tryGetDecryptValue(DISConfig.PROPERTY_AK));
        disProps.put(DISConfig.PROPERTY_SK, tryGetDecryptValue(DISConfig.PROPERTY_SK));
    }

    protected String tryGetDecryptValue(String key)
    {
        Object v = disProps.get(key);
        if (v == null)
        {
            return null;
        }
        String value = String.valueOf(v);
        String dataPassword = null;
        if (disProps.get(DISConfig.PROPERTY_DATA_PASSWORD) != null)
        {
            dataPassword = String.valueOf(disProps.get(DISConfig.PROPERTY_DATA_PASSWORD));
        }
        // 168 is the Minimum length of encrypt value.
        if (value.length() >= 168)
        {
            // not need to use configProviderClass
            disProps.remove(DISConfig.PROPERTY_CONFIG_PROVIDER_CLASS);
            try
            {
                log.info("Try to decrypt [{}].", key);
                return EncryptTool.decrypt(value, dataPassword);
            }
            catch (Exception e)
            {
                log.error("Failed to decrypt [{}].", key);
                throw e;
            }
        }
        return value;
    }
}

class SourceRebalanceListener implements ConsumerRebalanceListener
{
    private static final Logger log = LoggerFactory.getLogger(SourceRebalanceListener.class);
    
    private AtomicBoolean rebalanceFlag;
    
    public SourceRebalanceListener(AtomicBoolean rebalanceFlag)
    {
        this.rebalanceFlag = rebalanceFlag;
    }
    
    // Set a flag that a rebalance has occurred. Then commit already read events to DIS.
    public void onPartitionsRevoked(Collection<TopicPartition> partitions)
    {
        log.info("revoked partitions {}", partitions);
        rebalanceFlag.set(true);
    }
    
    public void onPartitionsAssigned(Collection<TopicPartition> partitions)
    {
        log.info("assigned partitions {}", partitions);
    }
}