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

import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.http.HttpStatus;
import org.apache.http.conn.ConnectTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.huaweicloud.dis.*;
import com.huaweicloud.dis.adapter.flume.sink.backoff.BackOffExecution;
import com.huaweicloud.dis.adapter.flume.sink.backoff.ExponentialBackOff;
import com.huaweicloud.dis.adapter.flume.sink.utils.EncryptTool;
import com.huaweicloud.dis.core.handler.AsyncHandler;
import com.huaweicloud.dis.core.util.StringUtils;
import com.huaweicloud.dis.exception.DISClientException;
import com.huaweicloud.dis.http.exception.HttpClientErrorException;
import com.huaweicloud.dis.http.exception.ResourceAccessException;
import com.huaweicloud.dis.http.exception.RestClientResponseException;
import com.huaweicloud.dis.http.exception.UnknownHttpStatusCodeException;
import com.huaweicloud.dis.iface.data.request.GetPartitionCursorRequest;
import com.huaweicloud.dis.iface.data.request.PutRecordsRequest;
import com.huaweicloud.dis.iface.data.request.PutRecordsRequestEntry;
import com.huaweicloud.dis.iface.data.response.PutRecordsResult;
import com.huaweicloud.dis.iface.data.response.PutRecordsResultEntry;
import com.huaweicloud.dis.util.PartitionCursorTypeEnum;

public class DISSink extends AbstractSink implements Configurable
{
    
    private static final Logger LOGGER = LoggerFactory.getLogger(DISSink.class);
    
    public static final String CONFIG_STREAM_NAME_KEY = "streamName";
    
    public static final String CONFIG_ENDPOINT_KEY = "endpoint";
    
    public static final String CONFIG_PROJECT_ID_KEY = "projectId";
    
    public static final String CONFIG_REGION_KEY = "region";
    
    public static final String CONFIG_ACCESS_KEY = "ak";
    
    public static final String CONFIG_SECRET_KEY = "sk";
    
    public static final String CONFIG_DATA_ENCRYPT_ENABLED_KEY = "dataEncryptEnabled";
    
    public static final String CONFIG_DATA_PASSWORD_KEY = "dataPassword";
    
    public static final String CONFIG_DEFAULT_MAX_PER_ROUTE_KEY = "httpClientDefaultMaxPerRoute";
    
    public static final String CONFIG_DEFAULT_MAX_TOTAL_KEY = "httpClientDefaultMaxTotal";
    
    public static final String CONFIG_BATCH_SIZE_KEY = "batchSize";
    
    public static final String CONFIG_RETRY_SIZE_KEY = "retrySize";
    
    public static final String CONFIG_PARTITION_COUNT_KEY = "partitionNumber";
    
    public static final String CONFIG_CONNECTION_TIMEOUT_KEY = "connectionTimeOutSeconds";
    
    public static final String CONFIG_SOCKET_TIMEOUT_KEY = "socketTimeOutSeconds";
    
    public static final String CONFIG_IS_DEFAULT_TRUSTED_JKS_ENABLED_KEY = "isDefaultTrustedJksEnabled";
    
    public static final String CONFIG_BODY_SERIALIZE_TYPE_KEY = "bodySerializeType";
    
    public static final String CONFIG_PROVIDER_CLASS_KEY = "configProviderClass";
    
    public static final String CONFIG_MAX_BUFFER_AGE_MILLIS_KEY = "maxBufferAgeMillis";
    
    public static final String CONFIG_RESULT_LOG_LEVEL_KEY = "resultLogLevel";
    
    public static final String CONFIG_SENDING_THREAD_SIZE_KEY = "sendingThreadSize";
    
    public static final String CONFIG_DEFAULT_REGION = "cn-north-1";
    
    public static final String CONFIG_SENDING_RECORD_SIZE_KEY = "sendingRecordSize";
    
    public static final String CONFIG_PARTITION_KEY_OPTION = "partitionKeyOption";
    
    public static final String CONFIG_PARTITION_KEY_DELIMITER = "partitionKeyDelimiter";

    public static final String CONFIG_REQUEST_BYTES_LIMIT = "requestBytesLimit";

    public static final String CONFIG_ENCRYPT_KEY = "encryptKey";
    
    public static final String DEFAULT_PARTITION_KEY_SPLIT = ",";


    /**
     * AK/SK认证失败返回码
     */
    public static final int AUTHENTICATION_ERROR_HTTP_CODE = 441;
    
    /**
     * 按一个分区，每秒请求4次计算批量的值 1000/4=250
     */
    public static final int BATCH_CONTROL_FACTOR = 250;
    
    protected DISConfig disConfig;
    
    protected DISAsync disAsyncClient;
    
    protected DIS disClient;
    
    /**
     * 通道名称
     */
    public String streamName;
    
    /**
     * 分区数量
     */
    private int partitionCount = 1;
    
    /**
     * 批量提交上限
     */
    public int batchSize = BATCH_CONTROL_FACTOR * partitionCount;
    
    /**
     * 重试次数
     */
    private int retrySize = Integer.MAX_VALUE;
    
    /**
     * 如果数据没有达到批量的值，则等待此时间之后立刻发送已缓存数据(ms)
     */
    private int maxBufferAgeMillis = 5000;
    
    /**
     * 在日志输出DIS响应体结果开关
     */
    private RESULT_LOG_LEVEL resultLogLevel = RESULT_LOG_LEVEL.OFF;
    
    private final AtomicLong totalPutCount = new AtomicLong(0);
    
    private Properties properties;
    
    private SinkCounter sinkCounter;
    
    /**
     * 退避算法初始间隔
     */
    private long initialInterval = 100;
    
    /**
     * 退避算法最大间隔
     */
    private long maxInterval = 10 * 1000L;
    
    /**
     * 等待的总最大时间
     */
    private long maxElapsedTime = Long.MAX_VALUE;
    
    /**
     * 递增倍数
     */
    private double multiplier = 1.5;
    
    private ExponentialBackOff backOff;
    
    private boolean isRunning;
    
    private int sendingRecordSize;
    
    private LinkedBlockingQueue<PutRecordsRequest> receivedQueue = new LinkedBlockingQueue<>();
    
    private BackOffExecution execution = null;
    
    private volatile long nextBackOff = 0;
    
    final private Object backOffLock = new Object();
    
    private List<String> partitionKeyOptionList;
    
    private String partitionKeyDelimiter;

    private long requestBytesLimit = 4 * 1024 * 1024;
    
    @Override
    public void configure(Context context)
    {
        ImmutableMap<String, String> props = context.getParameters();
        properties = new Properties();
        for (String key : props.keySet())
        {
            this.properties.put(key, props.get(key).trim());
        }
        
        this.streamName = get(CONFIG_STREAM_NAME_KEY, null);
        this.maxBufferAgeMillis = getInt(CONFIG_MAX_BUFFER_AGE_MILLIS_KEY, maxBufferAgeMillis);
        this.partitionCount = getInt(CONFIG_PARTITION_COUNT_KEY, partitionCount);
        this.batchSize = getInt(CONFIG_BATCH_SIZE_KEY, BATCH_CONTROL_FACTOR * partitionCount);
        this.retrySize = getInt(CONFIG_RETRY_SIZE_KEY, retrySize);
        
        // 获取结果日志开关
        try
        {
            this.resultLogLevel = RESULT_LOG_LEVEL
                .valueOf(get(CONFIG_RESULT_LOG_LEVEL_KEY, RESULT_LOG_LEVEL.INFO.toString()).toUpperCase());
        }
        catch (IllegalArgumentException e)
        {
            this.resultLogLevel = RESULT_LOG_LEVEL.INFO;
        }
        
        // 参数校验
        Preconditions.checkArgument(streamName != null, "DIS configuration error, streamName can not be null");
        Preconditions.checkArgument(properties.get(CONFIG_ACCESS_KEY) != null,
            "DIS configuration error, access Key can not be null");
        Preconditions.checkArgument(properties.get(CONFIG_SECRET_KEY) != null,
            "DIS configuration error, secret Key can not be null");
        Preconditions.checkArgument(properties.get(CONFIG_PROJECT_ID_KEY) != null,
            "DIS configuration error, project ID can not be null");
        Preconditions.checkArgument(properties.get(CONFIG_REGION_KEY) != null,
            "DIS configuration error, region can not be null");
        
        // 接口调用失败时退避算法
        backOff = new ExponentialBackOff(initialInterval, multiplier);
        backOff.setMaxInterval(maxInterval);
        backOff.setMaxElapsedTime(maxElapsedTime);
        
        // 发送线程池
        int sendingThreadSize = getInt(CONFIG_SENDING_THREAD_SIZE_KEY, 1);
        this.sendingRecordSize = getInt(CONFIG_SENDING_RECORD_SIZE_KEY, batchSize / sendingThreadSize);
        
        if (sendingThreadSize == 1 && sendingRecordSize != batchSize)
        {
            // 单线程发送，指定sendingRecordSize没有用处，修改为batchSize的值
            LOGGER.warn("SendingRecordSize[{}] will be override by BatchSize[{}] when SendingThreadSize is 1, ",
                sendingRecordSize,
                batchSize);
            sendingRecordSize = batchSize;
        }
        
        partitionKeyDelimiter = get(CONFIG_PARTITION_KEY_DELIMITER, DEFAULT_PARTITION_KEY_SPLIT);
        String partitionKeyOptionStr = get(CONFIG_PARTITION_KEY_OPTION, PartitionKeyOption.RANDOM_INT.name());
        partitionKeyOptionList = new ArrayList<>();
        for (String option : partitionKeyOptionStr.split(partitionKeyDelimiter, -1))
        {
            partitionKeyOptionList.add(option.trim());
        }
        
        // 初始化DIS Client
        disConfig = getDISConfig();
        
        disAsyncClient = new DISClientAsync(disConfig,
            new ThreadPoolExecutor(sendingThreadSize, sendingThreadSize, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new ThreadFactoryBuilder().setNameFormat("sender-%03d-" + this.streamName).build()));
        
        disClient = new DISClient(disConfig);
        if (sinkCounter == null)
        {
            sinkCounter = new SinkCounter(getName());
        }

        requestBytesLimit = getLong(CONFIG_REQUEST_BYTES_LIMIT, requestBytesLimit);
        
    }
    
    @Override
    public synchronized void start()
    {
        super.start();
        
        // 启动时调用获取迭代器接口测试DIS配置是否异常，如果有DIS异常则抛出Error不启动此Sink
        GetPartitionCursorRequest request = new GetPartitionCursorRequest();
        request.setStreamName(streamName);
        request.setPartitionId("0");
        request.setCursorType(PartitionCursorTypeEnum.LATEST.name());
        try
        {
            disClient.getPartitionCursor(request);
        }
        catch (DISClientException e)
        {
            String msg = e.getMessage();
            Throwable cause = e.getCause();
            if (cause instanceof HttpClientErrorException || cause instanceof UnknownHttpStatusCodeException)
            {
                if (((RestClientResponseException)cause).getRawStatusCode() == HttpStatus.SC_FORBIDDEN)
                {
                    msg = String.format(
                        "Error message [%s], this ip may have been locked due to too many invalid calls, please check configuration and retry later.",
                        msg);
                }
                else if (((RestClientResponseException)cause).getRawStatusCode() == AUTHENTICATION_ERROR_HTTP_CODE)
                {
                    msg = String.format("Error message [%s], please check configuration and retry later.", msg);
                }
            }
            LOGGER.error(msg);
            throw new Error(e);
        }
        catch (ResourceAccessException e)
        {
            // 网络异常只输出信息，不抛出
            LOGGER.error("Failed to access endpoint [{}].", e.getMessage(), e);
        }
        catch (Exception e)
        {
            LOGGER.error(e.getMessage(), e);
            throw e;
        }
        
        sinkCounter.start();
        isRunning = true;
        LOGGER.info("Dis flume sink [" + getName() + "] start.");
    }
    
    @Override
    public void stop()
    {
        isRunning = false;
        sinkCounter.stop();
        super.stop();
        LOGGER.info("Dis flume sink [" + getName() + "] stop.");
    }
    
    private DISConfig getDISConfig()
    {
        DISConfig disConfig = DISConfig.buildConfig((String)null);
        disConfig.putAll(this.properties);
        
        updateDisConfigParam(disConfig,
            DISConfig.PROPERTY_REGION_ID,
            get(CONFIG_REGION_KEY, CONFIG_DEFAULT_REGION),
            true);
        updateDisConfigParam(disConfig, DISConfig.PROPERTY_AK, properties.get(CONFIG_ACCESS_KEY), true);
        updateDisConfigParam(disConfig, DISConfig.PROPERTY_SK, properties.get(CONFIG_SECRET_KEY), true);
        updateDisConfigParam(disConfig, DISConfig.PROPERTY_PROJECT_ID, properties.get(CONFIG_PROJECT_ID_KEY), true);
        updateDisConfigParam(disConfig, DISConfig.PROPERTY_ENDPOINT, properties.get(CONFIG_ENDPOINT_KEY), false);
        updateDisConfigParam(disConfig,
            DISConfig.PROPERTY_IS_DEFAULT_DATA_ENCRYPT_ENABLED,
            properties.get(CONFIG_DATA_ENCRYPT_ENABLED_KEY),
            false);
        updateDisConfigParam(disConfig,
            DISConfig.PROPERTY_DATA_PASSWORD,
            properties.get(CONFIG_DATA_PASSWORD_KEY),
            false);
        updateDisConfigParam(disConfig,
            DISConfig.PROPERTY_CONNECTION_TIMEOUT,
            properties.get(CONFIG_CONNECTION_TIMEOUT_KEY),
            false);
        updateDisConfigParam(disConfig,
            DISConfig.PROPERTY_SOCKET_TIMEOUT,
            properties.get(CONFIG_SOCKET_TIMEOUT_KEY),
            false);
        updateDisConfigParam(disConfig,
            DISConfig.PROPERTY_MAX_PER_ROUTE,
            properties.get(CONFIG_DEFAULT_MAX_PER_ROUTE_KEY),
            false);
        updateDisConfigParam(disConfig,
            DISConfig.PROPERTY_MAX_TOTAL,
            properties.get(CONFIG_DEFAULT_MAX_TOTAL_KEY),
            false);
        updateDisConfigParam(disConfig,
            DISConfig.PROPERTY_IS_DEFAULT_TRUSTED_JKS_ENABLED,
            get(CONFIG_IS_DEFAULT_TRUSTED_JKS_ENABLED_KEY, "false"),
            false);
        updateDisConfigParam(disConfig,
            DISConfig.PROPERTY_CONFIG_PROVIDER_CLASS,
            properties.get(CONFIG_PROVIDER_CLASS_KEY),
            false);
        updateDisConfigParam(disConfig,
            DISConfig.PROPERTY_BODY_SERIALIZE_TYPE,
            get(CONFIG_BODY_SERIALIZE_TYPE_KEY, DISConfig.BodySerializeType.protobuf.name()),
            false);
        
        return disConfig;
    }
    
    @Override
    public Status process()
        throws EventDeliveryException
    {
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        transaction.begin();
        try
        {
            Event event;
            int txnEventCount = 0;
            List<Event> events = new ArrayList<>(batchSize);
            long beginTime = System.currentTimeMillis();
            while (txnEventCount < batchSize && (System.currentTimeMillis() - beginTime) < maxBufferAgeMillis)
            {
                event = channel.take();
                if (event != null && event.getBody().length > 0)
                {
                    events.add(event);
                    txnEventCount++;
                }
            }
            
            if (txnEventCount == 0)
            {
                // 没有获取到有效数据，不发送
                sinkCounter.incrementBatchEmptyCount();
                transaction.commit();
                return Status.BACKOFF;
            }
            
            long startMS = System.currentTimeMillis();
            // 准备处理的event的个数
            sinkCounter.addToEventDrainAttemptCount(txnEventCount);
            if (batchSize == txnEventCount)
            {
                // 本次处理数量与batchSize相同
                sinkCounter.incrementBatchCompleteCount();
            }
            else
            {
                // 本次处理数量在0~batchSize之间
                sinkCounter.incrementBatchUnderflowCount();
            }
            
            Map<PutRecordsRequest, Future<PutRecordsResult>> futureMap = new ConcurrentHashMap<>();
            Map<PutRecordsRequest, Integer> retryCountMap = new ConcurrentHashMap<>();
            List<PutRecordsRequestEntry> putRecordsRequestEntryList = new ArrayList<>();
            long bytesCount = 0;
            for (int i = 0; i < events.size(); i++)
            {
                byte[] body = events.get(i).getBody();
                PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();
                putRecordsRequestEntry.setData(ByteBuffer.wrap(body));
                putRecordsRequestEntry.setPartitionKey(generatePartitionKey(events.get(i)));
                putRecordsRequestEntryList.add(putRecordsRequestEntry);
                bytesCount += body.length;
                if ((i + 1) % this.sendingRecordSize == 0 || (i + 1) == events.size()
                    || bytesCount >= this.requestBytesLimit)
                {
                    PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
                    putRecordsRequest.setStreamName(this.streamName);
                    putRecordsRequest.setRecords(putRecordsRequestEntryList);
                    futureMap.put(putRecordsRequest, send(putRecordsRequest, 0));
                    putRecordsRequestEntryList = new ArrayList<>();
                    bytesCount = 0;
                }
            }
            
            while (!futureMap.isEmpty())
            {
                LOGGER.debug("Future map size {}, queue size {}", futureMap.size());
                PutRecordsRequest putRecordsRequest = receivedQueue.take();
                Future<PutRecordsResult> future = futureMap.get(putRecordsRequest);
                PutRecordsResult putRecordsResult = null;
                try
                {
                    putRecordsResult = future.get();
                }
                catch (Throwable e)
                {
                    // 接口等异常情况记录
                    sinkCounter.incrementConnectionFailedCount();
                    LOGGER.error("Error occurs when sending {} events. Will try again.",
                        putRecordsRequest.getRecords().size(),
                        e);
                }
                finally
                {
                    futureMap.remove(putRecordsRequest);
                }
                
                PutRecordsRequest retryPutRecordsRequest = null;
                if (putRecordsResult == null
                    || putRecordsResult.getFailedRecordCount().get() == putRecordsRequest.getRecords().size())
                {
                    // 全部失败，直接重试
                    retryPutRecordsRequest = putRecordsRequest;
                }
                else if (putRecordsResult.getFailedRecordCount().get() > 0)
                {
                    List<PutRecordsRequestEntry> retryPutRecordsRequestEntryList = new ArrayList<>();
                    for (int j = 0; j < putRecordsResult.getRecords().size(); j++)
                    {
                        PutRecordsResultEntry putRecordsRequestEntry = putRecordsResult.getRecords().get(j);
                        if (!StringUtils.isNullOrEmpty(putRecordsRequestEntry.getErrorCode()))
                        {
                            // 记录上传失败
                            retryPutRecordsRequestEntryList.add(putRecordsRequest.getRecords().get(j));
                        }
                    }
                    // 部分失败，重试部分
                    retryPutRecordsRequest = new PutRecordsRequest();
                    retryPutRecordsRequest.setStreamName(this.streamName);
                    retryPutRecordsRequest.setRecords(retryPutRecordsRequestEntryList);
                    retryCountMap.remove(putRecordsRequest);
                }
                else
                {
                    retryCountMap.remove(putRecordsRequest);
                }
                
                if (retryPutRecordsRequest != null)
                {
                    // 需要重试
                    if (retrySize > 0 && isRunning && (retryCountMap.get(retryPutRecordsRequest) == null
                        || retryCountMap.get(retryPutRecordsRequest) < retrySize))
                    {
                        if (this.nextBackOff == BackOffExecution.STOP)
                        {
                            break;
                        }
                        if (retryCountMap.get(retryPutRecordsRequest) == null)
                        {
                            retryCountMap.put(retryPutRecordsRequest, 1);
                        }
                        else
                        {
                            retryCountMap.put(retryPutRecordsRequest, retryCountMap.get(retryPutRecordsRequest) + 1);
                        }
                        futureMap.put(retryPutRecordsRequest,
                            send(retryPutRecordsRequest, retryCountMap.get(retryPutRecordsRequest)));
                    }
                    // 超出重试次数
                    else
                    {
                        LOGGER.error("Failed to put [{}] events to [{}], the retrySize[{}] has been reached.",
                            new Object[] {events.size(), this.streamName, this.retrySize});
                        // 设置为发送失败，且回退发送
                        transaction.rollback();
                        return Status.BACKOFF;
                    }
                }
            }
            
            if (resultLogLevel != RESULT_LOG_LEVEL.OFF)
            {
                outputLog("Put [{}] events to [{}] spend {}ms, totalPutCount {}",
                    events.size(),
                    this.streamName,
                    System.currentTimeMillis() - startMS,
                    totalPutCount.get());
            }
            
            // 发送成功
            transaction.commit();
            return Status.READY;
        }
        catch (Throwable th)
        {
            transaction.rollback();
            if (th instanceof InterruptedException)
            {
                return Status.BACKOFF;
            }
            else if (th instanceof Error)
            {
                LOGGER.error("Process failed", th.getMessage());
                throw (Error)th;
            }
            else
            {
                LOGGER.error("Process failed", th.getMessage());
                throw new EventDeliveryException(th);
            }
        }
        finally
        {
            if (receivedQueue.size() != 0)
            {
                // 重置响应队列
                LOGGER.warn("ReceivedQueue has {} element, will be clear.", receivedQueue.size());
                receivedQueue.clear();
            }
            
            transaction.close();
        }
    }
    
    private Future<PutRecordsResult> send(PutRecordsRequest putRecordsRequest, final int retryCount)
        throws InterruptedException
    {
        long localNextBackOff = this.nextBackOff;
        if (localNextBackOff > 0)
        {
            Thread.sleep(localNextBackOff);
        }
        
        return disAsyncClient.putRecordsAsync(putRecordsRequest, new AsyncHandler<PutRecordsResult>()
        {
            long startTime = System.currentTimeMillis();
            
            int sendSize = putRecordsRequest.getRecords().size();
            
            @Override
            public void onError(Exception e)
            {
                LOGGER.error(
                    "CurrentPut {} events[success 0 / failed {}] spend {} ms. Retry Count [{}]. Failure info [{}].",
                    new Object[] {sendSize, sendSize, (System.currentTimeMillis() - startTime), retryCount,
                        e.getMessage()});
                PutRecordsResult putRecordsResult = new PutRecordsResult();
                putRecordsResult.setFailedRecordCount(new AtomicInteger(putRecordsRequest.getRecords().size()));
                putRecordsResult.setRecords(Collections.emptyList());
                getNextBackOff();
                try
                {
                    receivedQueue.put(putRecordsRequest);
                }
                catch (InterruptedException e1)
                {
                    LOGGER.error(e1.getMessage(), e1);
                }
            }
            
            @Override
            public void onSuccess(PutRecordsResult putRecordsResult)
            {
                int failedCount = putRecordsResult.getFailedRecordCount().get();
                int successCount = putRecordsResult.getRecords().size() - putRecordsResult.getFailedRecordCount().get();
                
                if (resultLogLevel != RESULT_LOG_LEVEL.OFF)
                {
                    String logMsg = "CurrentPut {} events[success {} / failed {}] spend {} ms.";
                    List<Object> logObject = new ArrayList<>();
                    logObject.add(putRecordsResult.getRecords().size());
                    logObject.add(successCount);
                    logObject.add(failedCount);
                    logObject.add(System.currentTimeMillis() - startTime);
                    
                    if (failedCount > 0)
                    {
                        String errorMsg = "";
                        for (PutRecordsResultEntry putRecordsResultEntry : putRecordsResult.getRecords())
                        {
                            if (!StringUtils.isNullOrEmpty(putRecordsResultEntry.getErrorCode()))
                            {
                                errorMsg = putRecordsResultEntry.getErrorCode() + "|"
                                    + putRecordsResultEntry.getErrorMessage();
                                break;
                            }
                        }
                        logMsg += " Failure info [{}].";
                        logObject.add(errorMsg);
                    }
                    
                    if (retryCount > 0)
                    {
                        logMsg += " Retry Count [{}].";
                        logObject.add(retryCount);
                    }
                    outputLog(logMsg, logObject.toArray());
                }
                sinkCounter.addToEventDrainSuccessCount(successCount);
                totalPutCount.addAndGet(successCount);
                
                if (failedCount > 0)
                {
                    getNextBackOff();
                }
                else
                {
                    if (execution != null)
                    {
                        // 请求成功，重置退避算法计时器
                        synchronized (backOffLock)
                        {
                            execution = null;
                            nextBackOff = 0;
                        }
                    }
                }
                try
                {
                    receivedQueue.put(putRecordsRequest);
                }
                catch (InterruptedException e1)
                {
                    LOGGER.error(e1.getMessage(), e1);
                }
            }
        });
    }
    
    private void getNextBackOff()
    {
        synchronized (backOffLock)
        {
            if (execution == null)
            {
                execution = backOff.start();
            }
            this.nextBackOff = execution.nextBackOff();
        }
    }
    
    private void updateDisConfigParam(DISConfig disConfig, String param, Object value, boolean isRequired)
    {
        if (value == null)
        {
            if (isRequired)
            {
                throw new IllegalArgumentException("param [" + param + "]is null.");
            }
            return;
        }
        if (param.equals(DISConfig.PROPERTY_SK)) {
            value = tryGetDecryptValue(param, value.toString(), false);
        }
        disConfig.set(param, value.toString());
    }

    protected String tryGetDecryptValue(String key, String value, boolean ignoreException)
    {
        if (value == null)
        {
            return null;
        }
        String encryptKey = null;
        if (properties.get(CONFIG_ENCRYPT_KEY) != null)
        {
            encryptKey = String.valueOf(properties.get(CONFIG_ENCRYPT_KEY));
        }
        else
        {
            encryptKey = "";
        }
        // 168 is the Minimum length of encrypt value.
        if (value.length() >= 168)
        {
            try
            {
                LOGGER.info("Try to decrypt [{}].", key);
                return EncryptTool.decrypt(value, encryptKey);
            }
            catch (Exception e)
            {
                if (!ignoreException)
                {
                    LOGGER.error("Failed to decrypt [{}].", key);
                    throw e;
                }
                else
                {
                    LOGGER.warn("Try to decrypt but not success. key=[{}].", key);
                }
            }
        }
        return value;
    }
    
    protected String get(String propName, String defaultValue)
    {
        if (this.properties.containsKey(propName))
        {
            String value = properties.getProperty(propName);
            if (value != null)
            {
                return value.trim();
            }
        }
        return defaultValue;
    }
    
    protected int getInt(String propName, int defaultValue)
    {
        String value = get(propName, null);
        if (value != null)
        {
            try
            {
                return Integer.parseInt(value);
            }
            catch (NumberFormatException e)
            {
                LOGGER.error(e.getMessage(), e);
                return defaultValue;
            }
        }
        return defaultValue;
    }

    protected long getLong(String propName, long defaultValue)
    {
        String value = get(propName, null);
        if (value != null)
        {
            try
            {
                return Long.parseLong(value);
            }
            catch (NumberFormatException e)
            {
                LOGGER.error(e.getMessage(), e);
                return defaultValue;
            }
        }
        return defaultValue;
    }
    
    /**
     * 判断是否为客户端异常
     *
     * @param t
     * @return
     */
    protected boolean isClientError(Throwable t)
    {
        if (t instanceof HttpClientErrorException)
        {
            // 400 401 403
            return String.valueOf(((HttpClientErrorException)t).getRawStatusCode()).startsWith("4");
        }
        else if (t instanceof UnknownHttpStatusCodeException)
        {
            // 441
            return String.valueOf(((UnknownHttpStatusCodeException)t).getRawStatusCode()).startsWith("4");
        }
        else if (t.getClass().getPackage().getName().startsWith("java.lang"))
        {
            return true;
        }
        else if (t.getCause() != null)
        {
            return isClientError(t.getCause());
        }
        return false;
    }
    
    protected boolean isNetworkError(Throwable t)
    {
        return t instanceof ConnectTimeoutException || t instanceof SocketTimeoutException
            || t instanceof ResourceAccessException || (t.getCause() != null && isNetworkError(t.getCause()));
    }
    
    private void outputLog(String log, Object... params)
    {
        switch (resultLogLevel)
        {
            case DEBUG:
                LOGGER.debug(log, params);
                break;
            case INFO:
                LOGGER.info(log, params);
                break;
            case WARN:
                LOGGER.warn(log, params);
                break;
            case ERROR:
                LOGGER.error(log, params);
                break;
        }
    }
    
    private String generatePartitionKey(String option, Event event)
    {
        Preconditions.checkNotNull(option);
        
        if (PartitionKeyOption.RANDOM_INT.name().equalsIgnoreCase(option))
        {
            return String.valueOf(ThreadLocalRandom.current().nextInt(9999999));
        }
        else
        {
            String partitionKey = "null";
            Map<String, String> headers = event.getHeaders();
            if (headers != null && !StringUtils.isNullOrEmpty(headers.get(option)))
            {
                partitionKey = headers.get(option);
            }
            return partitionKey;
        }
    }
    
    private String generatePartitionKey(Event event)
    {
        String[] keys = new String[partitionKeyOptionList.size()];
        for (int i = 0; i < partitionKeyOptionList.size(); i++)
        {
            keys[i] = generatePartitionKey(partitionKeyOptionList.get(i), event);
        }
        return org.apache.commons.lang.StringUtils.join(keys, partitionKeyDelimiter);
    }
    
    /**
     * 接口响应信息记录日志级别
     */
    private enum RESULT_LOG_LEVEL
    {
        OFF, DEBUG, INFO, WARN, ERROR
    }
    
    /**
     * PartitionKey设置
     */
    private enum PartitionKeyOption
    {
        RANDOM_INT
    }
}