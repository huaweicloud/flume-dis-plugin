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

package com.huaweicloud.dis.adapter.flume.sink.backoff;

import com.huaweicloud.dis.exception.DISClientException;

public class ExponentialBackOff
{
    
    /**
     * The default initial interval.
     */
    public static final long DEFAULT_INITIAL_INTERVAL = 2000L;
    
    /**
     * The default multiplier (increases the interval by 50%).
     */
    public static final double DEFAULT_MULTIPLIER = 1.5;
    
    /**
     * The default maximum back off time.
     */
    public static final long DEFAULT_MAX_INTERVAL = 30000L;
    
    /**
     * The default maximum elapsed time.
     */
    public static final long DEFAULT_MAX_ELAPSED_TIME = Long.MAX_VALUE;
    
    private long initialInterval = DEFAULT_INITIAL_INTERVAL;
    
    private double multiplier = DEFAULT_MULTIPLIER;
    
    private long maxInterval = DEFAULT_MAX_INTERVAL;
    
    private long maxElapsedTime = DEFAULT_MAX_ELAPSED_TIME;
    
    /**
     * Create an instance with the default settings.
     *
     * @see #DEFAULT_INITIAL_INTERVAL
     * @see #DEFAULT_MULTIPLIER
     * @see #DEFAULT_MAX_INTERVAL
     * @see #DEFAULT_MAX_ELAPSED_TIME
     */
    public ExponentialBackOff()
    {
    }
    
    /**
     * Create an instance with the supplied settings.
     *
     * @param initialInterval the initial interval in milliseconds
     * @param multiplier the multiplier (should be greater than or equal to 1)
     */
    public ExponentialBackOff(long initialInterval, double multiplier)
    {
        checkMultiplier(multiplier);
        this.initialInterval = initialInterval;
        this.multiplier = multiplier;
    }
    
    /**
     * The initial interval in milliseconds.
     */
    public void setInitialInterval(long initialInterval)
    {
        this.initialInterval = initialInterval;
    }
    
    /**
     * Return the initial interval in milliseconds.
     */
    public long getInitialInterval()
    {
        return initialInterval;
    }
    
    /**
     * The value to multiply the current interval by for each retry attempt.
     */
    public void setMultiplier(double multiplier)
    {
        checkMultiplier(multiplier);
        this.multiplier = multiplier;
    }
    
    /**
     * Return the value to multiply the current interval by for each retry attempt.
     */
    public double getMultiplier()
    {
        return multiplier;
    }
    
    /**
     * The maximum back off time.
     */
    public void setMaxInterval(long maxInterval)
    {
        this.maxInterval = maxInterval;
    }
    
    /**
     * Return the maximum back off time.
     */
    public long getMaxInterval()
    {
        return maxInterval;
    }
    
    /**
     * The maximum elapsed time in milliseconds after which a call to {@link BackOffExecution#nextBackOff()} returns
     * {@link BackOffExecution#STOP}.
     */
    public void setMaxElapsedTime(long maxElapsedTime)
    {
        this.maxElapsedTime = maxElapsedTime;
    }
    
    /**
     * Return the maximum elapsed time in milliseconds after which a call to {@link BackOffExecution#nextBackOff()}
     * returns {@link BackOffExecution#STOP}.
     */
    public long getMaxElapsedTime()
    {
        return maxElapsedTime;
    }
    
    public BackOffExecution start()
    {
        return new ExponentialBackOffExecution();
    }
    
    private void checkMultiplier(double multiplier)
    {
        if (multiplier < 1)
        {
            throw new DISClientException("Invalid multiplier '" + multiplier
                + "'. Should be greater than or equal to 1." + " A multiplier of 1 is equivalent to a fixed interval.");
        }
    }
    
    private class ExponentialBackOffExecution implements BackOffExecution
    {
        
        private long currentInterval = -1;
        
        private long currentElapsedTime = 0;
        
        @Override
        public long nextBackOff()
        {
            if (this.currentElapsedTime >= maxElapsedTime)
            {
                return STOP;
            }
            
            long nextInterval = computeNextInterval();
            this.currentElapsedTime += nextInterval;
            return nextInterval;
        }
        
        private long computeNextInterval()
        {
            long maxInterval = getMaxInterval();
            if (this.currentInterval >= maxInterval)
            {
                return maxInterval;
            }
            else if (this.currentInterval < 0)
            {
                long initialInterval = getInitialInterval();
                this.currentInterval = (initialInterval < maxInterval ? initialInterval : maxInterval);
            }
            else
            {
                this.currentInterval = multiplyInterval(maxInterval);
            }
            return this.currentInterval;
        }
        
        private long multiplyInterval(long maxInterval)
        {
            long i = this.currentInterval;
            i *= getMultiplier();
            return (i > maxInterval ? maxInterval : i);
        }
        
        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder("ExponentialBackOff{");
            sb.append("currentInterval=").append(this.currentInterval < 0 ? "n/a" : this.currentInterval + "ms");
            sb.append(", multiplier=").append(getMultiplier());
            sb.append('}');
            return sb.toString();
        }
    }
}