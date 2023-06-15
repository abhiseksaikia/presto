/*
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
package com.facebook.presto.operator;

import org.weakref.jmx.Managed;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.atomic.AtomicLong;

@ThreadSafe
public class ExchangeClientStats
{
    private final AtomicLong retainedBytes = new AtomicLong();
    private final AtomicLong scheduledClientCountLessThanTen = new AtomicLong();
    private final AtomicLong longPollingNotifiedCount = new AtomicLong();
    private final AtomicLong bufferUsedUp = new AtomicLong();
    private final AtomicLong upstreamFetchIntervalOverTenSecond = new AtomicLong();

    @Managed
    public long getUpstreamFetchIntervalOverTenSecond()
    {
        return upstreamFetchIntervalOverTenSecond.get();
    }

    public void addUpstreamFetchIntervalOverTenSecond()
    {
        upstreamFetchIntervalOverTenSecond.addAndGet(1);
    }

    @Managed
    public long getBufferUsedUp()
    {
        return bufferUsedUp.get();
    }

    public void addBufferUsedUp()
    {
        bufferUsedUp.addAndGet(1);
    }

    @Managed
    public long getLongPollingNotifiedCount()
    {
        return longPollingNotifiedCount.get();
    }

    public void addLongPollingNotifiedCount()
    {
        longPollingNotifiedCount.addAndGet(1);
    }

    @Managed
    public long getScheduledClientCountLessThanTen()
    {
        return scheduledClientCountLessThanTen.get();
    }

    public void addScheduledClientCountLessThanTen()
    {
        scheduledClientCountLessThanTen.addAndGet(1);
    }
    @Managed
    public long getRetainedBytes()
    {
        return retainedBytes.get();
    }

    public void addRetainedBytes(long bytes)
    {
        retainedBytes.addAndGet(bytes);
    }

    public void removeRetainedBytes(long bytes)
    {
        retainedBytes.addAndGet(bytes * -1);
    }
}
