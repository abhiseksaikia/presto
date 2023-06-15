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

import com.google.common.util.concurrent.AtomicDouble;
import org.weakref.jmx.Managed;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.atomic.AtomicLong;

@ThreadSafe
public class ExchangeClientStats
{
    private final AtomicLong retainedBytes = new AtomicLong();
    private final AtomicLong neededBytes = new AtomicLong();
    private final AtomicLong scheduledClientCount = new AtomicLong();
    private final AtomicLong longPollingNotifiedCount = new AtomicLong();

    private final AtomicDouble p95UpstreamFetchInterval = new AtomicDouble();
    private final AtomicDouble p99UpstreamFetchInterval = new AtomicDouble();

    @Managed
    public double getP99UpstreamFetchInterval()
    {
        return p99UpstreamFetchInterval.get();
    }

    public void setP99UpstreamFetchInterval(double upstreamFetchInterval)
    {
        this.p99UpstreamFetchInterval.set(upstreamFetchInterval);
    }

    @Managed
    public double getP95UpstreamFetchInterval()
    {
        return p95UpstreamFetchInterval.get();
    }

    public void setP95UpstreamFetchInterval(double upstreamFetchInterval)
    {
        this.p95UpstreamFetchInterval.set(upstreamFetchInterval);
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
    public long getScheduledClientCount()
    {
        return scheduledClientCount.get();
    }

    public void setScheduledClientCount(long clientCount)
    {
        scheduledClientCount.set(clientCount);
    }

    @Managed
    public long getNeededBytes()
    {
        return neededBytes.get();
    }

    public void setNeededBytes(long bytes)
    {
        neededBytes.set(bytes);
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
