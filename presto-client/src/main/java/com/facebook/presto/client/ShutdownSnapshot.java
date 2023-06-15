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
package com.facebook.presto.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

import java.util.Set;

@Immutable
public class ShutdownSnapshot
{
    private int taskNumNoPageAdded;
    private int taskNumToDrain;
    private int taskNumBeKilled;
    private long shutdownStartTime;
    private long shutdownFinishKillTime;
    private long shutdownStartDrainTime;
    private long shutdownFinishDrainTime;
    private long shutdownFinishRunningSplitsTime;

    private double p95BufferFetchInterval;
    private double p99BufferFetchInterval;
    private Set<OutputBufferShutdownState> outputBufferShutdownStatesSet;

    private long snapshotTime;

    @JsonCreator
    public ShutdownSnapshot(
            @JsonProperty("taskNumNoPageAdded") int taskNumNoPageAdded,
            @JsonProperty("taskNumToDrain") int taskNumToDrain,
            @JsonProperty("taskNumBeKilled") int taskNumBeKilled,
            @JsonProperty("shutdownStartTime") long shutdownStartTime,
            @JsonProperty("shutdownFinishKillTime") long shutdownFinishKillTime,
            @JsonProperty("shutdownStartDrainTime") long shutdownStartDrainTime,
            @JsonProperty("shutdownFinishDrainTime") long shutdownFinishDrainTime,
            @JsonProperty("shutdownFinishRunningSplitsTime") long shutdownFinishRunningSplitsTime,
            @JsonProperty("p95BufferFetchInterval") double p95BufferFetchInterval,
            @JsonProperty("p99BufferFetchInterval") double p99BufferFetchInterval,
            @JsonProperty("outputBufferShutdownStatesSet") Set<OutputBufferShutdownState> outputBufferShutdownStatesSet)
    {
        this.taskNumNoPageAdded = taskNumNoPageAdded;
        this.taskNumToDrain = taskNumToDrain;
        this.taskNumBeKilled = taskNumBeKilled;
        this.shutdownStartTime = shutdownStartTime;
        this.shutdownFinishKillTime = shutdownFinishKillTime;
        this.shutdownStartDrainTime = shutdownStartDrainTime;
        this.shutdownFinishDrainTime = shutdownFinishDrainTime;
        this.shutdownFinishRunningSplitsTime = shutdownFinishRunningSplitsTime;
        this.p95BufferFetchInterval = p95BufferFetchInterval;
        this.p99BufferFetchInterval = p99BufferFetchInterval;
        this.outputBufferShutdownStatesSet = outputBufferShutdownStatesSet;
        this.snapshotTime = System.currentTimeMillis();
    }

    @JsonProperty
    public int getTaskNumNoPageAdded()
    {
        return taskNumNoPageAdded;
    }

    @JsonProperty
    public int getTaskNumToDrain()
    {
        return taskNumToDrain;
    }

    @JsonProperty
    public int getTaskNumBeKilled()
    {
        return taskNumBeKilled;
    }

    @JsonProperty
    public long getShutdownStartTime()
    {
        return shutdownStartTime;
    }

    @JsonProperty
    public long getShutdownFinishKillTime()
    {
        return shutdownFinishKillTime;
    }

    @JsonProperty
    public long getShutdownStartDrainTime()
    {
        return shutdownStartDrainTime;
    }

    @JsonProperty
    public long getShutdownFinishDrainTime()
    {
        return shutdownFinishDrainTime;
    }

    @JsonProperty
    public long getShutdownFinishRunningSplitsTime()
    {
        return shutdownFinishRunningSplitsTime;
    }

    @JsonProperty
    public double getP95BufferFetchInterval()
    {
        return p95BufferFetchInterval;
    }

    @JsonProperty
    public double getP99BufferFetchInterval()
    {
        return p99BufferFetchInterval;
    }

    @JsonProperty
    public Set<OutputBufferShutdownState> getOutputBufferShutdownStatesSet()
    {
        return outputBufferShutdownStatesSet;
    }

    @JsonProperty
    public long getSnapshotTime()
    {
        return snapshotTime;
    }
}
