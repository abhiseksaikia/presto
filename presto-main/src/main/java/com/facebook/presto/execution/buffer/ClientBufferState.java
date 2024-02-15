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
package com.facebook.presto.execution.buffer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.MoreObjects.toStringHelper;

public class ClientBufferState
{
    private final String bufferId;
    private final int pageSize;
    private final long currentSequenceID;

    @JsonCreator
    public ClientBufferState(@JsonProperty("bufferId") String bufferId, @JsonProperty("pageSize") int pageSize, @JsonProperty("currentSequenceID") long currentSequenceID)
    {
        this.bufferId = bufferId;
        this.pageSize = pageSize;
        this.currentSequenceID = currentSequenceID;
    }

    @JsonProperty
    public String getBufferId()
    {
        return bufferId;
    }

    @JsonProperty
    public int getPageSize()
    {
        return pageSize;
    }

    @JsonProperty
    public long getCurrentSequenceID()
    {
        return currentSequenceID;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("bufferId", bufferId)
                .add("pageSize", pageSize)
                .add("currentSequenceID", currentSequenceID)
                .toString();
    }
}
