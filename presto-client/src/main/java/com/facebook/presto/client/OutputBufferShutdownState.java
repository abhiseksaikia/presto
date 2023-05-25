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

import java.util.List;

public class OutputBufferShutdownState
{
    private String type;
    private long totalBufferedBytes;
    private long totalBufferedPages;
    private long totalPagesSent;
    private List<Integer> bufferedPages;

    @JsonCreator
    public OutputBufferShutdownState(
            @JsonProperty("type") String type,
            @JsonProperty("totalBufferedBytes") long totalBufferedBytes,
            @JsonProperty("totalBufferedPages") long totalBufferedPages,
            @JsonProperty("totalPagesSent") long totalPagesSent,
            @JsonProperty("bufferedPages") List<Integer> bufferedPages)
    {
        this.type = type;
        this.totalBufferedBytes = totalBufferedBytes;
        this.totalBufferedPages = totalBufferedPages;
        this.totalPagesSent = totalPagesSent;
        this.bufferedPages = bufferedPages;
    }

    @JsonProperty
    public String getType()
    {
        return type;
    }

    @JsonProperty
    public long getTotalBufferedBytes()
    {
        return totalBufferedBytes;
    }

    @JsonProperty
    public long getTotalBufferedPages()
    {
        return totalBufferedPages;
    }

    @JsonProperty
    public long getTotalPagesSent()
    {
        return totalPagesSent;
    }

    @JsonProperty
    public List<Integer> getBufferedPages()
    {
        return bufferedPages;
    }
}
