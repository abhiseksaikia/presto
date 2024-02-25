
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
package com.facebook.presto.server.remotetask;

import com.facebook.presto.execution.buffer.ClientBufferInfo;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class PageInitUploadRequest
{
    private final List<ClientBufferInfo> clientBufferInfos;

    @JsonCreator
    public PageInitUploadRequest(@JsonProperty("clientBufferStates") List<ClientBufferInfo> clientBufferInfos)
    {
        this.clientBufferInfos = requireNonNull(clientBufferInfos, "clientBufferInfos is null");
    }

    @JsonProperty
    public List<ClientBufferInfo> getClientBufferInfos()
    {
        return clientBufferInfos;
    }

    @Override
    public String toString()
    {
        return "PageInitUploadRequest{" +
                "clientBufferInfos=" + clientBufferInfos +
                '}';
    }
}
