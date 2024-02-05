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
package com.facebook.presto.execution;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public class PageKey
{
    private final String taskID;
    private final String bufferId;

    public PageKey(String taskID, String bufferId)
    {
        this.taskID = taskID;
        this.bufferId = bufferId;
    }

    public String getTaskID()
    {
        return taskID;
    }

    public String getBufferId()
    {
        return bufferId;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("taskID", taskID)
                .add("bufferId", bufferId)
                .toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PageKey pageKey = (PageKey) o;
        return Objects.equals(taskID, pageKey.taskID) && Objects.equals(bufferId, pageKey.bufferId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(taskID, bufferId);
    }
}
