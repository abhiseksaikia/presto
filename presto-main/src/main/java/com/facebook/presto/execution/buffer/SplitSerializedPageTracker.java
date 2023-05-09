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

import com.facebook.presto.execution.Lifespan;
import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class SplitSerializedPageTracker
    implements SerializedPageReference.SplitDrainedListener
{
    private final ConcurrentMap<Long, AtomicLong> outstandingPageCountPerSplit = new ConcurrentHashMap<>();
    private final Set<Long> transmittedSplits = new HashSet<>();
    private final Set<Long> noMorePagesForSplit = ConcurrentHashMap.newKeySet();

    private volatile Consumer<Long> splitDrainedCallback;

    public SplitSerializedPageTracker()
    {

    }

    public void incrementSplitPageCount(OptionalLong splitSequenceID, int pagesAdded)
    {
        if (!splitSequenceID.isPresent()) {
            return;
        }

        long splitID = splitSequenceID.getAsLong();
        transmittedSplits.add(splitID);
        AtomicLong counter = outstandingPageCountPerSplit.get(splitID);
        if (counter == null) {
            counter = outstandingPageCountPerSplit.computeIfAbsent(splitID, ignore -> new AtomicLong());
        }
        counter.addAndGet(pagesAdded);
    }

    public void addTransmittedSplit(Long splitID)
    {
        transmittedSplits.add(splitID);
    }

    public Set<Long> getTransmittedSplitIDs()
    {
        return transmittedSplits;
    }

    public void registerSplitDrainedCallback(Consumer<Long> callback)
    {
        checkState(splitDrainedCallback == null, "splitDrainedCallback is already set");
        this.splitDrainedCallback = requireNonNull(callback, "callback is null");
    }

    public void setNoMorePagesForSplit(Long splitID)
    {
        noMorePagesForSplit.add(splitID);
    }

    public boolean isGracefulDrained()
    {
        boolean hasPendingPages = false;
        for (Long splitID: transmittedSplits) {
            long pageCount = outstandingPageCountPerSplit.get(splitID).get();
            if (pageCount > 0) {
                hasPendingPages = true;
            }
        }

        return !hasPendingPages;
    }

    public void onSplitDrained(OptionalLong optionalSplitID, int releasedPageCount)
    {
        if (optionalSplitID.isPresent()) {
            Long splitID = optionalSplitID.getAsLong();
            long outstandingPageCount = outstandingPageCountPerSplit.get(splitID).addAndGet(-releasedPageCount);
            if (outstandingPageCount == 0 && noMorePagesForSplit.contains(splitID)) {
                Consumer<Long> splitDrainedCallback = this.splitDrainedCallback;
                checkState(splitDrainedCallback != null, "lifespanCompletionCallback is not null");

                // callback to make the coordinator know that the split is completed.
                splitDrainedCallback.accept(splitID);
                outstandingPageCountPerSplit.remove(splitID);
            }
        }
    }
}
