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
package com.facebook.presto.operator.aggregation.approxmostfrequent.exp;

import com.facebook.presto.array.ObjectBigArray;
import com.facebook.presto.operator.aggregation.state.AbstractGroupedAccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateFactory;

public class ApproximateMostFrequentStateFactory
        implements AccumulatorStateFactory<ApproximateMostFrequentState>
{
    @Override
    public ApproximateMostFrequentState createSingleState()
    {
        return new SingleApproximateMostFrequentState();
    }

    @Override
    public Class<? extends ApproximateMostFrequentState> getSingleStateClass()
    {
        return SingleApproximateMostFrequentState.class;
    }

    @Override
    public ApproximateMostFrequentState createGroupedState()
    {
        return new GroupedApproximateMostFrequentState();
    }

    @Override
    public Class<? extends ApproximateMostFrequentState> getGroupedStateClass()
    {
        return GroupedApproximateMostFrequentState.class;
    }

    public static class SingleApproximateMostFrequentState
            implements ApproximateMostFrequentState
    {
        private ApproximateMostFrequentFlattenHistogram histogram;
        private long size;

        @Override
        public ApproximateMostFrequentFlattenHistogram get()
        {
            return histogram;
        }

        @Override
        public void set(ApproximateMostFrequentFlattenHistogram histogram)
        {
            this.histogram = histogram;
            size = histogram.estimatedInMemorySize();
        }

        @Override
        public long getEstimatedSize()
        {
            return size;
        }
    }

    public static class GroupedApproximateMostFrequentState
            extends AbstractGroupedAccumulatorState
            implements ApproximateMostFrequentState
    {
        private final ObjectBigArray<ApproximateMostFrequentFlattenHistogram> histograms = new ObjectBigArray<>();
        private long size;

        @Override
        public ApproximateMostFrequentFlattenHistogram get()
        {
            return histograms.get(getGroupId());
        }

        @Override
        public void set(ApproximateMostFrequentFlattenHistogram histogram)
        {
            ApproximateMostFrequentFlattenHistogram previous = get();
            if (previous != null) {
                size -= previous.estimatedInMemorySize();
            }

            histograms.set(getGroupId(), histogram);
            size += histogram.estimatedInMemorySize();
        }

        @Override
        public void ensureCapacity(long size)
        {
            histograms.ensureCapacity(size);
        }

        @Override
        public long getEstimatedSize()
        {
            return size + histograms.sizeOf();
        }
    }
}
