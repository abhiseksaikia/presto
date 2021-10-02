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
package com.facebook.presto.operator.aggregation.approxmostfrequent;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.operator.aggregation.approxmostfrequent.exp.StreamSummary;
import com.facebook.presto.operator.aggregation.state.LongApproximateMostFrequentStateSerializer;
import com.facebook.presto.operator.aggregation.state.StringApproximateMostFrequentStateSerializer;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Map;

import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.util.StructuralTestUtil.mapType;
import static org.testng.Assert.assertEquals;

public class TestApproximateMostFrequentHistogram
{
    @Test
    public void testLongHistogram()
    {
        ApproximateMostFrequentHistogram<Long> histogram = new ApproximateMostFrequentHistogram<Long>(3, 15, LongApproximateMostFrequentStateSerializer::serializeBucket, LongApproximateMostFrequentStateSerializer::deserializeBucket);
        Arrays.asList(1L, 1L, 2L, 3L, 4L, 5L, 5L, 6L, 6L, 7L, 7L, 8L, 9L, 10L, 11L, 13L, 13L).stream().forEach(k -> histogram.add(k));

        Map<Long, Long> buckets = histogram.getBuckets();

        assertEquals(buckets.size(), 3);
        assertEquals(buckets, ImmutableMap.of(1L, 2L, 2L, 1L, 3L, 1L));
    }

    @Test
    public void testLongHistogramNew()
    {
        Block longsBlock = createLongsBlock(1L, 1L, 2L, 3L, 4L);
        StreamSummary histogram = new StreamSummary(BIGINT, 3, 15, 10);
        int pos = 0;
        histogram.add(longsBlock, pos++, 1);
        histogram.add(longsBlock, pos++, 1);
        histogram.add(longsBlock, pos++, 1);
        histogram.add(longsBlock, pos++, 1);
        histogram.add(longsBlock, pos++, 1);
        MapType mapType = mapType(BIGINT, BIGINT);
        BlockBuilder blockBuilder = mapType.createBlockBuilder(null, 10);
        histogram.topK(blockBuilder);
        Block object = mapType.getObject(blockBuilder, 0);
        Map<Long, Long> buckets = getMapFromLongMapBucket(object);
        assertEquals(buckets.size(), 3);
        assertEquals(buckets, ImmutableMap.of(1L, 2L, 2L, 1L, 3L, 1L));
    }

    public Map<Long, Long> getMapFromLongMapBucket(Block block)
    {
        ImmutableMap.Builder<Long, Long> buckets = new ImmutableMap.Builder<>();
        for (int pos = 0; pos < block.getPositionCount(); pos += 2) {
            buckets.put(block.getLong(pos), block.getLong(pos + 1));
        }
        return buckets.build();
    }

    @Test
    public void testLongRoundtrip()
    {
        ApproximateMostFrequentHistogram<Long> original = new ApproximateMostFrequentHistogram<Long>(3, 15, LongApproximateMostFrequentStateSerializer::serializeBucket, LongApproximateMostFrequentStateSerializer::deserializeBucket);

        original.add(1L);
        original.add(1L);
        original.add(2L);
        original.add(3L);
        original.add(4L);

        Slice serialized = original.serialize();

        ApproximateMostFrequentHistogram<Long> deserialized = new ApproximateMostFrequentHistogram<Long>(serialized, LongApproximateMostFrequentStateSerializer::serializeBucket, LongApproximateMostFrequentStateSerializer::deserializeBucket);

        assertEquals(deserialized.getBuckets(), original.getBuckets());
    }

    @Test
    public void testMerge()
    {
        ApproximateMostFrequentHistogram<Long> histogram1 = new ApproximateMostFrequentHistogram<Long>(3, 15, LongApproximateMostFrequentStateSerializer::serializeBucket, LongApproximateMostFrequentStateSerializer::deserializeBucket);

        histogram1.add(1L);
        histogram1.add(1L);
        histogram1.add(2L);

        ApproximateMostFrequentHistogram<Long> histogram2 = new ApproximateMostFrequentHistogram<Long>(3, 15, LongApproximateMostFrequentStateSerializer::serializeBucket, LongApproximateMostFrequentStateSerializer::deserializeBucket);
        histogram2.add(3L);
        histogram2.add(4L);

        histogram1.merge(histogram2);
        Map<Long, Long> buckets = histogram1.getBuckets();

        assertEquals(buckets.size(), 3);
        assertEquals(buckets, ImmutableMap.of(1L, 2L, 2L, 1L, 3L, 1L));
    }

    @Test
    public void testStringHistogram()
    {
        ApproximateMostFrequentHistogram<Slice> histogram = new ApproximateMostFrequentHistogram<Slice>(3, 15, StringApproximateMostFrequentStateSerializer::serializeBucket, StringApproximateMostFrequentStateSerializer::deserializeBucket);

        histogram.add(Slices.utf8Slice("A"));
        histogram.add(Slices.utf8Slice("A"));
        histogram.add(Slices.utf8Slice("B"));
        histogram.add(Slices.utf8Slice("C"));
        histogram.add(Slices.utf8Slice("D"));

        Map<Slice, Long> buckets = histogram.getBuckets();

        assertEquals(buckets.size(), 3);
        assertEquals(buckets, ImmutableMap.of(Slices.utf8Slice("A"), 2L, Slices.utf8Slice("B"), 1L, Slices.utf8Slice("C"), 1L));
    }

    @Test
    public void testStringRoundtrip()
    {
        ApproximateMostFrequentHistogram<Slice> original = new ApproximateMostFrequentHistogram<Slice>(3, 15, StringApproximateMostFrequentStateSerializer::serializeBucket, StringApproximateMostFrequentStateSerializer::deserializeBucket);

        original.add(Slices.utf8Slice("A"));
        original.add(Slices.utf8Slice("A"));
        original.add(Slices.utf8Slice("B"));
        original.add(Slices.utf8Slice("C"));
        original.add(Slices.utf8Slice("D"));

        Slice serialized = original.serialize();

        ApproximateMostFrequentHistogram<Slice> deserialized = new ApproximateMostFrequentHistogram<Slice>(serialized, StringApproximateMostFrequentStateSerializer::serializeBucket, StringApproximateMostFrequentStateSerializer::deserializeBucket);

        assertEquals(deserialized.getBuckets(), original.getBuckets());
    }
}
