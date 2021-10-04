package com.facebook.presto.operator.aggregation.approxmostfrequent;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.aggregation.approxmostfrequent.exp.StreamSummary;
import com.facebook.presto.operator.aggregation.state.LongApproximateMostFrequentStateSerializer;
import com.facebook.presto.operator.aggregation.state.StringApproximateMostFrequentStateSerializer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.util.StructuralTestUtil.mapType;
import static org.testng.Assert.assertEquals;

public class TestStreamSummary
{
    private static final Type INT_SERIALIZED_TYPE = RowType.withDefaultFieldNames(ImmutableList.of(BIGINT, BIGINT, BIGINT, new ArrayType(BIGINT), new ArrayType(BIGINT)));

    @Test
    public void testLongHistogram()
    {
        Block longsBlock = createLongsBlock(1L, 1L, 2L, 3L, 4L);
        StreamSummary histogram = new StreamSummary(BIGINT, 3, 15, 10);
        int pos = 0;
        histogram.add(longsBlock, pos++, 1);
        histogram.add(longsBlock, pos++, 1);
        histogram.add(longsBlock, pos++, 1);
        histogram.add(longsBlock, pos++, 1);
        histogram.add(longsBlock, pos++, 1);
        Map<Long, Long> buckets = getMapForLongType(histogram);
        assertEquals(buckets.size(), 3);
        assertEquals(buckets, ImmutableMap.of(1L, 2L, 2L, 1L, 3L, 1L));
    }

    @Test
    public void testLongHistogramRehash()
    {
        //Long[] values = {2L, 1L, 1L, 2L};
        //Long[] values = {14L, 14L, 11L, 13L, 13L, 13L, 14L, 14L}; //->failed
        Long[] values = {1L, 1L, 2L, 3L, 4L, 5L, 5L, 6L, 6L, 7L, 7L, 8L, 9L, 10L, 11L, 6L, 7L, 7L, 8L, 9L, 10L, 10L, 8L, 9L, 10L, 11L, 6L, 7L, 7L, 8L, 9L, 8L, 9L, 10L, 11L, 6L, 7L,
                7L, 8L, 9L};
        Block longsBlock = createLongsBlock(values);
        int maxBuckets = 3;
        int heapCapacity = 10;
        StreamSummary streamSummary = new StreamSummary(BIGINT, maxBuckets, heapCapacity, 10);
        int pos = 0;
        for (int i = 0; i < values.length; i++) {
            streamSummary.add(longsBlock, pos++, 1);
        }

        Map<Long, Long> buckets = getMapForLongType(streamSummary);

        ApproximateMostFrequentHistogram<Long> histogram = new ApproximateMostFrequentHistogram<Long>(maxBuckets, heapCapacity, LongApproximateMostFrequentStateSerializer::serializeBucket, LongApproximateMostFrequentStateSerializer::deserializeBucket);
        for (Long value : values) {
            histogram.add(value);
        }

        Map<Long, Long> oldBuckets = histogram.getBuckets();

        assertEquals(buckets.size(), oldBuckets.size());
        assertEquals(buckets, oldBuckets);
    }

    @Test
    public void testDeserialize()
    {
        StreamSummary original = new StreamSummary(BIGINT, 3, 15, 10);
        Long[] values = {1L, 1L, 2L, 3L, 4L};
        int pos = 0;
        Block longsBlock = createLongsBlock(values);
        for (int i = 0; i < values.length; i++) {
            original.add(longsBlock, pos++, 1);
        }
        Map<Long, Long> originalMap = getMapForLongType(original);
        BlockBuilder blockBuilder = INT_SERIALIZED_TYPE.createBlockBuilder(null, 10);
        original.serialize(blockBuilder);

        StreamSummary deserialize = StreamSummary.deserialize(BIGINT, (Block) INT_SERIALIZED_TYPE.getObject(blockBuilder, 0));
        Map<Long, Long> deserializedMap = getMapForLongType(deserialize);
        assertEquals(originalMap, deserializedMap);
    }

    @Test
    public void testComparison()
    {
        for (int test = 0; test < 100; test++) {
            int totalValues = 729413;
            int maxBuckets = 3; //ThreadLocalRandom.current().nextInt(1, 10);
            int heapCapacity = 100; //ThreadLocalRandom.current().nextInt(10, 20);
            Long[] values = new Long[totalValues];
            for (int i = 0; i < totalValues; i++) {
                values[i] = Long.valueOf(ThreadLocalRandom.current().nextInt(1, 1000));
            }
            Block longsBlock = createLongsBlock(values);

            StreamSummary streamSummary = new StreamSummary(BIGINT, maxBuckets, heapCapacity, 10);
            int pos = 0;
            for (int i = 0; i < values.length; i++) {
                streamSummary.add(longsBlock, pos++, 1);
            }

            Map<Long, Long> buckets = getMapForLongType(streamSummary);

            ApproximateMostFrequentHistogram<Long> histogram = new ApproximateMostFrequentHistogram<Long>(maxBuckets, heapCapacity, LongApproximateMostFrequentStateSerializer::serializeBucket, LongApproximateMostFrequentStateSerializer::deserializeBucket);
            for (Long value : values) {
                histogram.add(value);
            }

            Map<Long, Long> oldBuckets = histogram.getBuckets();

            assertEquals(buckets.size(), oldBuckets.size());
            assertEquals(buckets, oldBuckets);
            System.out.println("totalValues =" + totalValues + "maxBuckets" + maxBuckets + "heapCapacity=" + heapCapacity);
            System.out.println("values = " + values);
        }
    }

    @Test
    public void testComparisonFailedCase()
    {
        int maxBuckets = 2; //ThreadLocalRandom.current().nextInt(1, 10);
        int heapCapacity = 100; //ThreadLocalRandom.current().nextInt(10, 20);

        Long[] values = {17200L, 18853L, 55624L, 55624L, 18853L, 55624L, 55624L, 18853L, 18853L, 18853L, 17200L, 18853L, 18853L, 55624L, 55624L, 55624L, 17200L, 55624L, 18853L,
                55624L, 17200L, 18853L, 18853L, 17200L, 17200L, 17200L, 18853L, 55624L, 17200L, 18853L, 17200L, 17200L, 17200L, 55624L, 18853L, 17200L, 17200L, 17200L, 55624L,
                17200L, 55624L, 55624L, 17200L, 17200L, 55624L, 18853L, 18853L, 17200L, 17200L, 17200L, 17200L, 55624L, 55624L, 18853L, 17200L, 18853L, 18853L, 55624L, 55624L,
                18853L, 18853L, 17200L, 55624L, 55624L, 55624L, 17200L, 18853L, 17200L, 17200L, 18853L};
        Block longsBlock = createLongsBlock(values);

        StreamSummary streamSummary = new StreamSummary(BIGINT, maxBuckets, heapCapacity, 10);
        int pos = 0;
        for (int i = 0; i < values.length; i++) {
            streamSummary.add(longsBlock, pos++, 1);
        }

        Map<Long, Long> buckets = getMapForLongType(streamSummary);

        ApproximateMostFrequentHistogram<Long> histogram = new ApproximateMostFrequentHistogram<Long>(maxBuckets, heapCapacity, LongApproximateMostFrequentStateSerializer::serializeBucket, LongApproximateMostFrequentStateSerializer::deserializeBucket);
        for (Long value : values) {
            histogram.add(value);
        }

        Map<Long, Long> oldBuckets = histogram.getBuckets();

        assertEquals(buckets.size(), oldBuckets.size());
        assertEquals(buckets, oldBuckets);
    }

    private Map<Long, Long> getMapForLongType(StreamSummary histogram)
    {
        MapType mapType = mapType(BIGINT, BIGINT);
        BlockBuilder blockBuilder = mapType.createBlockBuilder(null, 10);
        histogram.topK(blockBuilder);
        Block object = mapType.getObject(blockBuilder, 0);
        Map<Long, Long> buckets = getMapFromLongMapBucket(object);
        return buckets;
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
        Block longsBlock = createLongsBlock(1L, 1L, 2L, 3L, 4L);
        StreamSummary original = new StreamSummary(BIGINT, 3, 15, 10);
        int pos = 0;
        original.add(longsBlock, pos++, 1);
        original.add(longsBlock, pos++, 1);
        original.add(longsBlock, pos++, 1);
        original.add(longsBlock, pos++, 1);
        original.add(longsBlock, pos++, 1);

        BlockBuilder blockBuilder = INT_SERIALIZED_TYPE.createBlockBuilder(null, 10);
        original.serialize(blockBuilder);

        StreamSummary deserialize = StreamSummary.deserialize(BIGINT, (Block) INT_SERIALIZED_TYPE.getObject(blockBuilder, 0));
        assertEquals(getMapForLongType(original), getMapForLongType(deserialize));
    }

    @Test
    public void testMerge()
    {
        StreamSummary histogram1 = new StreamSummary(BIGINT, 3, 15, 10);
        Long[] values = {1L, 1L, 2L};
        int pos = 0;
        Block longsBlock = createLongsBlock(values);
        for (int i = 0; i < values.length; i++) {
            histogram1.add(longsBlock, pos++, 1);
        }

        StreamSummary histogram2 = new StreamSummary(BIGINT, 3, 15, 10);
        values = new Long[] {3L, 4L};
        pos = 0;
        longsBlock = createLongsBlock(values);
        for (int i = 0; i < values.length; i++) {
            histogram2.add(longsBlock, pos++, 1);
        }

        histogram1.merge(histogram2);
        Map<Long, Long> buckets = getMapForLongType(histogram1);

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
