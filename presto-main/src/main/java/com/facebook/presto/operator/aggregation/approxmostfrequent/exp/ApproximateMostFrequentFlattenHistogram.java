package com.facebook.presto.operator.aggregation.approxmostfrequent.exp;

import com.facebook.presto.array.IntBigArray;
import com.facebook.presto.array.LongBigArray;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.type.TypeUtils;
import com.google.common.annotations.VisibleForTesting;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static it.unimi.dsi.fastutil.HashCommon.murmurHash3;
import static java.util.Objects.requireNonNull;

public class ApproximateMostFrequentFlattenHistogram
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ApproximateMostFrequentFlattenHistogram.class).instanceSize();
    private static final float FILL_RATIO = 0.75f;
    private final int maxBuckets;
    private final int capacity;
    private final LongBigArray counts;
    private final int mask;
    private IntBigArray hashPositions;
    private final BlockBuilder values;
    private final Type type;
    int newElementCount;

    public ApproximateMostFrequentFlattenHistogram(
            Type type,
            int maxBuckets,
            int capacity,
            int expectedSize)
    {
        this.type = requireNonNull(type, "type is null");
        this.maxBuckets = maxBuckets;
        this.capacity = capacity;
        this.values = type.createBlockBuilder(null, arraySize(expectedSize, FILL_RATIO));
        this.hashPositions = new IntBigArray(-1);

        int hashCapacity = arraySize(expectedSize, FILL_RATIO);

        this.counts = new LongBigArray();
        this.counts.ensureCapacity(capacity);

        this.hashPositions.ensureCapacity(hashCapacity);
        this.mask = hashCapacity - 1;
    }

    public void add(int position, Block block, long count)
    {
        int hashPosition = getBucketId(TypeUtils.hashPosition(type, block, position), mask);

        // look for empty slot or slot containing this key
        while (true) {
            if (hashPositions.get(hashPosition) == -1) {
                break;
            }

            if (type.equalTo(block, position, values, hashPositions.get(hashPosition))) {
                counts.add(hashPositions.get(hashPosition), count);
                return;
            }

            // increment position and mask to handle wrap around
            hashPosition = (hashPosition + 1) & mask;
        }
        newElementCount++;
        addNewGroup(hashPosition, position, block, count);
    }

    private void addNewGroup(int hashPosition, int position, Block block, long count)
    {
        if (this.newElementCount == this.capacity) {
            long minCount = Long.MAX_VALUE;
            int minPos = -1;
            for (int pos = 0; pos < values.getPositionCount(); pos++) {
                long currentCount = counts.get(pos);
                if (currentCount < minCount) {
                    minCount = currentCount;
                    minPos = pos;
                }
            }
            //now lets place the block in minPos and take its count+1
            //delete min
            counts.set(minPos, 0);
            newElementCount--;
            hashPositions.set(hashPosition, values.getPositionCount());
            counts.set(values.getPositionCount(), minCount + count);
            //add block[position] into values (block builder), type will have different implementation based on String/ Int etc
            type.appendTo(block, position, values);
        }
        else {
            hashPositions.set(hashPosition, values.getPositionCount());
            counts.set(values.getPositionCount(), count);
            //add block[position] into values (block builder), type will have different implementation based on String/ Int etc
            type.appendTo(block, position, values);
        }
    }

    private static int getBucketId(long rawHash, int mask)
    {
        return ((int) murmurHash3(rawHash)) & mask;
    }

    public void serialize(BlockBuilder out)
    {
        BlockBuilder blockBuilder = out.beginBlockEntry();
        long[][] topKCounts = getTopKCountsWithBlockPosition();
        if (topKCounts.length > 0) {
            BIGINT.writeLong(blockBuilder, maxBuckets);
            BIGINT.writeLong(blockBuilder, capacity);
            BlockBuilder keyBuilder = blockBuilder.beginBlockEntry();
            for (long[] countPositionPair : topKCounts) {
                long itemPos = countPositionPair[1];
                type.appendTo(values, (int) itemPos, keyBuilder);
            }
            blockBuilder.closeEntry();

            BlockBuilder valueBuilder = blockBuilder.beginBlockEntry();
            for (long[] countPositionPair : topKCounts) {
                long count = countPositionPair[0];
                BIGINT.writeLong(valueBuilder, count);
            }
            blockBuilder.closeEntry();
        }
        out.closeEntry();
    }

    public void writeMapDataTo(BlockBuilder out)
    {
        BlockBuilder blockBuilder = out.beginBlockEntry();
        writeMapData(blockBuilder);
        out.closeEntry();
    }

    @VisibleForTesting
    public void writeMapData(BlockBuilder blockBuilder)
    {
        long[][] topKCounts = getTopKCountsWithBlockPosition();
        // Serialize top k key and counts.
        for (long[] countPositionPair : topKCounts) {
            long count = countPositionPair[0];
            long itemPos = countPositionPair[1];
            type.appendTo(values, (int) itemPos, blockBuilder);
            BIGINT.writeLong(blockBuilder, count);
        }
    }

    public long[][] getTopKCountsWithBlockPosition()
    {
        long[][] sortedCounters = new long[newElementCount][];
        int counters = 0;
        for (int pos = 0; pos < values.getPositionCount(); pos++) {
            long currentCount = counts.get(pos);
            if (currentCount == 0) {
                continue;
            }
            sortedCounters[counters++] = new long[] {currentCount, pos};
        }
        Arrays.sort(sortedCounters, (a, b) -> {
            int compare = Long.compare(b[0], a[0]);
            if (compare == 0) {
                return Long.compare(a[1], b[1]);
            }
            return compare;
        });
        long[][] topKCounts = Arrays.copyOfRange(sortedCounters, 0, Math.min(maxBuckets, newElementCount));
        return topKCounts;
    }

    public void merge(ApproximateMostFrequentFlattenHistogram otherHistogram)
    {
        otherHistogram.readAllValues((itemPos, block, count) -> add(itemPos, block, count));
    }

    public void readAllValues(ApproximateHistogramReader reader)
    {
        long[][] topKCountsWithBlockPosition = getTopKCountsWithBlockPosition();
        for (long[] countPositionPair : topKCountsWithBlockPosition) {
            long count = countPositionPair[0];
            long itemPos = countPositionPair[1];
            reader.read((int) itemPos, values, count);
        }
    }

    public long estimatedInMemorySize()
    {
        return INSTANCE_SIZE +
                values.getRetainedSizeInBytes() +
                counts.sizeOf() +
                hashPositions.sizeOf();
    }
}
