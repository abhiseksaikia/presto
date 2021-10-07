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

import com.facebook.presto.array.IntBigArray;
import com.facebook.presto.array.LongBigArray;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.type.TypeUtils;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static com.google.common.base.Preconditions.checkArgument;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static it.unimi.dsi.fastutil.HashCommon.murmurHash3;
import static java.lang.Math.toIntExact;

public class StreamSummary
        implements HeapDataChangeListener<HeapEntry>
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(StreamSummary.class).instanceSize();
    private static final int COMPACT_THRESHOLD_BYTES = 32768; //32768; // using 100 for test to reach early 32768;
    private static final float FILL_RATIO = 0.75f;
    //1 for testing, use 3
    private static final int COMPACT_THRESHOLD_RATIO = 3; // when 2/3 of elements in heapBlockBuilder is unreferenced, do compact
    private static final int EMPTY_SLOT = -1;
    private static final int DELETE_MARKER = -2;
    private final Type type;
    private final int heapCapacity;
    private final int maxBuckets;
    private int maxFill;
    int insertOrder;

    /**
     * mapping between block's position and its count
     **/
    private LongBigArray blockPositionToCount;
    private IntBigArray hashToBlockPosition; // Map<value hash, value's position>
    private int hashCapacity;

    /**
     * min heap - values and indexes
     **/
    private BlockBuilder heapBlockBuilder;
    private final PriorityQueueWithIndexLookup<HeapEntry> minHeap;
    private IntBigArray blockToHeapIndex; // Map<keyhash, index pos>

    private int mask;

    public StreamSummary(
            Type type,
            int maxBuckets,
            int heapCapacity) //TODO figure out what drives hashCapacity, can we use heapCapacity instead
    {
        this.type = type;
        this.maxBuckets = maxBuckets;
        this.heapCapacity = heapCapacity;
        this.blockPositionToCount = new LongBigArray();
        this.blockToHeapIndex = new IntBigArray();
        this.hashToBlockPosition = new IntBigArray(EMPTY_SLOT);
        this.hashCapacity = arraySize(heapCapacity, FILL_RATIO);
        this.hashToBlockPosition.ensureCapacity(hashCapacity);
        this.heapBlockBuilder = type.createBlockBuilder(null, heapCapacity);
        this.minHeap = new PriorityQueueWithIndexLookup<>(heapCapacity, (heapEntry1, heapEntry2) -> compare(heapEntry1, heapEntry2), this);
        this.mask = hashCapacity - 1;
        this.maxFill = calculateMaxFill(hashCapacity);
        this.blockPositionToCount.ensureCapacity(hashCapacity);
        this.blockToHeapIndex.ensureCapacity(hashCapacity);
    }

    public void add(Block block, int blockPosition, long incrementCount)
    {
        int hashPosition = getBucketId(TypeUtils.hashPosition(type, block, blockPosition), mask);
        // look for empty slot or slot containing this key
        while (true) {
            int bucketPosition = hashToBlockPosition.get(hashPosition);
            if (bucketPosition == EMPTY_SLOT) {
                break;
            }
            if (bucketPosition != DELETE_MARKER && type.equalTo(block, blockPosition, heapBlockBuilder, bucketPosition)) {
                blockPositionToCount.add(bucketPosition, incrementCount);
                int heapIndex = blockToHeapIndex.get(bucketPosition);
                minHeap.get(heapIndex).setGeneration(insertOrder++);
                minHeap.percolateDown(heapIndex);
                return;
            }

            // increment position and mask to handle wrap around
            hashPosition = (hashPosition + 1) & mask;
        }

        addNewGroup(block, blockPosition, hashPosition, incrementCount);
    }

    private void addNewGroup(Block block, int blockPosition, int hashPosition, long incrementCount)
    {
        int newElementBlockPosition = heapBlockBuilder.getPositionCount();
        if (minHeap.isFull()) {
            //replace min
            HeapEntry min = minHeap.getMin();
            int removedBlock = getBlockPosition(min);
            long minCount = blockPositionToCount.get(removedBlock);
            handleDelete(removedBlock, min.getHashPosition());

            hashToBlockPosition.set(hashPosition, newElementBlockPosition);
            blockPositionToCount.set(newElementBlockPosition, minCount + incrementCount);
            minHeap.replaceMin(new HeapEntry(hashPosition, insertOrder++));

            type.appendTo(block, blockPosition, heapBlockBuilder);
        }
        else {
            hashToBlockPosition.set(hashPosition, newElementBlockPosition);
            blockPositionToCount.set(newElementBlockPosition, incrementCount);
            minHeap.add(new HeapEntry(hashPosition, insertOrder++));
            type.appendTo(block, blockPosition, heapBlockBuilder);
        }
        compactAndRehashIfNeeded();
    }

    private void handleDelete(int removedBlock, int removedHashPosition)
    {
        blockPositionToCount.set(removedBlock, 0);
        blockToHeapIndex.set(removedBlock, EMPTY_SLOT);
        hashToBlockPosition.set(removedHashPosition, DELETE_MARKER);
    }

    private void compactAndRehashIfNeeded()
    {
        if (heapBlockBuilder.getSizeInBytes() >= COMPACT_THRESHOLD_BYTES && heapBlockBuilder.getPositionCount() / minHeap.getSize() >= COMPACT_THRESHOLD_RATIO) {
            compact();
        }
        else {
            //since we do rehash everytime as well as recheck counts when we do compaction, whenever we don't need to do compaction, check if rehash is required
            if (heapBlockBuilder.getPositionCount() >= maxFill) {
                rehash();
            }
        }
    }

    private synchronized void compact()
    {
        //we have to rehash the map
        BlockBuilder newHeapBlockBuilder = type.createBlockBuilder(null, heapBlockBuilder.getPositionCount());
        //since block positions are changed, we need to update all data structures which are using blcok position as reference
        LongBigArray newBlockPositionToCount = new LongBigArray();
        hashCapacity = arraySize(heapCapacity, FILL_RATIO);
        maxFill = calculateMaxFill(hashCapacity);
        newBlockPositionToCount.ensureCapacity(hashCapacity);
        IntBigArray newBlockToHeapIndex = new IntBigArray();
        newBlockToHeapIndex.ensureCapacity(hashCapacity);

        for (int heapPosition = 0; heapPosition < minHeap.getSize(); heapPosition++) {
            //append data from heapIndex[heapPosition][HEAP_BLOCK_POS] to newHeapBlockBuilder
            int newBlockPos = newHeapBlockBuilder.getPositionCount();
            HeapEntry heapEntry = minHeap.get(heapPosition);
            int oldBlockPosition = getBlockPosition(heapEntry);
            //insert positon in the blocks could be 1-->120--->100, we need to start insert pos
            type.appendTo(heapBlockBuilder, oldBlockPosition, newHeapBlockBuilder);
            //minHeap[heapPosition][HEAP_BLOCK_POS_INDEX] = newBlockPos;

            //how to compact the insert position?
            newBlockPositionToCount.set(newBlockPos, blockPositionToCount.get(oldBlockPosition));
            newBlockToHeapIndex.set(newBlockPos, heapPosition);
            hashToBlockPosition.set(heapEntry.getHashPosition(), newBlockPos);
        }
        blockPositionToCount = newBlockPositionToCount;
        heapBlockBuilder = newHeapBlockBuilder;
        blockToHeapIndex = newBlockToHeapIndex;
        rehash();
    }

    private void rehash()
    {
        long newCapacityLong = hashCapacity * 2L;
        if (newCapacityLong > Integer.MAX_VALUE) {
            throw new PrestoException(GENERIC_INSUFFICIENT_RESOURCES, "Size of hash table cannot exceed 1 billion entries");
        }
        int newCapacity = (int) newCapacityLong;
        int newMask = newCapacity - 1;
        IntBigArray newHashToBlockPosition = new IntBigArray(EMPTY_SLOT);
        newHashToBlockPosition.ensureCapacity(newCapacity);

        for (int heapPosition = 0; heapPosition < minHeap.getSize(); heapPosition++) {
            HeapEntry heapEntry = minHeap.get(heapPosition);
            int blockPosition = getBlockPosition(heapEntry);
            // find an empty slot for the address
            int hashPosition = getBucketId(TypeUtils.hashPosition(type, heapBlockBuilder, blockPosition), newMask);

            while (newHashToBlockPosition.get(hashPosition) != EMPTY_SLOT) {
                hashPosition = (hashPosition + 1) & newMask;
            }

            // record the mapping
            newHashToBlockPosition.set(hashPosition, blockPosition);
            heapEntry.setHashPosition(hashPosition);
        }
        hashCapacity = newCapacity;
        mask = newMask;
        maxFill = calculateMaxFill(newCapacity);
        this.hashToBlockPosition = newHashToBlockPosition;
        this.blockPositionToCount.ensureCapacity(maxFill);
        this.blockToHeapIndex.ensureCapacity(maxFill);
    }

    private int compare(HeapEntry heapValue1, HeapEntry heapValue2)
    {
        int compare = Long.compare(
                getCount(heapValue1),
                getCount(heapValue2));
        if (compare == 0) {
            compare = Long.compare(heapValue1.getGeneration(), heapValue2.getGeneration());
        }
        return compare;
    }

    private long getCount(HeapEntry heapEntry)
    {
        return blockPositionToCount.get(getBlockPosition(heapEntry));
    }

    private int getBlockPosition(HeapEntry heapEntry)
    {
        return hashToBlockPosition.get(heapEntry.getHashPosition());
    }

    private static int getBucketId(long rawHash, int mask)
    {
        return ((int) murmurHash3(rawHash)) & mask;
    }

    public void topK(BlockBuilder out)
    {
        //get top k heap entries
        List<HeapEntry> sortedHeapEntries = getTopHeapEntries();
        //write data and count to output
        BlockBuilder valueBuilder = out.beginBlockEntry();
        for (HeapEntry heapEntry : sortedHeapEntries) {
            type.appendTo(heapBlockBuilder, getBlockPosition(heapEntry), valueBuilder);
            BIGINT.writeLong(valueBuilder, getCount(heapEntry));
        }
        out.closeEntry();
    }

    private List<HeapEntry> getTopHeapEntries()
    {
        List<HeapEntry> sortedHeapEntries = minHeap.topK(maxBuckets, (a, b) -> {
            int compare = Long.compare(
                    getCount(b),
                    getCount(a));
            if (compare == 0) {
                return Integer.compare(a.getGeneration(), b.getGeneration());
            }
            return compare;
        });
        return sortedHeapEntries;
    }

    public void merge(StreamSummary otherStreamSummary)
    {
        otherStreamSummary.readAllValues((block, itemPos, count) -> add(block, itemPos, count));
    }

    public void readAllValues(StreamSummaryReader reader)
    {
        List<HeapEntry> heapEntries = getTopHeapEntries();
        for (HeapEntry heapEntry : heapEntries) {
            reader.read(heapBlockBuilder, getBlockPosition(heapEntry), getCount(heapEntry));
        }
    }

    public void serialize(BlockBuilder out)
    {
        BlockBuilder blockBuilder = out.beginBlockEntry();
        if (minHeap.getSize() > 0) {
            BIGINT.writeLong(blockBuilder, maxBuckets);
            BIGINT.writeLong(blockBuilder, heapCapacity);
            //this resolves the issue with select approx_most_frequent_improved(2,custkey,100) from tpch.sf1.orders where  custkey in (55624,17200,18853) to return save value as the algo returns int[][] copyOfHeap = sortHeapByInsertionPosition();
            List<HeapEntry> sortedHeap = getTopHeapEntries();
            int topKPosition = Math.min(maxBuckets, minHeap.getSize());
            BlockBuilder keyItems = blockBuilder.beginBlockEntry();
            for (HeapEntry heapEntry : sortedHeap) {
                type.appendTo(heapBlockBuilder, getBlockPosition(heapEntry), keyItems);
            }
            blockBuilder.closeEntry();

            BlockBuilder valueItems = blockBuilder.beginBlockEntry();
            for (HeapEntry heapEntry : sortedHeap) {
                BIGINT.writeLong(valueItems, getCount(heapEntry));
            }
            blockBuilder.closeEntry();
        }
        out.closeEntry();
    }

    public static StreamSummary deserialize(Type type, Block block)
    {
        int currentPosition = 0;
        int maxBuckets = toIntExact(BIGINT.getLong(block, currentPosition++));
        int heapCapacity = toIntExact(BIGINT.getLong(block, currentPosition++));

        StreamSummary streamSummary = new StreamSummary(type, maxBuckets, heapCapacity);
        Block keysBlock = new ArrayType(type).getObject(block, currentPosition++);
        Block valuesBlock = new ArrayType(BIGINT).getObject(block, currentPosition++);

        for (int i = 0; i < keysBlock.getPositionCount(); i++) {
            streamSummary.add(keysBlock, i, valuesBlock.getLong(i));
        }
        return streamSummary;
    }

    public long estimatedInMemorySize()
    {
        return INSTANCE_SIZE + heapBlockBuilder.getRetainedSizeInBytes() + minHeap.estimatedInMemorySize() + blockPositionToCount.sizeOf() +
                hashToBlockPosition.sizeOf();
    }

    private static int calculateMaxFill(int hashSize)
    {
        checkArgument(hashSize > 0, "hashSize must be greater than 0");
        int maxFill = (int) Math.ceil(hashSize * FILL_RATIO);
        if (maxFill == hashSize) {
            maxFill--;
        }
        checkArgument(hashSize > maxFill, "hashSize must be larger than maxFill");
        return maxFill;
    }

    @Override
    public void positionChanged(HeapEntry heapEntry, int index)
    {
        blockToHeapIndex.set(getBlockPosition(heapEntry), index);
    }
}
