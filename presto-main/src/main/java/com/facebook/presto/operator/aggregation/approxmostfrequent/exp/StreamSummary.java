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

import java.util.Arrays;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.sizeOf;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static it.unimi.dsi.fastutil.HashCommon.murmurHash3;
import static java.lang.Math.toIntExact;

public class StreamSummary
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(StreamSummary.class).instanceSize();
    private static final int COMPACT_THRESHOLD_BYTES = 100; //32768; // using 100 for test to reach early 32768;
    private static final float FILL_RATIO = 0.75f;
    //1 for testing, use 3
    private static final int COMPACT_THRESHOLD_RATIO = 1; // when 2/3 of elements in heapBlockBuilder is unreferenced, do compact
    public static final int DELETE_MARKER = -1;
    private final Type type;
    private final int heapCapacity;
    private final int maxBuckets;
    private final int expectedSizeInHash;
    private int maxFill;
    int lastMaxInsertPos;
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
    private static final int HEAP_BLOCK_POS = 0;
    private static final int HEAP_HASH_POS = 1;
    private static final int HEAP_BLOCK_INSERTION_POS = 2;
    //in heap index, we need to maintain the last insertion
    private final int[][] heapIndexes; // store hash index
    private IntBigArray blockToHeapIndex; // Map<keyhash, index pos>

    private int mask;
    /*private static final int HEAP_BLOCK_POS = 0;
    private static final int HEAP_BLOCK_HASH_POS = 1;*/
    //maintain last position in the heap
    private int positionCount;

    public StreamSummary(
            Type type,
            int maxBuckets,
            int heapCapacity,
            int expectedSizeInHash) //TODO figure out what drives hashCapacity, can we use heapCapacity instead
    {
        this.type = type;
        this.maxBuckets = maxBuckets;
        this.heapCapacity = heapCapacity;
        this.expectedSizeInHash = expectedSizeInHash;
        this.blockPositionToCount = new LongBigArray();
        this.blockToHeapIndex = new IntBigArray();
        this.hashToBlockPosition = new IntBigArray(-1);
        this.hashCapacity = arraySize(heapCapacity, FILL_RATIO);
        this.hashToBlockPosition.ensureCapacity(hashCapacity);
        this.heapBlockBuilder = type.createBlockBuilder(null, heapCapacity);
        this.heapIndexes = new int[heapCapacity][3];
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
            if (bucketPosition == -1) {
                break;
            }
            if (type.equalTo(block, blockPosition, heapBlockBuilder, bucketPosition)) {
                blockPositionToCount.add(bucketPosition, incrementCount);
                int heapIndex = blockToHeapIndex.get(hashToBlockPosition.get(hashPosition));
                heapIndexes[heapIndex][HEAP_BLOCK_INSERTION_POS] = lastMaxInsertPos + (insertOrder++);
                percolateDown(heapIndex);
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
        if (this.positionCount == heapCapacity) {
            //replace min
            int removedBlock = heapIndexes[0][HEAP_BLOCK_POS];
            int removedHashPosition = heapIndexes[0][HEAP_HASH_POS];
            long minCount = blockPositionToCount.get(removedBlock);
            handleDelete(removedBlock, removedHashPosition);
            // lets skip this for now as during compaction we rebuild from the heap hashToBlockPosition.set(removedBlockHashPos, DELETE_MARKER);

            hashToBlockPosition.set(hashPosition, newElementBlockPosition);
            blockPositionToCount.set(newElementBlockPosition, minCount + incrementCount);
            insertIntoHeap(0, hashPosition, newElementBlockPosition);
            percolateDown(0);
            type.appendTo(block, blockPosition, heapBlockBuilder);
        }
        else {
            hashToBlockPosition.set(hashPosition, newElementBlockPosition);
            blockPositionToCount.set(newElementBlockPosition, incrementCount);
            insertIntoHeap(positionCount, hashPosition, newElementBlockPosition);
            positionCount++;
            type.appendTo(block, blockPosition, heapBlockBuilder);
            percolateUp(positionCount - 1);
        }
        compactAndRehashIfNeeded();
    }

    private void handleDelete(int removedBlock, int removedHashPosition)
    {
        blockPositionToCount.set(removedBlock, 0);
        hashToBlockPosition.set(removedHashPosition, DELETE_MARKER);
        blockToHeapIndex.set(removedBlock, DELETE_MARKER);
    }

    private void insertIntoHeap(int heapIndexPosition, int hashPosition, int blockPosition)
    {
        heapIndexes[heapIndexPosition][HEAP_BLOCK_POS] = blockPosition;
        heapIndexes[heapIndexPosition][HEAP_BLOCK_INSERTION_POS] = lastMaxInsertPos + (insertOrder++);
        heapIndexes[heapIndexPosition][HEAP_HASH_POS] = hashPosition;
        blockToHeapIndex.set(blockPosition, heapIndexPosition);
    }

    private void compactAndRehashIfNeeded()
    {
        if (heapBlockBuilder.getSizeInBytes() < COMPACT_THRESHOLD_BYTES || heapBlockBuilder.getPositionCount() / positionCount < COMPACT_THRESHOLD_RATIO) {
            //since we do rehash everytime as well as recheck counts when we do compaction, whenever we don't need to do compaction, check if rehash is required
            if (heapBlockBuilder.getPositionCount() >= maxFill) {
                refillCounts();
            }
            return;
        }
        //we have to rehash the map
        BlockBuilder newHeapBlockBuilder = type.createBlockBuilder(null, heapBlockBuilder.getPositionCount());
        //since block positions are changed, we need to update all data structures which are using blcok position as reference
        LongBigArray newBlockPositionToCount = new LongBigArray();
        hashCapacity = arraySize(heapCapacity, FILL_RATIO);
        maxFill = calculateMaxFill(hashCapacity);
        newBlockPositionToCount.ensureCapacity(hashCapacity);
        blockToHeapIndex = new IntBigArray();
        blockToHeapIndex.ensureCapacity(hashCapacity);

        for (int heapPosition = 0; heapPosition < positionCount; heapPosition++) {
            //append data from heapIndex[heapPosition][HEAP_BLOCK_POS] to newHeapBlockBuilder
            int newBlockPos = newHeapBlockBuilder.getPositionCount();
            int oldBlockPosition = heapIndexes[heapPosition][HEAP_BLOCK_POS];
            //insert positon in the blocks could be 1-->120--->100, we need to start insert pos
            lastMaxInsertPos = Math.max(lastMaxInsertPos, heapIndexes[heapPosition][HEAP_BLOCK_INSERTION_POS]);
            type.appendTo(heapBlockBuilder, oldBlockPosition, newHeapBlockBuilder);
            heapIndexes[heapPosition][HEAP_BLOCK_POS] = newBlockPos;

            //how to compact the insert position?
            newBlockPositionToCount.set(newBlockPos, blockPositionToCount.get(oldBlockPosition));
            blockToHeapIndex.set(newBlockPos, heapPosition);
        }
        this.blockPositionToCount = newBlockPositionToCount;
        heapBlockBuilder = newHeapBlockBuilder;
        rehash();
    }

    private void refillCounts()
    {
        long newCapacityLong = hashCapacity * 2L;
        if (newCapacityLong > Integer.MAX_VALUE) {
            throw new PrestoException(GENERIC_INSUFFICIENT_RESOURCES, "Size of hash table cannot exceed 1 billion entries");
        }
        int newCapacity = (int) newCapacityLong;
        hashCapacity = newCapacity;
        maxFill = calculateMaxFill(newCapacity);
        this.blockPositionToCount.ensureCapacity(hashCapacity);
        this.blockToHeapIndex.ensureCapacity(hashCapacity);
    }

    private void percolateDown(int position)
    {
        while (true) {
            int leftPosition = position * 2 + 1;
            if (leftPosition >= positionCount) {
                break;
            }
            int rightPosition = leftPosition + 1;
            int smallerChildPosition;
            if (rightPosition >= positionCount) {
                smallerChildPosition = leftPosition;
            }
            else {
                smallerChildPosition = compare(heapIndexes[leftPosition], heapIndexes[rightPosition]) >= 0 ? rightPosition : leftPosition;
            }
            if (compare(heapIndexes[smallerChildPosition], heapIndexes[position]) >= 0) {
                break; // child is larger or equal
            }
            swap(position, smallerChildPosition);
            position = smallerChildPosition;
        }
    }

    private void swap(int position, int smallerChildPosition)
    {
        int[] swapTemp = heapIndexes[position];
        heapIndexes[position] = heapIndexes[smallerChildPosition];
        blockToHeapIndex.set(heapIndexes[position][HEAP_BLOCK_POS], position);
        heapIndexes[smallerChildPosition] = swapTemp;
        blockToHeapIndex.set(heapIndexes[smallerChildPosition][HEAP_BLOCK_POS], smallerChildPosition);
    }

    private void percolateUp(int position)
    {
        //int position = positionCount - 1;
        while (position != 0) {
            int parentPosition = (position - 1) / 2;
            if (compare(heapIndexes[position], heapIndexes[parentPosition]) >= 0) {
                break; // child is larger or equal
            }
            swap(position, parentPosition);
            position = parentPosition;
        }
    }

    private int compare(int[] heapValue1, int[] heapValue2)
    {
        int compare = Long.compare(blockPositionToCount.get(heapValue1[HEAP_BLOCK_POS]), blockPositionToCount.get(heapValue2[HEAP_BLOCK_POS]));
        if (compare == 0) {
            compare = Long.compare(heapValue1[HEAP_BLOCK_INSERTION_POS], heapValue2[HEAP_BLOCK_INSERTION_POS]);
        }
        return compare;
    }

    private static int getBucketId(long rawHash, int mask)
    {
        return ((int) murmurHash3(rawHash)) & mask;
    }

    public void topK(BlockBuilder out)
    {
        //sort the heapindexes based on count , if equal then sort based on position
        Arrays.sort(heapIndexes, 0, positionCount, (a, b) -> {
            int compare = Long.compare(blockPositionToCount.get(b[HEAP_BLOCK_POS]), blockPositionToCount.get(a[HEAP_BLOCK_POS]));
            if (compare == 0) {
                return Integer.compare(a[HEAP_BLOCK_INSERTION_POS], b[HEAP_BLOCK_INSERTION_POS]);
            }
            return compare;
        });
        //write data and count to output
        BlockBuilder valueBuilder = out.beginBlockEntry();
        int[][] topKHeapIndexData = Arrays.copyOfRange(heapIndexes, 0, Math.min(maxBuckets, positionCount));
        for (int[] heapData : topKHeapIndexData) {
            long count = blockPositionToCount.get(heapData[HEAP_BLOCK_POS]);
            type.appendTo(heapBlockBuilder, heapData[HEAP_BLOCK_POS], valueBuilder);
            BIGINT.writeLong(valueBuilder, count);
        }
        out.closeEntry();
    }

    public void merge(StreamSummary otherStreamSummary)
    {
        otherStreamSummary.readAllValues((block, itemPos, count) -> add(block, itemPos, count));
    }

    public void readAllValues(StreamSummaryReader reader)
    {
        for (int heapIndexPosition = 0; heapIndexPosition < positionCount; heapIndexPosition++) {
            long count = blockPositionToCount.get(heapIndexes[heapIndexPosition][HEAP_BLOCK_POS]);
            reader.read(heapBlockBuilder, heapIndexes[heapIndexPosition][HEAP_BLOCK_POS], count);
        }
    }

    public void serialize(BlockBuilder out)
    {
        BlockBuilder blockBuilder = out.beginBlockEntry();
        if (positionCount > 0) {
            BIGINT.writeLong(blockBuilder, maxBuckets);
            BIGINT.writeLong(blockBuilder, heapCapacity);
            BIGINT.writeLong(blockBuilder, expectedSizeInHash);

            BlockBuilder keyItems = blockBuilder.beginBlockEntry();
            for (int position = 0; position < positionCount; position++) {
                type.appendTo(heapBlockBuilder, heapIndexes[position][HEAP_BLOCK_POS], keyItems);
            }
            blockBuilder.closeEntry();

            BlockBuilder valueItems = blockBuilder.beginBlockEntry();
            for (int position = 0; position < positionCount; position++) {
                BIGINT.writeLong(valueItems, blockPositionToCount.get(heapIndexes[position][HEAP_BLOCK_POS]));
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
        int expectedHashSize = toIntExact(BIGINT.getLong(block, currentPosition++));

        StreamSummary streamSummary = new StreamSummary(type, maxBuckets, heapCapacity, expectedHashSize);
        Block keysBlock = new ArrayType(type).getObject(block, currentPosition++);
        Block valuesBlock = new ArrayType(BIGINT).getObject(block, currentPosition++);

        for (int i = 0; i < keysBlock.getPositionCount(); i++) {
            streamSummary.add(keysBlock, i, valuesBlock.getLong(i));
        }
        return streamSummary;
    }

    public long estimatedInMemorySize()
    {
        return INSTANCE_SIZE + heapBlockBuilder.getRetainedSizeInBytes() + sizeOf(heapIndexes) + blockPositionToCount.sizeOf() +
                hashToBlockPosition.sizeOf();
    }

    private void rehash()
    {
        IntBigArray newHashToBlockPositions = new IntBigArray(-1);
        newHashToBlockPositions.ensureCapacity(hashCapacity);
        //heapBlockBuilder does not have duplicates
        for (int blockPosition = 0; blockPosition < heapBlockBuilder.getPositionCount(); blockPosition++) {
            // find an empty slot for the address
            int hashPosition = getBucketId(TypeUtils.hashPosition(type, heapBlockBuilder, blockPosition), mask);

            while (newHashToBlockPositions.get(hashPosition) != -1) {
                hashPosition = (hashPosition + 1) & mask;
            }

            // record the mapping
            newHashToBlockPositions.set(hashPosition, blockPosition);
            //update hash in heap

            int heapIndexForBlock = blockToHeapIndex.get(blockPosition);
            heapIndexes[heapIndexForBlock][HEAP_HASH_POS] = hashPosition;
        }
        hashToBlockPosition = newHashToBlockPositions;
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
}
