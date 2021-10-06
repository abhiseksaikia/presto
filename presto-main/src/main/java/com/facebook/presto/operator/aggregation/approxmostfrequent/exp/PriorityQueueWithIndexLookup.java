package com.facebook.presto.operator.aggregation.approxmostfrequent.exp;

import com.google.common.collect.ImmutableList;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static io.airlift.slice.SizeOf.sizeOf;

public class PriorityQueueWithIndexLookup<T>
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(StreamSummary.class).instanceSize();
    public static final int MIN_POSITION = 0;
    private final T[] minHeap;
    private final int heapCapacity;
    private final HeapDataChangeListener heapDataChangeListener;
    private Comparator<T> heapDataComparator;
    private int positionCount;

    public PriorityQueueWithIndexLookup(int heapCapacity, Comparator<T> heapDataComparator, HeapDataChangeListener heapDataChangeListener)
    {
        this.minHeap = (T[]) new Object[heapCapacity];
        this.heapCapacity = heapCapacity;
        this.heapDataChangeListener = heapDataChangeListener;
        this.heapDataComparator = heapDataComparator;
    }

    public boolean isFull()
    {
        return this.positionCount == heapCapacity;
    }

    public void add(T data)
    {
        minHeap[positionCount++] = data;
        percolateUp(positionCount - 1);
    }

    public void replaceMin(T data)
    {
        minHeap[MIN_POSITION] = data;
        percolateDown(MIN_POSITION);
    }

    public T get(int index)
    {
        return minHeap[index];
    }

    public T getMin()
    {
        return minHeap[MIN_POSITION];
    }

    public void percolateDown(int position)
    {
        int leftPosition;
        while ((leftPosition = position * 2 + 1) < positionCount) {
            int rightPosition = leftPosition + 1;
            int smallerChildPosition;
            if (rightPosition >= positionCount) {
                smallerChildPosition = leftPosition;
            }
            else {
                smallerChildPosition = heapDataComparator.compare(minHeap[leftPosition], minHeap[rightPosition]) >= 0 ? rightPosition : leftPosition;
            }
            if (heapDataComparator.compare(minHeap[smallerChildPosition], minHeap[position]) >= 0) {
                break; // child is larger or equal
            }
            swap(position, smallerChildPosition);

            position = smallerChildPosition;
        }
        heapDataChangeListener.positionChanged(minHeap[position], position);
    }

    private void swap(int position, int smallerChildPosition)
    {
        T swapTemp = minHeap[position];
        minHeap[position] = minHeap[smallerChildPosition];
        minHeap[smallerChildPosition] = swapTemp;
        heapDataChangeListener.positionChanged(minHeap[smallerChildPosition], smallerChildPosition);
        heapDataChangeListener.positionChanged(minHeap[position], position);
    }

    public void percolateUp(int position)
    {
        while (position != 0) {
            int parentPosition = (position - 1) / 2;
            if (heapDataComparator.compare(minHeap[position], minHeap[parentPosition]) >= 0) {
                break; // child is larger or equal
            }
            swap(position, parentPosition);
            position = parentPosition;
        }
        heapDataChangeListener.positionChanged(minHeap[position], position);
    }

    public int getSize()
    {
        return positionCount;
    }

    public List<T> topK(int k, Comparator<T> comparator)
    {
        ImmutableList.Builder<T> builder = ImmutableList.builder();
        T[] topKData = Arrays.copyOf(minHeap, positionCount);
        Arrays.sort(topKData, comparator); // not using Collections.reverse to avoid creating more objects
        for (int pos = 0; pos < Math.min(k, positionCount); pos++) {
            builder.add(topKData[pos]);
        }
        return builder.build();
    }

    public long estimatedInMemorySize()
    {
        return INSTANCE_SIZE + sizeOf(minHeap);
    }
}
