package com.facebook.presto.operator.aggregation.approxmostfrequent.exp.pq;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class PriorityQueueWithIndexLookup<T>
{
    public static final int MIN_POSITION = 0;
    private final T[] minHeap;
    private final int heapCapacity;
    private final HeapDataChangeListener heapDataChangeListener;
    private Comparator<T> comparator;
    private int positionCount;

    public PriorityQueueWithIndexLookup(int heapCapacity, Comparator<T> comparator, HeapDataChangeListener heapDataChangeListener)
    {
        this.minHeap = (T[]) new Object[heapCapacity];
        this.heapCapacity = heapCapacity;
        this.heapDataChangeListener = heapDataChangeListener;
        this.comparator = comparator;
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
                smallerChildPosition = comparator.compare(minHeap[leftPosition], minHeap[rightPosition]) >= 0 ? rightPosition : leftPosition;
            }
            if (comparator.compare(minHeap[smallerChildPosition], minHeap[position]) >= 0) {
                break; // child is larger or equal
            }
            swap(position, smallerChildPosition);
            heapDataChangeListener.positionChanged(minHeap[position], position);
            position = smallerChildPosition;
        }
        heapDataChangeListener.positionChanged(minHeap[position], position);
    }

    private void swap(int position, int smallerChildPosition)
    {
        T swapTemp = minHeap[position];
        minHeap[position] = minHeap[smallerChildPosition];
        minHeap[smallerChildPosition] = swapTemp;
    }

    public void percolateUp(int position)
    {
        //int position = positionCount - 1;
        while (position != 0) {
            int parentPosition = (position - 1) / 2;
            if (comparator.compare(minHeap[position], minHeap[parentPosition]) >= 0) {
                break; // child is larger or equal
            }
            swap(position, parentPosition);
            heapDataChangeListener.positionChanged(minHeap[position], position);
            position = parentPosition;
        }
        heapDataChangeListener.positionChanged(minHeap[position], position);
    }

    public int estimatedInMemorySize()
    {
        return 0;
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

    public List<T> getData()
    {
        List<T> copy = new ArrayList<>();

        for (int i = 0; i < positionCount; i++) {
            copy.add(minHeap[i]);
        }
        return copy;
    }
}
