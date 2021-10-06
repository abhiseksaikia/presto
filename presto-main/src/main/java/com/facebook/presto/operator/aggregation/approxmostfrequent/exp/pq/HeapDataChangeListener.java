package com.facebook.presto.operator.aggregation.approxmostfrequent.exp.pq;

public interface HeapDataChangeListener<T>
{
    public void positionChanged(T heapEntry, int index);
}
