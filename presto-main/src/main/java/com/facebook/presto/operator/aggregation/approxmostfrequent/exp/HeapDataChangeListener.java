package com.facebook.presto.operator.aggregation.approxmostfrequent.exp;

public interface HeapDataChangeListener<T>
{
    public void positionChanged(T heapEntry, int index);
}
