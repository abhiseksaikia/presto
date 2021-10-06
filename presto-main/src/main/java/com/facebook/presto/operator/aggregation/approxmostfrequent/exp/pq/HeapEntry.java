package com.facebook.presto.operator.aggregation.approxmostfrequent.exp.pq;

public class HeapEntry
{
    private int hashPosition;
    private int generation;

    public HeapEntry(int hashPosition, int generation)
    {
        this.hashPosition = hashPosition;
        this.generation = generation;
    }

    public int getHashPosition()
    {
        return hashPosition;
    }

    public int getGeneration()
    {
        return generation;
    }

    public void setHashPosition(int hashPosition)
    {
        this.hashPosition = hashPosition;
    }

    public void setGeneration(int generation)
    {
        this.generation = generation;
    }
}
