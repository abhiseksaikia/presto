package com.facebook.presto.operator.aggregation.approxmostfrequent.exp;

public class HeapEntry
{
    private int blockPosition;
    private int generation;

    public HeapEntry(int blockPosition, int generation)
    {
        this.blockPosition = blockPosition;
        this.generation = generation;
    }

    public int getGeneration()
    {
        return generation;
    }

    public int getBlockPosition()
    {
        return blockPosition;
    }

    public void setBlockPosition(int blockPosition)
    {
        this.blockPosition = blockPosition;
    }

    public void setGeneration(int generation)
    {
        this.generation = generation;
    }
}
