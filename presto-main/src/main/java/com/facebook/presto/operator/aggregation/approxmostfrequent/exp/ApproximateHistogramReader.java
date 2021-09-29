package com.facebook.presto.operator.aggregation.approxmostfrequent.exp;

import com.facebook.presto.common.block.Block;

public interface ApproximateHistogramReader
{
    void read(int position, Block block, long count);
}
