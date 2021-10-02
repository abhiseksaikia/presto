package com.facebook.presto.operator.aggregation.approxmostfrequent.exp;

import com.facebook.presto.common.block.Block;

public interface StreamSummaryReader
{
    void read(Block block, int position, long count);
}
