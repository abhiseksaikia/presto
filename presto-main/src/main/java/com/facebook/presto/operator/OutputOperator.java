package com.facebook.presto.operator;

public interface OutputOperator
        extends Operator
{
    void annotateSplitSequenceID(long splitID);
}
