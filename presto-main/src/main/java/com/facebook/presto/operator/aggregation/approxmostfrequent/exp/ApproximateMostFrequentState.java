package com.facebook.presto.operator.aggregation.approxmostfrequent.exp;

import com.facebook.presto.spi.function.AccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateMetadata;

@AccumulatorStateMetadata(stateSerializerClass = ApproximateMostFrequentStateSerializer.class, stateFactoryClass = ApproximateMostFrequentStateFactory.class)
public interface ApproximateMostFrequentState
        extends AccumulatorState
{
    ApproximateMostFrequentFlattenHistogram get();

    void set(ApproximateMostFrequentFlattenHistogram value);
}
