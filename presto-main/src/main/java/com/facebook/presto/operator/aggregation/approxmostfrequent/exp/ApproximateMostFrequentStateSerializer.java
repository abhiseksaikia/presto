/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.operator.aggregation.approxmostfrequent.exp;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class ApproximateMostFrequentStateSerializer
        implements AccumulatorStateSerializer<ApproximateMostFrequentState>
{
    private final Type type;
    private final Type serializedType;

    public ApproximateMostFrequentStateSerializer(Type type, Type serializedType)
    {
        this.type = type;
        this.serializedType = serializedType;
    }

    @Override
    public Type getSerializedType()
    {
        return serializedType;
    }

    @Override
    public void serialize(ApproximateMostFrequentState state, BlockBuilder out)
    {
        if (state.get() == null) {
            out.appendNull();
        }
        else {
            state.get().serialize(out);
        }
    }

    @Override
    public void deserialize(Block block, int index, ApproximateMostFrequentState state)
    {
        requireNonNull(block, "block is null");
        Block currentBlock = (Block) getSerializedType().getObject(block, index);
        int pos = 0;
        int maxBuckets = toIntExact(BIGINT.getLong(currentBlock, pos++));
        int capacity = toIntExact(BIGINT.getLong(currentBlock, pos++));
        int expectedSize = 10; // FIXME how to find this

        Block keysBlock = new ArrayType(type).getObject(currentBlock, pos++);
        Block valuesBlock = new ArrayType(BIGINT).getObject(currentBlock, pos++);

        final ApproximateMostFrequentFlattenHistogram histogram = new ApproximateMostFrequentFlattenHistogram(type, maxBuckets, capacity, expectedSize);
        for (int i = 0; i < keysBlock.getPositionCount(); i++) {
            histogram.add(i, keysBlock, BIGINT.getLong(valuesBlock, i));
        }
        state.set(histogram);
    }
}
