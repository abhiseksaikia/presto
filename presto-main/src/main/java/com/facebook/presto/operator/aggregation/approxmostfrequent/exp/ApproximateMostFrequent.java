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

import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.operator.aggregation.AccumulatorCompiler;
import com.facebook.presto.operator.aggregation.AggregationMetadata;
import com.facebook.presto.operator.aggregation.GenericAccumulatorFactoryBinder;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.spi.function.CombineFunction;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.common.type.StandardTypes.BIGINT;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.function.Signature.comparableTypeParameter;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Math.toIntExact;

/**
 * <p>
 * Aggregation function that approximates the frequency of the top-K elements.
 * This function keeps counts for a "frequent" subset of elements and assumes all other elements
 * once fewer than the least-frequent "frequent" element.
 * </p>
 *
 * <p>
 * The algorithm is based loosely on:
 * <a href="https://dl.acm.org/doi/10.1007/978-3-540-30570-5_27">Efficient Computation of Frequent and Top-*k* Elements in Data Streams</a>
 * by Ahmed Metwally, Divyakant Agrawal, and Amr El Abbadi
 * </p>
 */
public final class ApproximateMostFrequent
        extends SqlAggregationFunction
{
    public static final ApproximateMostFrequent APPROXIMATE_MOST_FREQUENT = new ApproximateMostFrequent();
    public static final String NAME = "approx_most_frequent_improved";
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(ApproximateMostFrequent.class, "output", ApproximateMostFrequentState.class, BlockBuilder.class);
    private static final MethodHandle INPUT_FUNCTION = methodHandle(ApproximateMostFrequent.class, "input", Type.class, ApproximateMostFrequentState.class, long.class, Block.class, int.class, long.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(ApproximateMostFrequent.class, "combine", ApproximateMostFrequentState.class, ApproximateMostFrequentState.class);

    protected ApproximateMostFrequent()
    {
        super(NAME,
                ImmutableList.of(comparableTypeParameter("K")),
                ImmutableList.of(),
                parseTypeSignature("map(K,bigint)"),
                ImmutableList.of(parseTypeSignature(BIGINT), parseTypeSignature("K"), parseTypeSignature(BIGINT)));
    }

    @Override
    public InternalAggregationFunction specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        Type keyType = boundVariables.getTypeVariable("K");
        Type serializedType = RowType.withDefaultFieldNames(ImmutableList.of(BigintType.BIGINT, BigintType.BIGINT, new ArrayType(keyType), new ArrayType(BigintType.BIGINT)));
        Type outputType = functionAndTypeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(
                TypeSignatureParameter.of(keyType.getTypeSignature()),
                TypeSignatureParameter.of(BigintType.BIGINT.getTypeSignature())));

        DynamicClassLoader classLoader = new DynamicClassLoader(ApproximateMostFrequent.class.getClassLoader());
        List<Type> inputTypes = ImmutableList.of(keyType);
        ApproximateMostFrequentStateSerializer stateSerializer = new ApproximateMostFrequentStateSerializer(keyType, serializedType);
        Type intermediateType = stateSerializer.getSerializedType();
        MethodHandle inputFunction = INPUT_FUNCTION.bindTo(keyType);

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, outputType.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                createInputParameterMetadata(keyType),
                inputFunction,
                COMBINE_FUNCTION,
                OUTPUT_FUNCTION,
                ImmutableList.of(new AggregationMetadata.AccumulatorStateDescriptor(
                        ApproximateMostFrequentState.class,
                        stateSerializer,
                        new ApproximateMostFrequentStateFactory())),
                outputType);

        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(NAME, inputTypes, ImmutableList.of(intermediateType), outputType, true, false, factory);
    }

    private static List<AggregationMetadata.ParameterMetadata> createInputParameterMetadata(Type keyType)
    {
        return ImmutableList.of(new AggregationMetadata.ParameterMetadata(STATE),
                new AggregationMetadata.ParameterMetadata(INPUT_CHANNEL, BigintType.BIGINT),
                new AggregationMetadata.ParameterMetadata(BLOCK_INPUT_CHANNEL, keyType),
                new AggregationMetadata.ParameterMetadata(BLOCK_INDEX),
                new AggregationMetadata.ParameterMetadata(INPUT_CHANNEL, BigintType.BIGINT));
    }

    @Override
    public String getDescription()
    {
        return "Computes the top frequent elements approximately";
    }

    public static void input(Type type,
            ApproximateMostFrequentState state,
            long buckets,
            Block valueBlock,
            int valueIndex, long capacity)
    {
        StreamSummary streamSummary = state.get();
        if (streamSummary == null) {
            checkCondition(buckets >= 2, INVALID_FUNCTION_ARGUMENT, "approx_most_frequent bucket count must be greater than one");
            streamSummary = new StreamSummary(
                    type,
                    toIntExact(buckets),
                    toIntExact(capacity));
            state.set(streamSummary);
        }
        streamSummary.add(valueBlock, valueIndex, 1L);
    }

    @CombineFunction
    public static void combine(ApproximateMostFrequentState state, ApproximateMostFrequentState otherState)
    {
        StreamSummary otherStreamSummary = otherState.get();

        StreamSummary streamSummary = state.get();
        if (streamSummary == null) {
            state.set(otherStreamSummary);
        }
        else {
            streamSummary.merge(otherStreamSummary);
        }
    }

    public static void output(ApproximateMostFrequentState state, BlockBuilder out)
    {
        if (state.get() == null) {
            out.appendNull();
        }
        else {
            state.get().topK(out);
        }
    }
}
