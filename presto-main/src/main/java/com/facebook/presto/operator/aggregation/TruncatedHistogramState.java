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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.operator.aggregation.state.AbstractGroupedAccumulatorState;
import com.facebook.presto.operator.aggregation.state.AccumulatorState;
import com.facebook.presto.operator.aggregation.state.AccumulatorStateFactory;
import com.facebook.presto.operator.aggregation.state.AccumulatorStateMetadata;
import com.facebook.presto.operator.aggregation.state.AccumulatorStateSerializer;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.MapType;
import com.facebook.presto.type.RowType;
import com.facebook.presto.util.array.ObjectBigArray;
import com.google.common.collect.ImmutableList;

import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

@AccumulatorStateMetadata(stateSerializerClass = TruncatedHistogramState.Serializer.class, stateFactoryClass = TruncatedHistogramState.Factory.class)
public interface TruncatedHistogramState
        extends AccumulatorState
{
    void set(TypedTruncatedHistogram histogram);

    TypedTruncatedHistogram get();

    void addMemoryUsage(long memory);

    class Factory
            implements AccumulatorStateFactory<TruncatedHistogramState>
    {
        @Override
        public TruncatedHistogramState createSingleState()
        {
            return new Factory.SingleTruncatedHistogramState();
        }

        @Override
        public Class<? extends TruncatedHistogramState> getSingleStateClass()
        {
            return Factory.SingleTruncatedHistogramState.class;
        }

        @Override
        public TruncatedHistogramState createGroupedState()
        {
            return new Factory.GroupedTruncatedHistogramState();
        }

        @Override
        public Class<? extends TruncatedHistogramState> getGroupedStateClass()
        {
            return Factory.GroupedTruncatedHistogramState.class;
        }

        public static class GroupedTruncatedHistogramState
                extends AbstractGroupedAccumulatorState
                implements TruncatedHistogramState
        {
            private final ObjectBigArray<TypedTruncatedHistogram> typedHistogram = new ObjectBigArray<>();
            private long size;

            @Override
            public void ensureCapacity(long size)
            {
                typedHistogram.ensureCapacity(size);
            }

            @Override
            public TypedTruncatedHistogram get()
            {
                return typedHistogram.get(getGroupId());
            }

            @Override
            public void set(TypedTruncatedHistogram value)
            {
                requireNonNull(value, "value is null");

                TypedTruncatedHistogram previous = get();
                if (previous != null) {
                    size -= previous.getEstimatedSize();
                }

                typedHistogram.set(getGroupId(), value);
                size += value.getEstimatedSize();
            }

            @Override
            public void addMemoryUsage(long memory)
            {
                size += memory;
            }

            @Override
            public long getEstimatedSize()
            {
                return size + typedHistogram.sizeOf();
            }
        }

        public static class SingleTruncatedHistogramState
                implements TruncatedHistogramState
        {
            private TypedTruncatedHistogram typedHistogram;

            @Override
            public TypedTruncatedHistogram get()
            {
                return typedHistogram;
            }

            @Override
            public void set(TypedTruncatedHistogram value)
            {
                typedHistogram = value;
            }

            @Override
            public void addMemoryUsage(long memory)
            {
            }

            @Override
            public long getEstimatedSize()
            {
                if (typedHistogram == null) {
                    return 0;
                }
                return typedHistogram.getEstimatedSize();
            }
        }
    }

    class Serializer
            implements AccumulatorStateSerializer<TruncatedHistogramState>
    {
        private final Type type;

        public Serializer(Type type)
        {
            this.type = type;
        }

        @Override
        public Type getSerializedType()
        {
            return new RowType(ImmutableList.of(BIGINT, new MapType(type, BIGINT)), Optional.empty());
        }

        @Override
        public void serialize(TruncatedHistogramState state, BlockBuilder out)
        {
            if (state.get() == null) {
                out.appendNull();
            }
            else {
                state.get().serialize(out);
            }
        }

        @Override
        public void deserialize(Block block, int index, TruncatedHistogramState state)
        {
            if (block.isNull(index)) {
                state.set(null);
            }
            else {
                Block subBlock = (Block) getSerializedType().getObject(block, index);
                state.set(TypedTruncatedHistogram.deserialize(type, subBlock));
            }
        }
    }
}
