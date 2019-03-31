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
package com.facebook.presto.operator.aggregation.state;

import com.facebook.presto.array.ObjectBigArray;
import com.facebook.presto.spi.function.AccumulatorStateFactory;

import org.openjdk.jol.info.ClassLayout;

import static java.util.Objects.requireNonNull;

public class TopElementsStateFactory
        implements AccumulatorStateFactory<TopElementsState>
{
    @Override
    public TopElementsState createSingleState()
    {
        return new SingleTopElementsState();
    }

    @Override
    public Class<? extends TopElementsState> getSingleStateClass()
    {
        return SingleTopElementsState.class;
    }

    @Override
    public TopElementsState createGroupedState()
    {
        return new GroupedTopElementsState();
    }

    @Override
    public Class<? extends TopElementsState> getGroupedStateClass()
    {
        return GroupedTopElementsState.class;
    }

    public static class GroupedTopElementsState
            extends AbstractGroupedAccumulatorState
            implements TopElementsState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedTopElementsState.class).instanceSize();
        private final ObjectBigArray<TopElementsHistogram> histogramArray = new ObjectBigArray<>();
        private long size;

        @Override
        public void ensureCapacity(long size)
        {
            histogramArray.ensureCapacity(size);
        }

        @Override
        public TopElementsHistogram getHistogram()
        {
            return histogramArray.get(getGroupId());
        }

        @Override
        public void setHistogram(TopElementsHistogram value)
        {
            requireNonNull(value, "value is null");
            histogramArray.set(getGroupId(), value);
        }

        @Override
        public void addMemoryUsage(long value)
        {
            size += value;
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + size + histogramArray.sizeOf();
        }
    }

    public static class SingleTopElementsState
            implements TopElementsState
    {
        private TopElementsHistogram histogram;

        @Override
        public TopElementsHistogram getHistogram()
        {
            return histogram;
        }

        @Override
        public void setHistogram(TopElementsHistogram value)
        {
            histogram = value;
        }

        @Override
        public long getEstimatedSize()
        {
            if (histogram == null) {
                return 0;
            }
            return histogram.estimatedInMemorySize();
        }

        @Override
        public void addMemoryUsage(long value)
        {
            // noop
        }

    }
}
