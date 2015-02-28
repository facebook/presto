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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;

import static com.facebook.presto.spi.type.DateType.DATE;

public class NullableDateStateSerializer
        implements AccumulatorStateSerializer<NullableDateState>
{
    @Override
    public Type getSerializedType()
    {
        return DATE;
    }

    @Override
    public void serialize(NullableDateState state, BlockBuilder out)
    {
        if (state.isNull()) {
            out.appendNull();
        }
        else {
            DATE.writeLong(out, state.getDate());
        }
    }

    @Override
    public void deserialize(Block block, int index, NullableDateState state)
    {
        state.setNull(block.isNull(index));
        if (!state.isNull()) {
            state.setDate(DATE.getLong(block, index));
        }
    }
}
