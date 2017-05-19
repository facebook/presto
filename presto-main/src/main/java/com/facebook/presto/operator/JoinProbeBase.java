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
package com.facebook.presto.operator;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.DictionaryBlock;

public abstract class JoinProbeBase
    implements JoinProbe
{
    protected Page internalBuildDictionaryPage(int[] indices, int[] probeOutputChannels, PageBuilder sourcePageBuilder)
    {
        int sourceChannelCount = sourcePageBuilder.getChannelCount();
        Block[] blocks = new Block[probeOutputChannels.length + sourceChannelCount];
        int index = 0;
        for (int i = 0; i < probeOutputChannels.length; i++) {
            blocks[index++] = DictionaryBlock.mask(getPage().getBlock(probeOutputChannels[i]), indices);
        }
        for (int i = 0; i < sourceChannelCount; i++) {
            blocks[index++] = sourcePageBuilder.getBlockBuilder(i).build();
        }
        return new Page(sourcePageBuilder.getPositionCount(), blocks);
    }
}
