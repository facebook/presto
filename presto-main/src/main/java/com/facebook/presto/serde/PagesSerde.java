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
package com.facebook.presto.serde;

import com.facebook.presto.block.Block;
import com.facebook.presto.operator.Page;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Arrays.asList;

public final class PagesSerde
{
    private PagesSerde()
    {
    }

    public static PagesWriter createPagesWriter(final BlockEncodingManager blockEncodingManager, final SliceOutput sliceOutput)
    {
        checkNotNull(sliceOutput, "sliceOutput is null");
        return new PagesWriter()
        {
            private BlockEncoding[] blockEncodings;

            @Override
            public PagesWriter append(Page page)
            {
                Preconditions.checkNotNull(page, "page is null");

                if (blockEncodings == null) {
                    Block[] blocks = page.getBlocks();
                    blockEncodings = new BlockEncoding[blocks.length];
                    sliceOutput.writeInt(blocks.length);
                    for (int i = 0; i < blocks.length; i++) {
                        Block block = blocks[i];
                        BlockEncoding blockEncoding = block.getEncoding();
                        blockEncodings[i] = blockEncoding;
                        blockEncodingManager.writeBlockEncoding(sliceOutput, blockEncoding);
                    }
                }

                sliceOutput.writeInt(page.getPositionCount());
                Block[] blocks = page.getBlocks();
                for (int i = 0; i < blocks.length; i++) {
                    blockEncodings[i].writeBlock(sliceOutput, blocks[i]);
                }

                return this;
            }
        };
    }

    public static void writePages(BlockEncodingManager blockEncodingManager, SliceOutput sliceOutput, Page... pages)
    {
        writePages(blockEncodingManager, sliceOutput, asList(pages).iterator());
    }

    public static void writePages(BlockEncodingManager blockEncodingManager, SliceOutput sliceOutput, Iterable<Page> pages)
    {
        writePages(blockEncodingManager, sliceOutput, pages.iterator());
    }

    public static void writePages(BlockEncodingManager blockEncodingManager, SliceOutput sliceOutput, Iterator<Page> pages)
    {
        PagesWriter pagesWriter = createPagesWriter(blockEncodingManager, sliceOutput);
        while (pages.hasNext()) {
            pagesWriter.append(pages.next());
        }
    }

    public static Iterator<Page> readPages(BlockEncodingManager blockEncodingManager, SliceInput sliceInput)
    {
        Preconditions.checkNotNull(sliceInput, "sliceInput is null");
        return new PagesReader(blockEncodingManager, sliceInput);
    }

    private static class PagesReader
            extends AbstractIterator<Page>
    {
        private final BlockEncodingManager blockEncodingManager;
        private final BlockEncoding[] blockEncodings;
        private final SliceInput sliceInput;

        public PagesReader(BlockEncodingManager blockEncodingManager, SliceInput sliceInput)
        {
            this.blockEncodingManager = blockEncodingManager;
            this.sliceInput = sliceInput;

            if (!sliceInput.isReadable()) {
                endOfData();
                blockEncodings = new BlockEncoding[0];
            }
            else {
                int channelCount = sliceInput.readInt();

                blockEncodings = new BlockEncoding[channelCount];
                for (int i = 0; i < blockEncodings.length; i++) {
                    blockEncodings[i] = this.blockEncodingManager.readBlockEncoding(sliceInput);
                }
            }
        }

        @Override
        protected Page computeNext()
        {
            if (!sliceInput.isReadable()) {
                return endOfData();
            }

            int positions = sliceInput.readInt();
            Block[] blocks = new Block[blockEncodings.length];
            for (int i = 0; i < blocks.length; i++) {
                blocks[i] = blockEncodings[i].readBlock(sliceInput);
            }
            Page page = new Page(positions, blocks);
            return page;
        }
    }
}
