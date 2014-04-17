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
package com.facebook.presto.spi.block;

import com.facebook.presto.spi.Session;
import io.airlift.slice.Slice;

public interface RandomAccessBlock
        extends Block
{
    /**
     * Returns a block starting at the specified position and extends for the
     * specified length.  The specified region must be entirely contained
     * within this block.
     */
    RandomAccessBlock getRegion(int positionOffset, int length);

    /**
     * Gets the value at the specified position as a boolean.
     *
     * @throws IllegalArgumentException if this position is not valid
     */
    boolean getBoolean(int position);

    /**
     * Gets the value at the specified position as a long.
     *
     * @throws IllegalArgumentException if this position is not valid
     */
    long getLong(int position);

    /**
     * Gets the value at the specified position as a double.
     *
     * @throws IllegalArgumentException if this position is not valid
     */
    double getDouble(int position);

    /**
     * Gets the value at the specified position as an Object.
     *
     * @throws IllegalArgumentException if this position is not valid
     */
    Object getObjectValue(Session session, int position);

    /**
     * Gets the value at the specified position as a Slice.
     *
     * @throws IllegalArgumentException if this position is not valid
     */
    Slice getSlice(int position);

    /**
     * Gets the value at the specified position as a single element block.
     *
     * @throws IllegalStateException if this cursor is not at a valid position
     */
    RandomAccessBlock getSingleValueBlock(int position);

    /**
     * Is the specified position null.
     *
     * @throws IllegalArgumentException if this position is not valid
     */
    boolean isNull(int position);

    boolean equals(int position, RandomAccessBlock right, int rightPosition);
    boolean equals(int position, BlockCursor cursor);
    boolean equals(int position, Slice slice, int offset);

    int hashCode(int position);

    int compareTo(SortOrder sortOrder, int position, RandomAccessBlock right, int rightPosition);
    int compareTo(SortOrder sortOrder, int position, BlockCursor cursor);
    int compareTo(int position, Slice slice, int offset);

    void appendTo(int position, BlockBuilder blockBuilder);
}
