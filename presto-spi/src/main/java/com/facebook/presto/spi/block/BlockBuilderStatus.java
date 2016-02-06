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

import org.openjdk.jol.info.ClassLayout;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import static com.facebook.presto.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static java.util.Objects.requireNonNull;

public class BlockBuilderStatus
{
    public static final int INSTANCE_SIZE = deepInstanceSize(BlockBuilderStatus.class);
    public static final int DEFAULT_MAX_BLOCK_SIZE_IN_BYTES = 64 * 1024;

    private final PageBuilderStatus pageBuilderStatus;
    private final int maxBlockSizeInBytes;

    private int currentSize;

    public BlockBuilderStatus()
    {
        // When this constructor is used, this class has no observable internal state (except getMaxBlockSizeInBytes).
        // TODO: this constructor essentially constructs a black hole. This constructor and all its usage should probably be removed.
        this(new PageBuilderStatus(DEFAULT_MAX_PAGE_SIZE_IN_BYTES, DEFAULT_MAX_BLOCK_SIZE_IN_BYTES), DEFAULT_MAX_BLOCK_SIZE_IN_BYTES);
    }

    BlockBuilderStatus(PageBuilderStatus pageBuilderStatus, int maxBlockSizeInBytes)
    {
        this.pageBuilderStatus = requireNonNull(pageBuilderStatus, "pageBuilderStatus must not be null");
        this.maxBlockSizeInBytes = maxBlockSizeInBytes;
    }

    public int getMaxBlockSizeInBytes()
    {
        return maxBlockSizeInBytes;
    }

    public void addBytes(int bytes)
    {
        currentSize += bytes;
        pageBuilderStatus.addBytes(bytes);
        if (currentSize >= maxBlockSizeInBytes) {
            pageBuilderStatus.setFull();
        }
    }

    @Override
    public String toString()
    {
        StringBuilder buffer = new StringBuilder("BlockBuilderStatus{");
        buffer.append("maxSizeInBytes=").append(maxBlockSizeInBytes);
        buffer.append(", currentSize=").append(currentSize);
        buffer.append('}');
        return buffer.toString();
    }

    /**
     * Computes the size of an instance of this class assuming that all reference fields are non-null
     */
    private static int deepInstanceSize(Class<?> clazz)
    {
        if (clazz.isArray()) {
            throw new IllegalArgumentException(String.format("Cannot determine size of %s because it contains an array", clazz.getSimpleName()));
        }
        if (clazz.isInterface()) {
            throw new IllegalArgumentException(String.format("%s is an interface", clazz.getSimpleName()));
        }
        if (Modifier.isAbstract(clazz.getModifiers())) {
            throw new IllegalArgumentException(String.format("%s is abstract", clazz.getSimpleName()));
        }
        if (!clazz.getSuperclass().equals(Object.class)) {
            throw new IllegalArgumentException(String.format("Cannot determine size of a subclass. %s extends from %s", clazz.getSimpleName(), clazz.getSuperclass().getSimpleName()));
        }

        int size = ClassLayout.parseClass(clazz).instanceSize();
        for (Field field : clazz.getDeclaredFields()) {
            if (!field.getType().isPrimitive()) {
                size += deepInstanceSize(field.getType());
            }
        }
        return size;
    }
}
