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

public interface PartitionFunction
{
    int getPartitionCount();

    /**
     * Gets the bucket for the tuple at the specified position.  This
     * value must be between 0 and partition count.
     *
     * @param functionArguments the arguments to partition function in order (no extra columns)
     */
    int getPartition(Page functionArguments, int position);
}
