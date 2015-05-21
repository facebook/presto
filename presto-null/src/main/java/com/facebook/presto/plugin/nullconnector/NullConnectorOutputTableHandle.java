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

package com.facebook.presto.plugin.nullconnector;

import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class NullConnectorOutputTableHandle
        implements ConnectorOutputTableHandle
{
    // HACK: dummy field is required to make this class be auto serializable
    private String dummy;

    @JsonCreator
    public NullConnectorOutputTableHandle(@JsonProperty("dummy") String dummy)
    {
        this.dummy = dummy;
    }

    @JsonProperty
    public String getDummy()
    {
        return dummy;
    }
}
