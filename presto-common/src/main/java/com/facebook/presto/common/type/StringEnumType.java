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
package com.facebook.presto.common.type;

import java.util.Map;

public class StringEnumType
        extends VarcharType
        implements EnumType<String>
{
    private final Map<String, String> entries;

    public StringEnumType(String name, Map<String, String> entries)
    {
        super(VarcharType.MAX_LENGTH, TypeSignature.parseTypeSignature(name));
        this.entries = entries;
    }

    @Override
    public Map<String, String> getEntries()
    {
        return entries;
    }

    @Override
    public boolean isOrderable()
    {
        return false;
    }

    @Override
    public boolean isComparable()
    {
        return false;
    }

    @Override
    public Type getValueType()
    {
        return VarcharType.VARCHAR;
    }

    @Override
    public TypeMetadata getTypeMetadata()
    {
        return new TypeMetadata(getValueType().getTypeSignature(), this.getEntries());
    }
}
