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

import com.facebook.presto.operator.window.WindowFunction;
import com.facebook.presto.sql.tree.Input;
import com.google.common.base.Preconditions;

import java.util.Arrays;
import java.util.List;

public class WindowFunctionDefinition
{
    public static WindowFunctionDefinition window(WindowFunction function, List<Input> inputs)
    {
        Preconditions.checkNotNull(function, "function is null");
        Preconditions.checkNotNull(inputs, "inputs is null");

        return new WindowFunctionDefinition(function, inputs);
    }

    public static WindowFunctionDefinition window(WindowFunction function, Input... inputs)
    {
        Preconditions.checkNotNull(function, "function is null");
        Preconditions.checkNotNull(inputs, "inputs is null");

        return window(function, Arrays.asList(inputs));
    }

    private final WindowFunction function;
    private final List<Input> inputs;

    WindowFunctionDefinition(WindowFunction function, List<Input> inputs)
    {
        this.function = function;
        this.inputs = inputs;
    }

    public WindowFunction getFunction()
    {
        return function;
    }

    public List<Input> getInputs()
    {
        return inputs;
    }
}
