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
package com.facebook.presto.common.function;

import java.lang.invoke.MethodHandle;

public class OperatorMethodHandle
{
    private final InvocationConvention callingConvention;
    private final MethodHandle methodHandle;

    public OperatorMethodHandle(InvocationConvention callingConvention, MethodHandle methodHandle)
    {
        this.callingConvention = callingConvention;
        this.methodHandle = methodHandle;
    }

    public InvocationConvention getCallingConvention()
    {
        return callingConvention;
    }

    public MethodHandle getMethodHandle()
    {
        return methodHandle;
    }
}
