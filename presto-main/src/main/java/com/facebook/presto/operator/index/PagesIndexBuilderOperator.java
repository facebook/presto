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
package com.facebook.presto.operator.index;

import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.Page;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

@ThreadSafe
public class PagesIndexBuilderOperator
    implements Operator
{
    public static class PagesIndexBuilderOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final List<Type> types;
        private final AtomicReference<PagesIndexBuilder> pagesIndexBuilderReference = new AtomicReference<>();
        private boolean closed;

        public PagesIndexBuilderOperatorFactory(
                int operatorId,
                List<Type> types)
        {
            this.operatorId = operatorId;
            this.types = checkNotNull(types, "types is null");
        }

        public void setPagesIndexBuilder(PagesIndexBuilder pagesIndexBuilder)
        {
            checkNotNull(pagesIndexBuilder, "pagesIndexBuilder is null");
            checkState(pagesIndexBuilderReference.compareAndSet(null, pagesIndexBuilder), "Pages index builder has already been set");
        }

        @Override
        public List<Type> getTypes()
        {
            return types;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");

            PagesIndexBuilder pagesIndexBuilder = pagesIndexBuilderReference.get();
            checkState(pagesIndexBuilder != null, "Pages index builder has not been set");

            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, PagesIndexBuilderOperator.class.getSimpleName());
            return new PagesIndexBuilderOperator(
                    types,
                    operatorContext,
                    pagesIndexBuilder);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private final List<Type> types;
    private final OperatorContext operatorContext;
    private final PagesIndexBuilder pagesIndexBuilder;

    private boolean finished;

    public PagesIndexBuilderOperator(
            List<Type> types,
            OperatorContext operatorContext,
            PagesIndexBuilder pagesIndexBuilder)
    {
        this.types = ImmutableList.copyOf(checkNotNull(types, "types is null"));
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.pagesIndexBuilder = checkNotNull(pagesIndexBuilder, "pagesIndexBuilder is null");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public void finish()
    {
        finished = true;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return NOT_BLOCKED;
    }

    @Override
    public boolean needsInput()
    {
        return !finished;
    }

    @Override
    public void addInput(Page page)
    {
        checkNotNull(page, "page is null");
        checkState(!isFinished(), "Operator is already finished");

        if (!pagesIndexBuilder.tryAddPage(page)) {
            finish();
            return;
        }
        operatorContext.recordGeneratedOutput(page.getDataSize(), page.getPositionCount());
    }

    @Override
    public Page getOutput()
    {
        return null;
    }
}
