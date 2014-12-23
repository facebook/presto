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
package com.facebook.presto.raptor.storage;

import com.google.common.collect.ComparisonChain;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.log.Logger;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Comparator;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;

/**
 * This class is based on io.airlift.concurrent.BoundedExecutor
 */
@ThreadSafe
public class PrioritizedFifoExecutor
{
    private static final Logger log = Logger.get(PrioritizedFifoExecutor.class);

    private final Queue<FifoRunnableTask> queue;
    private final AtomicInteger queueSize = new AtomicInteger(0);
    private final AtomicLong sequenceNumber = new AtomicLong(0);
    private final Runnable triggerTask = this::executeOrMerge;

    private final ListeningExecutorService listeningExecutorService;
    private final int maxThreads;
    private final Comparator<Runnable> taskComparator;

    public PrioritizedFifoExecutor(ExecutorService coreExecutor, int maxThreads, Comparator<Runnable> taskComparator)
    {
        checkNotNull(coreExecutor, "coreExecutor is null");
        checkArgument(maxThreads > 0, "maxThreads must be greater than zero");

        this.taskComparator = checkNotNull(taskComparator, "taskComparator is null");
        this.listeningExecutorService = listeningDecorator(coreExecutor);
        this.maxThreads = maxThreads;
        this.queue = new PriorityBlockingQueue<>(maxThreads);
    }

    public ListenableFuture<?> submit(Runnable task)
    {
        queue.add(new FifoRunnableTask(task, sequenceNumber.incrementAndGet(), taskComparator));
        return listeningExecutorService.submit(triggerTask);
    }

    private void executeOrMerge()
    {
        int size = queueSize.incrementAndGet();
        if (size > maxThreads) {
            return;
        }
        do {
            try {
                queue.poll().getTask().run();
            }
            catch (Throwable e) {
                log.error(e, "Task failed");
            }
        }
        while (queueSize.getAndDecrement() > maxThreads);
    }

    private static class FifoRunnableTask
            implements Comparable<FifoRunnableTask>
    {
        private final Runnable task;
        private final long sequenceNumber;
        private final Comparator<Runnable> taskComparator;

        public FifoRunnableTask(Runnable task, long sequenceNumber, Comparator<Runnable> taskComparator)
        {
            this.task = checkNotNull(task, "task is null");
            this.sequenceNumber = checkNotNull(sequenceNumber, "sequenceNumber is null");
            this.taskComparator = checkNotNull(taskComparator, "taskComparator is null");
        }

        public Runnable getTask()
        {
            return task;
        }

        @Override
        public int compareTo(FifoRunnableTask other)
        {
            return ComparisonChain.start()
                    .compare(this.task, other.task, taskComparator)
                    .compare(this.sequenceNumber, other.sequenceNumber)
                    .result();
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            FifoRunnableTask other = (FifoRunnableTask) o;
            return Objects.equals(this.task, other.task) &&
                    Objects.equals(this.sequenceNumber, other.sequenceNumber);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(task, sequenceNumber);
        }
    }
}
