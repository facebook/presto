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
package com.facebook.presto.execution;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import io.airlift.concurrent.ThreadPoolExecutorMBean;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.PreDestroy;
import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import static io.airlift.concurrent.Threads.threadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

@ThreadSafe
public class TaskNotificationExecutor
{
    private final ExecutorService executor;
    private final ThreadPoolExecutorMBean executorMBean;

    @Inject
    public TaskNotificationExecutor()
    {
        executor = newCachedThreadPool(threadsNamed("task-notification-%d"));
        executorMBean = new ThreadPoolExecutorMBean((ThreadPoolExecutor) executor);
    }

    @VisibleForTesting
    public TaskNotificationExecutor(ExecutorService executor)
    {
        this.executor = executor;
        executorMBean = new ThreadPoolExecutorMBean((ThreadPoolExecutor) executor);
    }

    public ExecutorService getExecutor()
    {
        return executor;
    }

    @PreDestroy
    public void stop()
    {
        executor.shutdownNow();
    }

    @Managed(description = "Task notification executor")
    @Nested
    public ThreadPoolExecutorMBean getTaskNotificationExecutor()
    {
        return executorMBean;
    }
}
