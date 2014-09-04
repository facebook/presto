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

import com.facebook.presto.OutputBuffers;
import com.facebook.presto.TaskSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.sql.planner.PlanFragment;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.ThreadPoolExecutorMBean;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.notNull;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.concurrent.Threads.threadsNamed;
import static java.util.concurrent.Executors.newScheduledThreadPool;

public class SqlTaskManager
        implements TaskManager, Closeable
{
    private static final Logger log = Logger.get(SqlTaskManager.class);

    private final ScheduledExecutorService taskManagementExecutor;
    private final ThreadPoolExecutorMBean taskManagementExecutorMBean;

    private final Duration infoCacheTime;
    private final Duration clientTimeout;

    private final LoadingCache<TaskId, SqlTask> tasks;

    private final SqlTaskIoStats cachedStats = new SqlTaskIoStats();
    private final SqlTaskIoStats finishedTaskStats = new SqlTaskIoStats();

    @Inject
    public SqlTaskManager(
            final LocationFactory locationFactory,
            final TaskNotificationExecutor taskNotificationExecutor,
            final TaskExecutionFactory taskExecutionFactory,
            TaskManagerConfig config)
    {
        checkNotNull(config, "config is null");
        infoCacheTime = config.getInfoMaxAge();
        clientTimeout = config.getClientTimeout();

        final DataSize maxBufferSize = config.getSinkMaxBufferSize();

        checkNotNull(taskNotificationExecutor, "taskNotificationExecutor is null");

        taskManagementExecutor = newScheduledThreadPool(5, threadsNamed("task-management-%d"));
        taskManagementExecutorMBean = new ThreadPoolExecutorMBean((ThreadPoolExecutor) taskManagementExecutor);

        tasks = CacheBuilder.newBuilder().build(new CacheLoader<TaskId, SqlTask>()
        {
            @Override
            public SqlTask load(TaskId taskId)
                    throws Exception
            {
                return new SqlTask(
                        taskId,
                        locationFactory.createLocalTaskLocation(taskId),
                        taskExecutionFactory,
                        taskNotificationExecutor.getExecutor(),
                        new Function<SqlTask, Void>()
                        {
                            @Override
                            public Void apply(SqlTask sqlTask)
                            {
                                finishedTaskStats.merge(sqlTask.getIoStats());
                                return null;
                            }
                        },
                        maxBufferSize
                );
            }
        });
    }

    @PostConstruct
    public void start()
    {
        taskManagementExecutor.scheduleAtFixedRate(new Runnable()
        {
            @Override
            public void run()
            {
                try {
                    removeOldTasks();
                }
                catch (Throwable e) {
                    log.warn(e, "Error removing old tasks");
                }
                try {
                    failAbandonedTasks();
                }
                catch (Throwable e) {
                    log.warn(e, "Error canceling abandoned tasks");
                }
            }
        }, 200, 200, TimeUnit.MILLISECONDS);

        taskManagementExecutor.scheduleAtFixedRate(new Runnable()
        {
            @Override
            public void run()
            {
                try {
                    updateStats();
                }
                catch (Throwable e) {
                    log.warn(e, "Error updating stats");
                }
            }
        }, 0, 1, TimeUnit.SECONDS);
    }

    @PreDestroy
    public void close()
    {
        taskManagementExecutor.shutdownNow();
    }

    @Managed
    @Flatten
    public SqlTaskIoStats getIoStats()
    {
        return cachedStats;
    }

    @Managed(description = "Task garbage collector executor")
    @Nested
    public ThreadPoolExecutorMBean getTaskManagementExecutor()
    {
        return taskManagementExecutorMBean;
    }

    @Override
    public List<TaskInfo> getAllTaskInfo()
    {
        return ImmutableList.copyOf(transform(tasks.asMap().values(), SqlTask.taskInfoGetter()));
    }

    @Override
    public TaskInfo getTaskInfo(TaskId taskId)
    {
        checkNotNull(taskId, "taskId is null");

        return tasks.getUnchecked(taskId).getTaskInfo();
    }

    @Override
    public ListenableFuture<TaskInfo> getTaskInfo(TaskId taskId, TaskState currentState)
    {
        checkNotNull(taskId, "taskId is null");
        checkNotNull(currentState, "currentState is null");

        return tasks.getUnchecked(taskId).getTaskInfo(currentState);
    }

    @Override
    public TaskInfo updateTask(ConnectorSession session, TaskId taskId, PlanFragment fragment, List<TaskSource> sources, OutputBuffers outputBuffers)
    {
        checkNotNull(session, "session is null");
        checkNotNull(taskId, "taskId is null");
        checkNotNull(fragment, "fragment is null");
        checkNotNull(sources, "sources is null");
        checkNotNull(outputBuffers, "outputBuffers is null");

        return tasks.getUnchecked(taskId).updateTask(session, fragment, sources, outputBuffers);
    }

    @Override
    public ListenableFuture<BufferResult> getTaskResults(TaskId taskId, String outputName, long startingSequenceId, DataSize maxSize)
    {
        checkNotNull(taskId, "taskId is null");
        checkNotNull(outputName, "outputName is null");
        Preconditions.checkArgument(startingSequenceId >= 0, "startingSequenceId is negative");
        checkNotNull(maxSize, "maxSize is null");

        return tasks.getUnchecked(taskId).getTaskResults(outputName, startingSequenceId, maxSize);
    }

    @Override
    public TaskInfo abortTaskResults(TaskId taskId, String outputId)
    {
        checkNotNull(taskId, "taskId is null");
        checkNotNull(outputId, "outputId is null");

        return tasks.getUnchecked(taskId).abortTaskResults(outputId);
    }

    @Override
    public TaskInfo cancelTask(TaskId taskId)
    {
        checkNotNull(taskId, "taskId is null");

        return tasks.getUnchecked(taskId).cancel();
    }

    public void removeOldTasks()
    {
        DateTime oldestAllowedTask = DateTime.now().minus(infoCacheTime.toMillis());
        for (TaskInfo taskInfo : filter(transform(tasks.asMap().values(), SqlTask.taskInfoGetter()), notNull())) {
            try {
                DateTime endTime = taskInfo.getStats().getEndTime();
                if (endTime != null && endTime.isBefore(oldestAllowedTask)) {
                    tasks.asMap().remove(taskInfo.getTaskId());
                }
            }
            catch (RuntimeException e) {
                log.warn(e, "Error while inspecting age of complete task %s", taskInfo.getTaskId());
            }
        }
    }

    public void failAbandonedTasks()
    {
        DateTime now = DateTime.now();
        DateTime oldestAllowedHeartbeat = now.minus(clientTimeout.toMillis());
        for (SqlTask sqlTask : tasks.asMap().values()) {
            try {
                TaskInfo taskInfo = sqlTask.getTaskInfo();
                if (taskInfo.getState().isDone()) {
                    continue;
                }
                DateTime lastHeartbeat = taskInfo.getLastHeartbeat();
                if (lastHeartbeat != null && lastHeartbeat.isBefore(oldestAllowedHeartbeat)) {
                    log.info("Failing abandoned task %s", taskInfo.getTaskId());
                    sqlTask.failed(new AbandonedException("Task " + taskInfo.getTaskId(), lastHeartbeat, now));
                }
            }
            catch (RuntimeException e) {
                log.warn(e, "Error while inspecting age of task %s", sqlTask.getTaskId());
            }
        }
    }

    //
    // Jmxutils only calls nested getters once, so we are forced to maintain a single
    // instance and periodically recalculate the stats.
    //
    private void updateStats()
    {
        SqlTaskIoStats tempIoStats = new SqlTaskIoStats();
        tempIoStats.merge(finishedTaskStats);

        for (SqlTask task : tasks.asMap().values()) {
            // there is a race here between task completion, which merges stats into
            // finishedTaskStats, and getting the stats from the task.  Since we have
            // already merged the final stats, we could miss the stats from this task
            // which would result in an under-count, but we will not get an over-count.
            if (!task.getTaskInfo().getState().isDone()) {
                tempIoStats.merge(task.getIoStats());
            }
        }
        cachedStats.resetTo(tempIoStats);
    }
}
