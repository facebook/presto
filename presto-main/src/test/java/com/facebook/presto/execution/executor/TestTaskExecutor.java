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
package com.facebook.presto.execution.executor;

import com.facebook.presto.execution.SplitRunner;
import com.facebook.presto.execution.TaskId;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.testing.TestingTicker;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.execution.executor.MultilevelSplitQueue.LEVELS;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;

public class TestTaskExecutor
{
    @Test(invocationCount = 100)
    public void testTasksComplete()
            throws Exception
    {
        TestingTicker ticker = new TestingTicker();
        TaskExecutor taskExecutor = new TaskExecutor(4, 8, ticker);
        taskExecutor.start();
        ticker.increment(20, MILLISECONDS);

        try {
            TaskId taskId = new TaskId("test", 0, 0);
            TaskHandle taskHandle = taskExecutor.addTask(taskId, () -> 0, 10, new Duration(1, MILLISECONDS));

            Phaser beginPhase = new Phaser();
            beginPhase.register();
            Phaser verificationComplete = new Phaser();
            verificationComplete.register();

            // add two jobs
            TestingJob driver1 = new TestingJob(ticker, new Phaser(), beginPhase, verificationComplete, 10, 0);
            ListenableFuture<?> future1 = getOnlyElement(taskExecutor.enqueueSplits(taskHandle, true, ImmutableList.of(driver1)));
            TestingJob driver2 = new TestingJob(ticker, new Phaser(), beginPhase, verificationComplete, 10, 0);
            ListenableFuture<?> future2 = getOnlyElement(taskExecutor.enqueueSplits(taskHandle, true, ImmutableList.of(driver2)));
            assertEquals(driver1.getCompletedPhases(), 0);
            assertEquals(driver2.getCompletedPhases(), 0);

            // verify worker have arrived but haven't processed yet
            beginPhase.arriveAndAwaitAdvance();
            assertEquals(driver1.getCompletedPhases(), 0);
            assertEquals(driver2.getCompletedPhases(), 0);
            ticker.increment(10, MILLISECONDS);
            assertEquals(taskExecutor.getMaxActiveSplitTime(), 10);
            verificationComplete.arriveAndAwaitAdvance();

            // advance one phase and verify
            beginPhase.arriveAndAwaitAdvance();
            assertEquals(driver1.getCompletedPhases(), 1);
            assertEquals(driver2.getCompletedPhases(), 1);

            verificationComplete.arriveAndAwaitAdvance();

            // add one more job
            TestingJob driver3 = new TestingJob(ticker, new Phaser(), beginPhase, verificationComplete, 10, 0);
            ListenableFuture<?> future3 = getOnlyElement(taskExecutor.enqueueSplits(taskHandle, false, ImmutableList.of(driver3)));

            // advance one phase and verify
            beginPhase.arriveAndAwaitAdvance();
            assertEquals(driver1.getCompletedPhases(), 2);
            assertEquals(driver2.getCompletedPhases(), 2);
            assertEquals(driver3.getCompletedPhases(), 0);
            verificationComplete.arriveAndAwaitAdvance();

            // advance to the end of the first two task and verify
            beginPhase.arriveAndAwaitAdvance();
            for (int i = 0; i < 7; i++) {
                verificationComplete.arriveAndAwaitAdvance();
                beginPhase.arriveAndAwaitAdvance();
                assertEquals(beginPhase.getPhase(), verificationComplete.getPhase() + 1);
            }
            assertEquals(driver1.getCompletedPhases(), 10);
            assertEquals(driver2.getCompletedPhases(), 10);
            assertEquals(driver3.getCompletedPhases(), 8);
            future1.get(1, TimeUnit.SECONDS);
            future2.get(1, TimeUnit.SECONDS);
            verificationComplete.arriveAndAwaitAdvance();

            // advance two more times and verify
            beginPhase.arriveAndAwaitAdvance();
            verificationComplete.arriveAndAwaitAdvance();
            beginPhase.arriveAndAwaitAdvance();
            assertEquals(driver1.getCompletedPhases(), 10);
            assertEquals(driver2.getCompletedPhases(), 10);
            assertEquals(driver3.getCompletedPhases(), 10);
            future3.get(1, TimeUnit.SECONDS);
            verificationComplete.arriveAndAwaitAdvance();

            assertEquals(driver1.getFirstPhase(), 0);
            assertEquals(driver2.getFirstPhase(), 0);
            assertEquals(driver3.getFirstPhase(), 2);

            assertEquals(driver1.getLastPhase(), 10);
            assertEquals(driver2.getLastPhase(), 10);
            assertEquals(driver3.getLastPhase(), 12);

            // no splits remaining
            ticker.increment(30, MILLISECONDS);
            assertEquals(taskExecutor.getMaxActiveSplitTime(), 0);
        }
        finally {
            taskExecutor.stop();
        }
    }

    @Test(invocationCount = 100)
    public void testQuantaFairness()
            throws Exception
    {
        TestingTicker ticker = new TestingTicker();
        TaskExecutor taskExecutor = new TaskExecutor(1, 2, ticker);
        taskExecutor.start();
        ticker.increment(20, MILLISECONDS);

        try {
            TaskHandle shortQuantaTaskHandle = taskExecutor.addTask(new TaskId("shortQuanta", 0, 0), () -> 0, 10, new Duration(1, MILLISECONDS));
            TaskHandle longQuantaTaskHandle = taskExecutor.addTask(new TaskId("longQuanta", 0, 0), () -> 0, 10, new Duration(1, MILLISECONDS));

            Phaser globalPhaser = new Phaser();
            globalPhaser.bulkRegister(2);

            TestingJob shortQuantaDriver = new TestingJob(ticker, globalPhaser, new Phaser(), new Phaser(), 10, 10);
            TestingJob longQuantaDriver = new TestingJob(ticker, globalPhaser, new Phaser(), new Phaser(), 10, 20);

            taskExecutor.enqueueSplits(shortQuantaTaskHandle, true, ImmutableList.of(shortQuantaDriver));
            taskExecutor.enqueueSplits(longQuantaTaskHandle, true, ImmutableList.of(longQuantaDriver));

            for (int i = 0; i < 12; i++) {
                globalPhaser.arriveAndAwaitAdvance();
            }

            MILLISECONDS.sleep(1);

            assertEquals(shortQuantaDriver.getCompletedPhases(), 8);
            assertEquals(longQuantaDriver.getCompletedPhases(), 4);

            globalPhaser.arriveAndDeregister();
        }
        finally {
            taskExecutor.stop();
        }
    }

    @Test(invocationCount = 100)
    public void testLevelMovement()
            throws Exception
    {
        TestingTicker ticker = new TestingTicker();
        TaskExecutor taskExecutor = new TaskExecutor(2, 2, ticker);
        taskExecutor.start();
        ticker.increment(20, MILLISECONDS);

        try {
            TaskHandle testTaskHandle = taskExecutor.addTask(new TaskId("test", 0, 0), () -> 0, 10, new Duration(1, MILLISECONDS));

            Phaser globalPhaser = new Phaser();
            globalPhaser.bulkRegister(3);

            int quantaTimeMills = 500;
            int phasesPerSecond = 1000 / quantaTimeMills;
            int totalPhases = LEVELS[LEVELS.length - 1] * phasesPerSecond;
            TestingJob driver1 = new TestingJob(ticker, globalPhaser, new Phaser(), new Phaser(), totalPhases, quantaTimeMills);
            TestingJob driver2 = new TestingJob(ticker, globalPhaser, new Phaser(), new Phaser(), totalPhases, quantaTimeMills);

            taskExecutor.enqueueSplits(testTaskHandle, true, ImmutableList.of(driver1, driver2));

            int completedPhases = 0;
            for (int i = 0; i < (LEVELS.length - 1); i++) {
                for (; (completedPhases / phasesPerSecond) < LEVELS[i + 1]; completedPhases++) {
                    globalPhaser.arriveAndAwaitAdvance();
                }

                MILLISECONDS.sleep(1);
                assertEquals(taskExecutor.calculateRunningTasksForLevel(i + 1), 1);
            }

            globalPhaser.arriveAndDeregister();
        }
        finally {
            taskExecutor.stop();
        }
    }

    @Test
    public void testTaskHandle()
            throws Exception
    {
        TestingTicker ticker = new TestingTicker();
        TaskExecutor taskExecutor = new TaskExecutor(4, 8, ticker);
        taskExecutor.start();

        try {
            TaskId taskId = new TaskId("test", 0, 0);
            TaskHandle taskHandle = taskExecutor.addTask(taskId, () -> 0, 10, new Duration(1, MILLISECONDS));

            Phaser beginPhase = new Phaser();
            beginPhase.register();
            Phaser verificationComplete = new Phaser();
            verificationComplete.register();

            TestingJob driver1 = new TestingJob(ticker, new Phaser(), beginPhase, verificationComplete, 10, 0);
            TestingJob driver2 = new TestingJob(ticker, new Phaser(), beginPhase, verificationComplete, 10, 0);

            // force enqueue a split
            taskExecutor.enqueueSplits(taskHandle, true, ImmutableList.of(driver1));
            assertEquals(taskHandle.getRunningLeafSplits(), 0);

            // normal enqueue a split
            taskExecutor.enqueueSplits(taskHandle, false, ImmutableList.of(driver2));
            assertEquals(taskHandle.getRunningLeafSplits(), 1);

            // let the split continue to run
            beginPhase.arriveAndDeregister();
            verificationComplete.arriveAndDeregister();
        }
        finally {
            taskExecutor.stop();
        }
    }

    private static class TestingJob
            implements SplitRunner
    {
        private final TestingTicker ticker;
        private final Phaser globalPhaser;
        private final Phaser beginQuantaPhaser;
        private final Phaser endQuantaPhaser;
        private final int requiredPhases;
        private final int quantaTimeMillis;
        private final AtomicInteger completedPhases = new AtomicInteger();

        private final AtomicInteger firstPhase = new AtomicInteger(-1);
        private final AtomicInteger lastPhase = new AtomicInteger(-1);

        public TestingJob(TestingTicker ticker, Phaser globalPhaser, Phaser beginQuantaPhaser, Phaser endQuantaPhaser, int requiredPhases, int quantaTimeMillis)
        {
            this.ticker = ticker;
            this.globalPhaser = globalPhaser;
            this.beginQuantaPhaser = beginQuantaPhaser;
            this.endQuantaPhaser = endQuantaPhaser;
            this.requiredPhases = requiredPhases;
            this.quantaTimeMillis = quantaTimeMillis;

            beginQuantaPhaser.register();
            endQuantaPhaser.register();

            if (globalPhaser.getRegisteredParties() == 0) {
                globalPhaser.register();
            }
        }

        private int getFirstPhase()
        {
            return firstPhase.get();
        }

        private int getLastPhase()
        {
            return lastPhase.get();
        }

        private int getCompletedPhases()
        {
            return completedPhases.get();
        }

        @Override
        public ListenableFuture<?> processFor(Duration duration)
                throws Exception
        {
            ticker.increment(quantaTimeMillis, MILLISECONDS);
            globalPhaser.arriveAndAwaitAdvance();
            int phase = beginQuantaPhaser.arriveAndAwaitAdvance();
            firstPhase.compareAndSet(-1, phase - 1);
            lastPhase.set(phase);
            endQuantaPhaser.arriveAndAwaitAdvance();
            completedPhases.getAndIncrement();
            return Futures.immediateFuture(null);
        }

        @Override
        public String getInfo()
        {
            return "testing-split";
        }

        @Override
        public boolean isFinished()
        {
            boolean isFinished = completedPhases.get() >= requiredPhases;
            if (isFinished) {
                endQuantaPhaser.arriveAndDeregister();
                beginQuantaPhaser.arriveAndDeregister();
            }
            return isFinished;
        }

        @Override
        public void close()
        {
        }
    }
}
