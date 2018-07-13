package org.apache.oozie.service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.oozie.service.CallableQueueService.CallableWrapper;
import org.apache.oozie.util.NamedThreadFactory;
import org.apache.oozie.util.XCallable;
import org.apache.oozie.util.XLog;

import com.google.common.annotations.VisibleForTesting;

public class AsyncXCommandExecutor {
    private static XLog log = XLog.getLog(AsyncXCommandExecutor.class);
    private final ThreadPoolExecutor executor;
    private final ScheduledThreadPoolExecutor scheduledExecutor;
    private boolean needConcurrencyCheck;
    private final CallableAccess callableAccess;
    private List<CallableWrapper<?>> pendingCommands = new ArrayList<>();
    private AtomicInteger activeCommands = new AtomicInteger();
    private final long maxActiveCommands;  // equivalent of "queueSize" in CQS

    private final BlockingQueue<CallableWrapper<?>> priorityBlockingQueue;
    private final BlockingQueue<AccessibleRunnableScheduledFuture<DelayedXCallable>> delayWorkQueue;

    @SuppressWarnings("unchecked")
    public AsyncXCommandExecutor(int threads,
            boolean needEligibilityCheck,
            CallableAccess callableAccess,
            long maxActiveCommands) {

        priorityBlockingQueue = new PriorityBlockingQueue<CallableWrapper<?>>(100, new PriorityComparator());

        executor = new ThreadPoolExecutor(threads, threads, 10, TimeUnit.SECONDS,
                (BlockingQueue) priorityBlockingQueue,
                new NamedThreadFactory("CallableQueue")) {
            protected void beforeExecute(Thread t, Runnable r) {
                XLog.Info.get().clear();
            }

            protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
                return (RunnableFuture<T>)callable;
            }
        };

        this.scheduledExecutor = new ScheduledThreadPoolExecutor(4) {
            protected <V> RunnableScheduledFuture<V> decorateTask(
                    Runnable runnable, RunnableScheduledFuture<V> task) {

                    AccessibleRunnableScheduledFuture<V> arsf =
                            new AccessibleRunnableScheduledFuture<>(task, runnable);

                    return arsf;
            }
        };

        this.delayWorkQueue = (BlockingQueue) scheduledExecutor.getQueue();
        this.needConcurrencyCheck = needEligibilityCheck;
        this.callableAccess = callableAccess;
        this.maxActiveCommands = maxActiveCommands;
    }

    @VisibleForTesting
    AsyncXCommandExecutor(boolean needEligibilityCheck,
            CallableAccess callableAccess,
            long maxActiveCommands,
            ThreadPoolExecutor executor,
            ScheduledThreadPoolExecutor scheduledExecutor,
            PriorityBlockingQueue<CallableWrapper<?>> priorityBlockingQueue,
            BlockingQueue<AccessibleRunnableScheduledFuture<DelayedXCallable>> delayQueue,
            List<CallableWrapper<?>> pendingCommands,
            AtomicInteger activeCommands) {

        this.priorityBlockingQueue = priorityBlockingQueue;
        this.delayWorkQueue = delayQueue;
        this.pendingCommands = pendingCommands;
        this.executor = executor;
        this.scheduledExecutor = scheduledExecutor;
        this.needConcurrencyCheck = needEligibilityCheck;
        this.callableAccess = callableAccess;
        this.maxActiveCommands = maxActiveCommands;
        this.activeCommands = activeCommands;
    }

    public synchronized boolean queue(CallableWrapper<?> wrapper, boolean ignoreQueueSize) {
        if (!ignoreQueueSize && activeCommands.get() >= maxActiveCommands) {
            log.warn("queue full, ignoring queuing for [{0}]", wrapper.getElement().getKey());
            return false;
        }

        if (wrapper.filterDuplicates()) {
            wrapper.addToUniqueCallables();

            int priority = wrapper.getPriority();
            long delay = wrapper.getInitialDelay();

            try {
                if (priority >= 3 || priority < 0) {
                    throw new IllegalArgumentException("priority out of range: " + priority);
                }

                if (delay == 0) {
                    executor.execute(wrapper);
                } else {
                    DelayedXCallable delayedXCallable = new DelayedXCallable(wrapper);
                    long schedDelay = wrapper.getDelay(TimeUnit.MILLISECONDS);
                    scheduledExecutor.schedule(delayedXCallable,
                            schedDelay, TimeUnit.MILLISECONDS);
                }

                activeCommands.incrementAndGet();
            } catch (Throwable ree) {
                wrapper.removeFromUniqueCallables();
                throw new RuntimeException(ree);
            }
        }

        return true;
    }

    public void handleConcurrencyExceeded(CallableWrapper<?> command) {
        synchronized (pendingCommands) {
            pendingCommands.add(command);
        }
    }

    public void checkMaxConcurrency(String type) {
        // TODO: anti-starvation can be performed here
        synchronized (pendingCommands) {
            for (Iterator<CallableWrapper<?>> itr = pendingCommands.iterator(); itr.hasNext();) {
                CallableWrapper<?> command = itr.next();

                if (callableAccess.canSubmitCallable(command.getElement())) {
                    if (activeCommands.get() >= maxActiveCommands) {
                        log.warn("queue full, ignoring queuing for [{0}]", command.getElement().getKey());
                    } else {
                        executor.execute(command);
                    }

                    itr.remove();
                }
            }
        }
    }

    public void commandFinished() {
        // Note: this is to track the number of elements. Otherwise we'd have to combine the size of
        // two queues + a list.
        activeCommands.decrementAndGet();
    }

    public ThreadPoolExecutor getExecutorService() {
        return executor;
    }

    public class DelayedXCallable implements Runnable {
        private CallableWrapper<?> target;

        public DelayedXCallable(CallableWrapper<?> target) {
            this.target = target;
        }

        @Override
        public void run() {
            if (needConcurrencyCheck && !callableAccess.handleMaxConcurrencyIfNeeded(target)) {
                XCallable<?> callable = target.getElement();
                log.warn("Max concurrency reached for type = " + callable.getType());
            } else {
                executor.execute(target);
            }
        }

        public CallableWrapper<?> getCallableWrapper() {
            return target;
        }
    }

    public static class PriorityComparator implements Comparator<CallableWrapper<?>> {
        @Override
        public int compare(CallableWrapper<?> o1, CallableWrapper<?> o2) {
            return o2.getPriority() - o1.getPriority();
        }
    }

    // We have to use this so that scheduled elements in the DelayWorkQueue are accessible
    public static class AccessibleRunnableScheduledFuture<V> implements RunnableScheduledFuture<V> {
        private final Runnable task;
        private RunnableScheduledFuture<V> originalFuture;

        public AccessibleRunnableScheduledFuture(RunnableScheduledFuture<V> originalFuture,
                Runnable task) {
            this.task = task;
            this.originalFuture = originalFuture;
        }

        @Override
        public void run() {
            originalFuture.run();
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return originalFuture.cancel(mayInterruptIfRunning);
        }

        @Override
        public boolean isCancelled() {
            return originalFuture.isCancelled();
        }

        @Override
        public boolean isDone() {
            return originalFuture.isDone();
        }

        @Override
        public V get() throws InterruptedException, ExecutionException {
            return originalFuture.get();
        }

        @Override
        public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return originalFuture.get(timeout, unit);
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return originalFuture.getDelay(unit);
        }

        @Override
        public int compareTo(Delayed o) {
            return originalFuture.compareTo(o);
        }

        @Override
        public boolean isPeriodic() {
            return originalFuture.isPeriodic();
        }

        public Runnable getTask() {
            return task;
        }
    }

    public void shutdown() {
        scheduledExecutor.shutdown();
        executor.shutdown();
    }

    public List<String> getQueueDump() {
        List<CallableWrapper<?>> copyOfPending = new ArrayList<>(100);
        List<String> queueDump = new ArrayList<>(100);

        // must be copied to avoid possible ConcorrentModificationException during iteration
        synchronized (pendingCommands) {
            copyOfPending.addAll(pendingCommands);
        }

        // Safe to iterate
        for (final CallableWrapper<?> wrapper : priorityBlockingQueue) {
            queueDump.add(wrapper.toString());
        }

        // Safe to iterate
        for (final AccessibleRunnableScheduledFuture<DelayedXCallable> future : delayWorkQueue) {
            DelayedXCallable delayedXCallable = (DelayedXCallable) future.getTask();
            queueDump.add(delayedXCallable.getCallableWrapper().toString());
        }

        for (final CallableWrapper<?> wrapper : copyOfPending) {
            queueDump.add(wrapper.toString());
        }

        return queueDump;
    }

    public int getSize() {
        return activeCommands.get();
    }
}
