package org.apache.oozie.service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.oozie.service.AsyncXCommandExecutor.AccessibleRunnableScheduledFuture;
import org.apache.oozie.service.AsyncXCommandExecutor.DelayedXCallable;
import org.apache.oozie.service.CallableQueueService.CallableWrapper;
import org.apache.oozie.util.XCallable;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.mockito.stubbing.OngoingStubbing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.times;
import static org.mockito.Matchers.same;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;

@RunWith(MockitoJUnitRunner.class)
public class TestAsyncXCommandExecutor {
    private static final int DEFAULT_MAX_ACTIVE_COMMANDS = 5;
    private static final boolean DEFAULT_ELIGIBILITY_CHECK = true;

    @Mock
    private ThreadPoolExecutor executor;

    @Mock
    private ScheduledThreadPoolExecutor scheduledExecutor;

    @Mock
    private CallableWrapper<?> callableWrapper;

    @Mock
    private CallableAccess callableAccess;

    private PriorityBlockingQueue<CallableWrapper<?>> priorityBlockingQueue;
    private BlockingQueue<AccessibleRunnableScheduledFuture<DelayedXCallable>> delayQueue;
    private List<CallableWrapper<?>> pendingCommands;
    private AtomicInteger activeCommands;
    private AsyncXCommandExecutor asyncExecutor;

    @Before
    public void setup() {
        activeCommands = new AtomicInteger(0);
        priorityBlockingQueue = new PriorityBlockingQueue<>();
        pendingCommands = new ArrayList<>();
        delayQueue = new LinkedBlockingQueue<>();  // in reality it's not LBQ, but it's fine here
        asyncExecutor = createExecutor(DEFAULT_ELIGIBILITY_CHECK, DEFAULT_MAX_ACTIVE_COMMANDS);
        when(callableWrapper.filterDuplicates()).thenReturn(true);
    }

    @Test
    public void testSubmitCallableWithNoDelay() {
        boolean result = asyncExecutor.queue(callableWrapper, false);

        verify(executor).execute(same(callableWrapper));
        verifyZeroInteractions(scheduledExecutor);
        assertEquals("Active commands", 1, asyncExecutor.getSize());
        assertTrue("Queuing result", result);
    }

    @Test
    public void testSubmitCallableWithDelay() {
        when(callableWrapper.getInitialDelay()).thenReturn(111L);
        when(callableWrapper.getDelay(eq(TimeUnit.MILLISECONDS))).thenReturn(222L);

        boolean result = asyncExecutor.queue(callableWrapper, false);

        verify(scheduledExecutor).schedule(any(DelayedXCallable.class), eq(222L), eq(TimeUnit.MILLISECONDS));
        verifyZeroInteractions(executor);
        assertEquals("Active commands", 1, asyncExecutor.getSize());
        assertTrue("Queuing result", result);
    }

    @Test
    public void testSubmissionSuccessfulAfterDelay() {
        when(callableWrapper.getInitialDelay()).thenReturn(100L);
        when(callableWrapper.getDelay(eq(TimeUnit.MILLISECONDS))).thenReturn(50L);
        when(callableAccess.handleMaxConcurrencyIfNeeded(eq(callableWrapper))).thenReturn(true);

        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                DelayedXCallable target = (DelayedXCallable) invocation.getArguments()[0];
                target.run();
                return null;
            }
        }).when(scheduledExecutor).schedule(any(DelayedXCallable.class), any(Long.class),
                any(TimeUnit.class));

        asyncExecutor.queue(callableWrapper, false);

        verify(scheduledExecutor).schedule(any(DelayedXCallable.class), eq(50L),
                eq(TimeUnit.MILLISECONDS));
        verify(executor).execute(callableWrapper);
    }

    @Test
    public void testSubmissionFailsAfterDelay() {
        when(callableWrapper.getInitialDelay()).thenReturn(100L);
        when(callableWrapper.getDelay(eq(TimeUnit.MILLISECONDS))).thenReturn(50L);
        when(callableAccess.handleMaxConcurrencyIfNeeded(eq(callableWrapper))).thenReturn(false);
        XCallable<?> wrappedCommand = mock(XCallable.class);
        Mockito.<XCallable<?>>when(callableWrapper.getElement()).thenReturn(wrappedCommand);

        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                DelayedXCallable target = (DelayedXCallable) invocation.getArguments()[0];
                target.run();
                return null;
            }
        }).when(scheduledExecutor).schedule(any(DelayedXCallable.class), any(Long.class),
                any(TimeUnit.class));

        asyncExecutor.queue(callableWrapper, false);

        verify(scheduledExecutor).schedule(any(DelayedXCallable.class), eq(50L),
                eq(TimeUnit.MILLISECONDS));
        verifyZeroInteractions(executor);
    }

    @Test
    public void testSubmissionSuccessfulAfterDelayWhenMaxConcurrencyCheckDisabled() {
        asyncExecutor = createExecutor(false, 2);
        when(callableWrapper.getInitialDelay()).thenReturn(100L);
        when(callableWrapper.getDelay(eq(TimeUnit.MILLISECONDS))).thenReturn(50L);
        when(callableAccess.handleMaxConcurrencyIfNeeded(eq(callableWrapper))).thenReturn(false);
        XCallable<?> wrappedCommand = mock(XCallable.class);
        Mockito.<XCallable<?>>when(callableWrapper.getElement()).thenReturn(wrappedCommand);


        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                DelayedXCallable target = (DelayedXCallable) invocation.getArguments()[0];
                target.run();
                return null;
            }
        }).when(scheduledExecutor).schedule(any(DelayedXCallable.class), any(Long.class),
                any(TimeUnit.class));

        asyncExecutor.queue(callableWrapper, false);

        verify(scheduledExecutor).schedule(any(DelayedXCallable.class), eq(50L),
                eq(TimeUnit.MILLISECONDS));
        verify(executor).execute(eq(callableWrapper));
    }

    @Test
    public void testCannotSubmitDueToFiltering() {
        when(callableWrapper.filterDuplicates()).thenReturn(false);

        boolean result = asyncExecutor.queue(callableWrapper, false);

        verifyZeroInteractions(scheduledExecutor);
        verifyZeroInteractions(executor);
        assertEquals("Active commands", 0, asyncExecutor.getSize());
        assertTrue("Queuing result", result);
    }

    @Test
    public void testExceptionThrownDuringSubmission() {
        doThrow(new RuntimeException("test")).when(executor).execute(any(Runnable.class));

        boolean exceptionThrown = false;
        try {
            asyncExecutor.queue(callableWrapper, false);
        } catch (RuntimeException e) {
            exceptionThrown = true;
        }

        assertTrue("Exception was not thrown", exceptionThrown);
        verify(callableWrapper).removeFromUniqueCallables();
        verifyZeroInteractions(scheduledExecutor);
    }

    @Test
    public void testSubmitWithNegativePriority() {
        testIllegalPriority(-1);
    }

    @Test
    public void testSubmitWithTooHighPriority() {
        testIllegalPriority(4);
    }

    @Test
    public void testQueueSizeWhenCommandIsFinished() {
        CallableWrapper<?> delayedCommand = mock(CallableWrapper.class);
        when(delayedCommand.getInitialDelay()).thenReturn(100L);
        when(delayedCommand.filterDuplicates()).thenReturn(true);

        asyncExecutor.queue(callableWrapper, false);
        asyncExecutor.queue(delayedCommand, false);
        asyncExecutor.commandFinished();
        asyncExecutor.commandFinished();

        assertEquals("Active commands", 0, asyncExecutor.getSize());
    }

    @Test
    public void testSubmissionWhenQueueIsFull() {
        asyncExecutor = createExecutor(true, 2);
        callableWrapper = mock(CallableWrapper.class, Mockito.RETURNS_DEEP_STUBS);
        when(callableWrapper.filterDuplicates()).thenReturn(true);
        when(callableWrapper.getElement().getKey()).thenReturn("key");

        asyncExecutor.queue(callableWrapper, false);
        asyncExecutor.queue(callableWrapper, false);
        boolean finalResult = asyncExecutor.queue(callableWrapper, false);

        assertFalse("Last submission shouldn't have succeeded", finalResult);
        verify(executor, times(2)).execute(same(callableWrapper));
    }

    @Test
    public void testSubmissionWhenQueueSizeIsIgnored() {
        asyncExecutor = createExecutor(true, 2);
        callableWrapper = mock(CallableWrapper.class, Mockito.RETURNS_DEEP_STUBS);
        when(callableWrapper.filterDuplicates()).thenReturn(true);
        when(callableWrapper.getElement().getKey()).thenReturn("key");

        asyncExecutor.queue(callableWrapper, false);
        asyncExecutor.queue(callableWrapper, false);
        boolean finalResult = asyncExecutor.queue(callableWrapper, true);

        assertTrue("Last submission should have succeeded", finalResult);
        verify(executor, times(3)).execute(same(callableWrapper));
    }

    @Test
    public void testPendingCommandSubmission() {
        XCallable<?> callable = mock(XCallable.class);
        Mockito.<XCallable<?>>when(callableWrapper.getElement()).thenReturn(callable);
        when(callableAccess.canSubmitCallable(eq(callable))).thenReturn(true);

        asyncExecutor.handleConcurrencyExceeded(callableWrapper);
        asyncExecutor.checkMaxConcurrency("type");

        verify(executor).execute(eq(callableWrapper));
        assertEquals("List of pending commands should be empty", 0, pendingCommands.size());
    }

    @Test
    public void testPendingCommandSubmissionWhenQueueIsFull() {
        XCallable<?> callable = mock(XCallable.class);
        Mockito.<XCallable<?>>when(callableWrapper.getElement()).thenReturn(callable);

        when(callableAccess.canSubmitCallable(eq(callable))).thenReturn(true);

        activeCommands.set(10);
        asyncExecutor.handleConcurrencyExceeded(callableWrapper);
        asyncExecutor.checkMaxConcurrency("type");

        verifyZeroInteractions(executor);
        assertEquals("List of pending commands should be empty", 0, pendingCommands.size());
    }

    @Test
    public void testPendingCommandSubmissionWhenMaxConcurrencyReached() {
        XCallable<?> callable = mock(XCallable.class);
        Mockito.<XCallable<?>>when(callableWrapper.getElement()).thenReturn(callable);
        when(callableAccess.canSubmitCallable(eq(callable))).thenReturn(false);

        asyncExecutor.handleConcurrencyExceeded(callableWrapper);
        asyncExecutor.checkMaxConcurrency("type");

        verifyZeroInteractions(executor);
        assertEquals("List of pending commands should not be empty", 1, pendingCommands.size());
    }

    @Test
    public void testQueueDump() {
        CallableWrapper<?> pendingCallable = mock(CallableWrapper.class);
        CallableWrapper<?> waitingCallable = mock(CallableWrapper.class);
        DelayedXCallable delayedXCallable = mock(DelayedXCallable.class);
        @SuppressWarnings("unchecked")
        AccessibleRunnableScheduledFuture<DelayedXCallable> asrf = mock(AccessibleRunnableScheduledFuture.class);
        Mockito.<CallableWrapper<?>>when(delayedXCallable.getCallableWrapper()).thenReturn(waitingCallable);
        when(asrf.getTask()).thenReturn(delayedXCallable);
        when(pendingCallable.toString()).thenReturn("pendingCallable");
        when(waitingCallable.toString()).thenReturn("waitingCallable");
        when(callableWrapper.toString()).thenReturn("callableWrapper");

        priorityBlockingQueue.add(callableWrapper);
        delayQueue.add(asrf);
        pendingCommands.add(pendingCallable);

        List<String> queueDump = asyncExecutor.getQueueDump();
        assertEquals("Size", 3, queueDump.size());
        assertTrue("PendingCallable not found", queueDump.contains("pendingCallable"));
        assertTrue("WaitingCallable not found", queueDump.contains("waitingCallable"));
        assertTrue("CallableWrapper not found", queueDump.contains("callableWrapper"));
    }

    private void testIllegalPriority(int prio) {
        when(callableWrapper.getPriority()).thenReturn(prio);

        boolean exceptionThrown = false;
        Throwable cause = null;
        try {
            asyncExecutor.queue(callableWrapper, false);
        } catch (RuntimeException e) {
            exceptionThrown = true;
            cause = e.getCause();
        }

        assertTrue("Exception was not thrown", exceptionThrown);
        verifyZeroInteractions(scheduledExecutor);
        verifyZeroInteractions(executor);
        assertTrue("Illegal exception", cause instanceof IllegalArgumentException);
        verify(callableWrapper).removeFromUniqueCallables();
    }

    private AsyncXCommandExecutor createExecutor(boolean needMaxConcurrencyCheck, int maxActiveCallables) {
        return new AsyncXCommandExecutor(needMaxConcurrencyCheck,
                callableAccess,
                maxActiveCallables,
                executor,
                scheduledExecutor,
                priorityBlockingQueue,
                delayQueue,
                pendingCommands,
                activeCommands);
    }
}
