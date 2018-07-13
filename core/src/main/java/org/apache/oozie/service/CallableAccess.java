package org.apache.oozie.service;

import org.apache.oozie.service.CallableQueueService.CallableWrapper;
import org.apache.oozie.util.XCallable;

public interface CallableAccess {
    boolean handleMaxConcurrencyIfNeeded(CallableWrapper<?> wrapper);

    boolean canSubmitCallable(XCallable<?> callable);
}
