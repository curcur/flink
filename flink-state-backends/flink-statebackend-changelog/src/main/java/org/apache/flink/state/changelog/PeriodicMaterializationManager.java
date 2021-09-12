package org.apache.flink.state.changelog;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.FileSystemSafetyNet;
import org.apache.flink.runtime.mailbox.MailboxExecutor;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.changelog.SequenceNumber;
import org.apache.flink.runtime.taskmanager.AsyncExceptionHandler;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class PeriodicMaterializationManager {
    private static final Logger LOG = LoggerFactory.getLogger(PeriodicMaterializationManager.class);

    /** task mailbox executor, execute from Task Thread. */
    private final MailboxExecutor mailboxExecutor;

    /** Async thread pool, to complete async phase of materialization. */
    private final ExecutorService asyncOperationsThreadPool;

    /** scheduled executor, periodically trigger materialization. */
    private final ScheduledExecutorService periodicExecutor;

    private final AsyncExceptionHandler asyncExceptionHandler;

    private final String subtaskName;

    private final long periodicMaterializeDelay;

    /** Allowed number of consecutive materialization failures. */
    private final int allowedNumberOfFailures;

    /** Number of consecutive materialization failures. */
    private final AtomicInteger numberOfConsecutiveFailures;

    private final ChangelogKeyedStateBackend<?> keyedStateBackend;

    PeriodicMaterializationManager(
            MailboxExecutor mailboxExecutor,
            ExecutorService asyncOperationsThreadPool,
            String subtaskName,
            AsyncExceptionHandler asyncExceptionHandler,
            long periodicMaterializeDelay,
            int allowedNumberOfFailures,
            ChangelogKeyedStateBackend<?> keyedStateBackend) {
        this.mailboxExecutor = mailboxExecutor;
        this.asyncOperationsThreadPool = asyncOperationsThreadPool;

        this.subtaskName = subtaskName;
        this.periodicMaterializeDelay = periodicMaterializeDelay;
        this.asyncExceptionHandler = asyncExceptionHandler;
        this.allowedNumberOfFailures = allowedNumberOfFailures;
        this.numberOfConsecutiveFailures = new AtomicInteger(0);
        this.keyedStateBackend = keyedStateBackend;

        this.periodicExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        new ExecutorThreadFactory(
                                "periodic-materialization-scheduler-" + subtaskName));

        scheduleNextMaterialization();
    }

    @VisibleForTesting
    public void triggerMaterialization() {
        mailboxExecutor.execute(
                () -> {
                    keyedStateBackend
                            .initMaterialization()
                            .ifPresentOrElse(
                                    initMaterializationResult ->
                                            asyncOperationsThreadPool.execute(
                                                    () ->
                                                            asyncMaterializationPhase(
                                                                    initMaterializationResult.f0,
                                                                    initMaterializationResult.f1)),
                                    () -> {
                                        scheduleNextMaterialization();

                                        LOG.info(
                                                "Task {} has no state updates since last materialization, "
                                                        + "skip this one and schedule the next one in {} seconds",
                                                subtaskName,
                                                periodicMaterializeDelay / 1000);
                                    });
                },
                "materialization");
    }

    private void asyncMaterializationPhase(
            RunnableFuture<SnapshotResult<KeyedStateHandle>> materializedRunnableFuture,
            SequenceNumber upTo) {

        SnapshotResult<KeyedStateHandle> materializedSnapshot = null;
        FileSystemSafetyNet.initializeSafetyNetForThread();
        try {
            FutureUtils.runIfNotDoneAndGet(materializedRunnableFuture);

            LOG.debug("Task {} finishes asynchronous part of materialization.", subtaskName);

            materializedSnapshot = materializedRunnableFuture.get();

        } catch (Exception e) {
            int retryTime = numberOfConsecutiveFailures.incrementAndGet();

            LOG.info(
                    "Task {} asynchronous part of materialization is not completed for the {} time.",
                    subtaskName,
                    retryTime,
                    e);

            handleExecutionException(materializedRunnableFuture);

            if (retryTime >= allowedNumberOfFailures) {
                // Fail the task externally
                asyncExceptionHandler.handleAsyncException(
                        "Task "
                                + subtaskName
                                + " fails to complete the asynchronous part of materialization",
                        e);

                return;
            }
        } finally {
            FileSystemSafetyNet.closeSafetyNetAndGuardedResourcesForThread();
        }

        // if succeed, update state and finish up
        if (materializedSnapshot != null) {

            final SnapshotResult<KeyedStateHandle> copyMaterializedSnapshot = materializedSnapshot;

            mailboxExecutor.execute(
                    () -> {
                        keyedStateBackend.updateChangelogSnapshotState(
                                copyMaterializedSnapshot, upTo);
                        numberOfConsecutiveFailures.set(0);
                    },
                    "Task {} update materializedSnapshot up to changelog sequence number: {}",
                    subtaskName,
                    upTo);
        }

        mailboxExecutor.execute(
                this::scheduleNextMaterialization,
                "Task {} schedules the next materialization in {} seconds",
                subtaskName,
                periodicMaterializeDelay / 1000);
    }

    private void handleExecutionException(
            RunnableFuture<SnapshotResult<KeyedStateHandle>> materializedRunnableFuture) {

        LOG.info("Task {} cleanup asynchronous runnable for materialization.", subtaskName);

        if (materializedRunnableFuture != null) {
            // materialization has started
            if (!materializedRunnableFuture.cancel(true)) {
                try {
                    StateObject stateObject = materializedRunnableFuture.get();
                    if (stateObject != null) {
                        stateObject.discardState();
                    }
                } catch (Exception ex) {
                    LOG.debug(
                            "Task "
                                    + subtaskName
                                    + " cancelled execution of snapshot future runnable. "
                                    + "Cancellation produced the following "
                                    + "exception, which is expected and can be ignored.",
                            ex);
                }
            }
        }
    }

    // Only be called in the task thread to simplify the threading model
    private void scheduleNextMaterialization() {
        periodicExecutor.schedule(
                this::triggerMaterialization, periodicMaterializeDelay, TimeUnit.MILLISECONDS);
    }

    public void close() {
        if (!periodicExecutor.isShutdown()) {
            periodicExecutor.shutdownNow();
        }
    }
}
