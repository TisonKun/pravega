/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.logs;

import com.google.common.util.concurrent.Service;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.SequencedItemList;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentException;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.server.CachePolicy;
import io.pravega.segmentstore.server.ContainerOfflineException;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.MetadataBuilder;
import io.pravega.segmentstore.server.OperationLog;
import io.pravega.segmentstore.server.ReadIndex;
import io.pravega.segmentstore.server.ServiceListeners;
import io.pravega.segmentstore.server.TestDurableDataLog;
import io.pravega.segmentstore.server.TestDurableDataLogFactory;
import io.pravega.segmentstore.server.UpdateableContainerMetadata;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import io.pravega.segmentstore.server.containers.StreamSegmentContainerMetadata;
import io.pravega.segmentstore.server.logs.operations.CachedStreamSegmentAppendOperation;
import io.pravega.segmentstore.server.logs.operations.CheckpointOperationBase;
import io.pravega.segmentstore.server.logs.operations.MetadataCheckpointOperation;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.logs.operations.OperationComparer;
import io.pravega.segmentstore.server.logs.operations.StorageMetadataCheckpointOperation;
import io.pravega.segmentstore.server.logs.operations.StorageOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentAppendOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentMapOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentSealOperation;
import io.pravega.segmentstore.server.reading.ContainerReadIndex;
import io.pravega.segmentstore.server.reading.ReadIndexConfig;
import io.pravega.segmentstore.storage.CacheFactory;
import io.pravega.segmentstore.storage.DataLogDisabledException;
import io.pravega.segmentstore.storage.DataLogNotAvailableException;
import io.pravega.segmentstore.storage.DataLogWriterNotPrimaryException;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.LogAddress;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.mocks.InMemoryCacheFactory;
import io.pravega.segmentstore.storage.mocks.InMemoryDurableDataLogFactory;
import io.pravega.segmentstore.storage.mocks.InMemoryStorageFactory;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ErrorInjector;
import io.pravega.test.common.IntentionalException;
import io.pravega.test.common.TestUtils;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for DurableLog class.
 * Note: Some of the tests within this class are very similar to those of OperationProcessorTests. This is because the
 * DurableLog makes heavy use of that class and has the same semantics for the add/append methods. The main difference
 * is that the DurableLog sets up many of the components that the OperationProcessor requires, so we still need to do all
 * these tests.
 */
public class DurableLogTests extends OperationLogTestBase {
    private static final int CONTAINER_ID = 1234567;
    private static final int CHECKPOINT_MIN_COMMIT_COUNT = 10;
    private static final int START_RETRY_DELAY_MILLIS = 20;
    private static final int MAX_DATA_LOG_APPEND_SIZE = 8 * 1024;
    private static final int METADATA_CHECKPOINT_EVERY = 100;
    private static final int NO_METADATA_CHECKPOINT = 0;
    private static final ReadIndexConfig DEFAULT_READ_INDEX_CONFIG = ReadIndexConfig.builder().with(ReadIndexConfig.STORAGE_READ_ALIGNMENT, 1024).build();

    @Rule
    public Timeout globalTimeout = Timeout.seconds(TIMEOUT.getSeconds());

    //region Adding operations

    /**
     * Tests the ability of the DurableLog to process Operations in a failure-free environment.
     */
    @Test
    public void testAddWithNoFailures() throws Exception {
        int streamSegmentCount = 50;
        int transactionsPerStreamSegment = 2;
        int appendsPerStreamSegment = 20;
        boolean mergeTransactions = true;
        boolean sealStreamSegments = true;

        // Setup a DurableLog and start it.
        @Cleanup
        ContainerSetup setup = new ContainerSetup(executorService());
        @Cleanup
        DurableLog durableLog = setup.createDurableLog();
        durableLog.startAsync().awaitRunning();

        // Verify that on a freshly created DurableLog, it auto-adds a MetadataCheckpoint as the first operation.
        verifyFirstItemIsMetadataCheckpoint(durableLog.read(-1L, 1, TIMEOUT).join());

        // Generate some test data (we need to do this after we started the DurableLog because in the process of
        // recovery, it wipes away all existing metadata).
        HashSet<Long> streamSegmentIds = createStreamSegmentsInMetadata(streamSegmentCount, setup.metadata);
        AbstractMap<Long, Long> transactions = createTransactionsInMetadata(streamSegmentIds, transactionsPerStreamSegment, setup.metadata);
        List<Operation> operations = generateOperations(streamSegmentIds, transactions, appendsPerStreamSegment, METADATA_CHECKPOINT_EVERY, mergeTransactions, sealStreamSegments);

        // Process all generated operations.
        List<OperationWithCompletion> completionFutures = processOperations(operations, durableLog);

        // Wait for all such operations to complete. If any of them failed, this will fail too and report the exception.
        OperationWithCompletion.allOf(completionFutures).join();

        performLogOperationChecks(completionFutures, durableLog);
        performMetadataChecks(streamSegmentIds, new HashSet<>(), transactions, completionFutures, setup.metadata, mergeTransactions, sealStreamSegments);
        performReadIndexChecks(completionFutures, setup.readIndex);

        // Stop the processor.
        durableLog.stopAsync().awaitTerminated();
    }

    /**
     * Tests the ability of the DurableLog to process Operations when encountering invalid operations (such as
     * appends to StreamSegments that do not exist or to those that are sealed). This covers the following exceptions:
     * * StreamSegmentNotExistsException
     * * StreamSegmentSealedException
     * * General MetadataUpdateException.
     */
    @Test
    public void testAddWithInvalidOperations() throws Exception {
        int streamSegmentCount = 10;
        int appendsPerStreamSegment = 40;
        long sealedStreamSegmentId = 6; // We are going to prematurely seal this StreamSegment.
        long deletedStreamSegmentId = 8; // We are going to prematurely mark this StreamSegment as deleted.
        long nonExistentStreamSegmentId; // This is a bogus StreamSegment, that does not exist.

        // Setup a DurableLog and start it.
        @Cleanup
        ContainerSetup setup = new ContainerSetup(executorService());
        @Cleanup
        DurableLog durableLog = setup.createDurableLog();
        durableLog.startAsync().awaitRunning();

        // Generate some test data (we need to do this after we started the DurableLog because in the process of
        // recovery, it wipes away all existing metadata).
        HashSet<Long> streamSegmentIds = createStreamSegmentsInMetadata(streamSegmentCount, setup.metadata);
        nonExistentStreamSegmentId = streamSegmentIds.size();
        streamSegmentIds.add(nonExistentStreamSegmentId);
        setup.metadata.getStreamSegmentMetadata(sealedStreamSegmentId).markSealed();
        setup.metadata.getStreamSegmentMetadata(deletedStreamSegmentId).markDeleted();
        List<Operation> operations = generateOperations(streamSegmentIds, new HashMap<>(), appendsPerStreamSegment, METADATA_CHECKPOINT_EVERY, false, false);

        // Process all generated operations.
        List<OperationWithCompletion> completionFutures = processOperations(operations, durableLog);

        // Wait for all such operations to complete. We are expecting exceptions, so verify that we do.
        AssertExtensions.assertThrows(
                "No operations failed.",
                OperationWithCompletion.allOf(completionFutures)::join,
                ex -> ex instanceof MetadataUpdateException || ex instanceof StreamSegmentException);

        HashSet<Long> streamSegmentsWithNoContents = new HashSet<>();
        streamSegmentsWithNoContents.add(sealedStreamSegmentId);
        streamSegmentsWithNoContents.add(deletedStreamSegmentId);
        streamSegmentsWithNoContents.add(nonExistentStreamSegmentId);

        // Verify that the "right" operations failed, while the others succeeded.
        for (OperationWithCompletion oc : completionFutures) {
            if (oc.operation instanceof StorageOperation) {
                long streamSegmentId = ((StorageOperation) oc.operation).getStreamSegmentId();
                if (streamSegmentsWithNoContents.contains(streamSegmentId)) {
                    Assert.assertTrue("Completion future for invalid StreamSegment " + streamSegmentId + " did not complete exceptionally.", oc.completion.isCompletedExceptionally());
                    Predicate<Throwable> errorValidator;
                    if (streamSegmentId == sealedStreamSegmentId) {
                        errorValidator = ex -> ex instanceof StreamSegmentSealedException;
                    } else if (streamSegmentId == deletedStreamSegmentId) {
                        errorValidator = ex -> ex instanceof StreamSegmentNotExistsException;
                    } else {
                        errorValidator = ex -> ex instanceof MetadataUpdateException;
                    }

                    AssertExtensions.assertThrows("Unexpected exception for failed Operation.", oc.completion::join, errorValidator);
                    continue;
                }
            }

            // If we get here, we must verify no exception was thrown.
            oc.completion.join();
        }

        performLogOperationChecks(completionFutures, durableLog);
        performMetadataChecks(streamSegmentIds, streamSegmentsWithNoContents, new HashMap<>(), completionFutures, setup.metadata, false, false);
        performReadIndexChecks(completionFutures, setup.readIndex);

        // Stop the processor.
        durableLog.stopAsync().awaitTerminated();
    }

    /**
     * Tests the ability of the DurableLog to process Operations when Serialization errors happen.
     */
    @Test
    public void testAddWithOperationSerializationFailures() throws Exception {
        int streamSegmentCount = 10;
        int appendsPerStreamSegment = 80;
        int failAppendFrequency = 7; // Fail every X appends encountered.

        // Setup a DurableLog and start it.
        @Cleanup
        ContainerSetup setup = new ContainerSetup(executorService());
        @Cleanup
        DurableLog durableLog = setup.createDurableLog();
        durableLog.startAsync().awaitRunning();

        // Generate some test data (we need to do this after we started the DurableLog because in the process of
        // recovery, it wipes away all existing metadata).
        HashSet<Long> streamSegmentIds = createStreamSegmentsInMetadata(streamSegmentCount, setup.metadata);
        List<Operation> operations = generateOperations(streamSegmentIds, new HashMap<>(), appendsPerStreamSegment, METADATA_CHECKPOINT_EVERY, false, false);

        // Replace some of the Append Operations with a FailedAppendOperations. Some operations fail at the beginning,
        // some at the end of the serialization.
        int appendCount = 0;
        HashSet<Integer> failedOperationIndices = new HashSet<>();
        for (int i = 0; i < operations.size(); i++) {
            if (operations.get(i) instanceof StreamSegmentAppendOperation) {
                if ((appendCount++) % failAppendFrequency == 0) {
                    operations.set(i, new FailedStreamSegmentAppendOperation((StreamSegmentAppendOperation) operations.get(i)));
                    failedOperationIndices.add(i);
                }
            }
        }

        // Process all generated operations.
        List<OperationWithCompletion> completionFutures = processOperations(operations, durableLog);

        // Wait for all such operations to complete. We are expecting exceptions, so verify that we do.
        AssertExtensions.assertThrows(
                "No operations failed.",
                OperationWithCompletion.allOf(completionFutures)::join,
                ex -> ex instanceof IntentionalException);

        // Verify that the "right" operations failed, while the others succeeded.
        for (int i = 0; i < completionFutures.size(); i++) {
            OperationWithCompletion oc = completionFutures.get(i);
            if (failedOperationIndices.contains(i)) {
                AssertExtensions.assertThrows(
                        "Unexpected exception for failed Operation.",
                        oc.completion::join,
                        ex -> ex instanceof IntentionalException);
            } else {
                // Verify no exception was thrown.
                oc.completion.join();
            }
        }

        performLogOperationChecks(completionFutures, durableLog);
        performMetadataChecks(streamSegmentIds, new HashSet<>(), new HashMap<>(), completionFutures, setup.metadata, false, false);
        performReadIndexChecks(completionFutures, setup.readIndex);

        // Stop the processor.
        durableLog.stopAsync().awaitTerminated();
    }

    /**
     * Tests the ability of the DurableLog to process Operations when there are DataLog write failures.
     */
    @Test
    public void testAddWithDataLogFailures() throws Exception {
        int streamSegmentCount = 10;
        int appendsPerStreamSegment = 80;
        int failAsyncAfter = 5; // Fail (asynchronously) after X DataFrame commits (to DataLog).

        // Setup a DurableLog and start it.
        @Cleanup
        ContainerSetup setup = new ContainerSetup(executorService());
        @Cleanup
        DurableLog durableLog = setup.createDurableLog();
        durableLog.startAsync().awaitRunning();

        Assert.assertNotNull("Internal error: could not grab a pointer to the created TestDurableDataLog.", setup.dataLog.get());

        // Generate some test data (we need to do this after we started the DurableLog because in the process of
        // recovery, it wipes away all existing metadata).
        HashSet<Long> streamSegmentIds = createStreamSegmentsInMetadata(streamSegmentCount, setup.metadata);

        List<Operation> operations = generateOperations(streamSegmentIds, new HashMap<>(), appendsPerStreamSegment, METADATA_CHECKPOINT_EVERY, false, false);
        ErrorInjector<Exception> aSyncErrorInjector = new ErrorInjector<>(
                count -> count >= failAsyncAfter,
                () -> new DurableDataLogException("intentional"));
        setup.dataLog.get().setAppendErrorInjectors(null, aSyncErrorInjector);

        // Process all generated operations.
        List<OperationWithCompletion> completionFutures = processOperations(operations, durableLog);

        // Wait for all such operations to complete. We are expecting exceptions, so verify that we do.
        AssertExtensions.assertThrows(
                "No operations failed.",
                OperationWithCompletion.allOf(completionFutures)::join,
                super::isExpectedExceptionForNonDataCorruption);

        // Wait for the DurableLog to shutdown with failure.
        ServiceListeners.awaitShutdown(durableLog, TIMEOUT, false);
        Assert.assertEquals("Expected the DurableLog to fail after DurableDataLogException encountered.",
                Service.State.FAILED, durableLog.state());

        // We can't really check the DurableLog or the DurableDataLog contents since they are both closed.
        performMetadataChecks(streamSegmentIds, new HashSet<>(), new HashMap<>(), completionFutures, setup.metadata, false, false);
        performReadIndexChecks(completionFutures, setup.readIndex);
    }

    /**
     * Tests the ability of the DurableLog to handle a DataLogWriterNotPrimaryException.
     */
    @Test
    public void testAddWithDataLogWriterNotPrimaryException() throws Exception {
        int streamSegmentCount = 1;
        int appendsPerStreamSegment = 1;

        // Setup a DurableLog and start it.
        @Cleanup
        ContainerSetup setup = new ContainerSetup(executorService());
        @Cleanup
        DurableLog durableLog = setup.createDurableLog();
        durableLog.startAsync().awaitRunning();
        HashSet<Long> streamSegmentIds = createStreamSegmentsInMetadata(streamSegmentCount, setup.metadata);
        List<Operation> operations = generateOperations(streamSegmentIds, new HashMap<>(), appendsPerStreamSegment, METADATA_CHECKPOINT_EVERY, false, false);
        ErrorInjector<Exception> aSyncErrorInjector = new ErrorInjector<>(
                count -> true,
                () -> new CompletionException(new DataLogWriterNotPrimaryException("intentional")));
        setup.dataLog.get().setAppendErrorInjectors(null, aSyncErrorInjector);

        // Process all generated operations.
        List<OperationWithCompletion> completionFutures = processOperations(operations, durableLog);

        // Wait for all such operations to complete. We are expecting exceptions, so verify that we do.
        AssertExtensions.assertThrows(
                "No operations failed.",
                OperationWithCompletion.allOf(completionFutures)::join,
                ex -> ex instanceof IOException || ex instanceof DataLogWriterNotPrimaryException);

        // Verify that the OperationProcessor automatically shuts down and that it has the right failure cause.
        ServiceListeners.awaitShutdown(durableLog, TIMEOUT, false);
        Assert.assertEquals("DurableLog is not in a failed state after fence-out detected.",
                Service.State.FAILED, durableLog.state());
        Assert.assertTrue("DurableLog did not fail with the correct exception.",
                Exceptions.unwrap(durableLog.failureCause()) instanceof DataLogWriterNotPrimaryException);
    }

    /**
     * Tests the ability of the DurableLog to process Operations when a simulated DataCorruptionException
     * is generated.
     */
    @Test
    public void testAddWithDataCorruptionFailures() throws Exception {
        int streamSegmentCount = 10;
        int appendsPerStreamSegment = 80;
        int failAtOperationIndex = 123;

        // Setup a DurableLog and start it.
        @Cleanup
        ContainerSetup setup = new ContainerSetup(executorService());
        DurableLogConfig config = setup.durableLogConfig == null ? ContainerSetup.defaultDurableLogConfig() : setup.durableLogConfig;
        CorruptedDurableLog.FAIL_AT_INDEX.set(failAtOperationIndex);
        val durableLog = new CorruptedDurableLog(config, setup);
        durableLog.startAsync().awaitRunning();

        Assert.assertNotNull("Internal error: could not grab a pointer to the created TestDurableDataLog.", setup.dataLog.get());

        // Generate some test data (we need to do this after we started the DurableLog because in the process of
        // recovery, it wipes away all existing metadata).
        HashSet<Long> streamSegmentIds = createStreamSegmentsInMetadata(streamSegmentCount, setup.metadata);

        List<Operation> operations = generateOperations(streamSegmentIds, new HashMap<>(), appendsPerStreamSegment, METADATA_CHECKPOINT_EVERY, false, false);

        // Process all generated operations.
        List<OperationWithCompletion> completionFutures = processOperations(operations, durableLog);

        // Wait for the service to fail (and make sure it failed).
        AssertExtensions.assertThrows(
                "DurableLog did not shut down with failure.",
                () -> ServiceListeners.awaitShutdown(durableLog, true),
                ex -> ex instanceof IllegalStateException);

        Assert.assertEquals("Unexpected service state after encountering DataCorruptionException.", Service.State.FAILED, durableLog.state());

        // Verify that the "right" operations failed, while the others succeeded.
        int successCount = 0;
        boolean encounteredFirstFailure = false;
        for (int i = 0; i < completionFutures.size(); i++) {
            OperationWithCompletion oc = completionFutures.get(i);

            // Once an operation failed (in our scenario), no other operation can succeed.
            if (encounteredFirstFailure) {
                Assert.assertTrue("Encountered successful operation after a failed operation.", oc.completion.isCompletedExceptionally());
            }

            // The operation that failed may have inadvertently failed other operations that were aggregated together
            // with it, which is why it's hard to determine precisely what the first expected failed operation is.
            if (oc.completion.isCompletedExceptionally()) {
                // If we do find a failed one in this area, make sure it is failed with DataCorruptionException.
                AssertExtensions.assertThrows(
                        "Unexpected exception for failed Operation.",
                        oc.completion::join,
                        super::isExpectedExceptionForDataCorruption);
                encounteredFirstFailure = true;
            } else {
                successCount++;
            }
        }

        AssertExtensions.assertGreaterThan("No operation succeeded.", 0, successCount);

        // There is no point in performing any other checks. A DataCorruptionException means the Metadata (and the general
        // state of the Container) is in an undefined state.
    }

    /**
     * Tests the ability to block reads if the read is at the tail and no more data is available (for now).
     */
    @Test
    public void testTailReads() throws Exception {
        final int operationCount = 10;
        final long segmentId = 1;
        final String segmentName = Long.toString(segmentId);

        // Setup a DurableLog and start it.
        @Cleanup
        ContainerSetup setup = new ContainerSetup(executorService());
        @Cleanup
        DurableLog durableLog = setup.createDurableLog();
        durableLog.startAsync().awaitRunning();

        // Create a segment, which will be used for testing later.
        UpdateableSegmentMetadata segmentMetadata = setup.metadata.mapStreamSegmentId(segmentName, segmentId);
        segmentMetadata.setLength(0);
        segmentMetadata.setStorageLength(0);

        // Setup a bunch of read operations, and make sure they are blocked (since there is no data).
        ArrayList<CompletableFuture<Iterator<Operation>>> readFutures = new ArrayList<>();
        for (int i = 0; i < operationCount; i++) {
            long afterSeqNo = i + 1;
            CompletableFuture<Iterator<Operation>> readFuture = durableLog.read(afterSeqNo, operationCount, TIMEOUT);
            Assert.assertFalse("read() returned a completed future when there is no data available (afterSeqNo = " + afterSeqNo + ").", readFuture.isDone());
            readFutures.add(readFuture);
        }

        // Add one operation at at time, and each time, verify that the correct Read got activated.
        OperationComparer operationComparer = new OperationComparer(true);
        for (int appendId = 0; appendId < operationCount; appendId++) {
            Operation operation = new StreamSegmentAppendOperation(segmentId, ("foo" + Integer.toString(appendId)).getBytes(), null);
            durableLog.add(operation, TIMEOUT).join();
            for (int readId = 0; readId < readFutures.size(); readId++) {
                val readFuture = readFutures.get(readId);
                boolean expectedComplete = readId <= appendId;
                if (expectedComplete) {
                    // The internal callback happens asynchronously, so wait for this future to complete in a bit.
                    readFuture.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                }

                Assert.assertEquals(
                        String.format("Unexpected read completion status for read after seqNo %d after adding op with seqNo %d", readId + 1, operation.getSequenceNumber()),
                        expectedComplete,
                        readFutures.get(readId).isDone());

                if (appendId == readId) {
                    // Verify that the read result matches the operation.
                    Iterator<Operation> readResult = readFuture.join();

                    // Verify that we actually have a non-empty read result.
                    Assert.assertTrue(
                            String.format("Empty read result read after seqNo %d after adding op with seqNo %d", readId + 1, operation.getSequenceNumber()),
                            readResult.hasNext());

                    // Verify the read result.
                    Operation readOp = readResult.next();
                    operationComparer.assertEquals(
                            String.format("Unexpected result operation for read after seqNo %d after adding op with seqNo %d", readId + 1, operation.getSequenceNumber()),
                            operation,
                            readOp);

                    // Verify that we don't have more than one read result.
                    Assert.assertFalse(
                            String.format("Not expecting more than one result for read after seqNo %d after adding op with seqNo %d", readId + 1, operation.getSequenceNumber()),
                            readResult.hasNext());
                }
            }
        }

        // Verify that such reads are cancelled when the DurableLog is closed.
        CompletableFuture<Iterator<Operation>> readFuture = durableLog.read(operationCount + 2, operationCount, TIMEOUT);
        Assert.assertFalse("read() returned a completed future when there is no data available (afterSeqNo = MAX).", readFuture.isDone());
        durableLog.stopAsync().awaitTerminated();
        Assert.assertTrue("A tail read was not cancelled when the DurableLog was stopped.", readFuture.isCompletedExceptionally());
    }

    /**
     * Tests the ability to timeout tail reads. This does not actually test the functionality of tail reads - it just
     * tests that they will time out appropriately.
     */
    @Test
    public void testTailReadsTimeout() {
        final long segmentId = 1;
        final String segmentName = Long.toString(segmentId);

        // Setup a DurableLog and start it.
        @Cleanup
        ContainerSetup setup = new ContainerSetup(executorService());
        @Cleanup
        DurableLog durableLog = setup.createDurableLog();
        durableLog.startAsync().awaitRunning();

        // Create a segment, which will be used for testing later.
        UpdateableSegmentMetadata segmentMetadata = setup.metadata.mapStreamSegmentId(segmentName, segmentId);
        segmentMetadata.setLength(0);

        Duration shortTimeout = Duration.ofMillis(30);

        // Setup a read operation, and make sure it is blocked (since there is no data).
        CompletableFuture<Iterator<Operation>> readFuture = durableLog.read(1, 1, shortTimeout);
        Assert.assertFalse("read() returned a completed future when there is no data available.", Futures.isSuccessful(readFuture));

        CompletableFuture<Void> controlFuture = Futures.delayedFuture(Duration.ofMillis(2000), setup.executorService);
        AssertExtensions.assertSuppliedFutureThrows(
                "Future from read() operation did not fail with a TimeoutException after the timeout expired.",
                () -> CompletableFuture.anyOf(controlFuture, readFuture),
                ex -> ex instanceof TimeoutException);
    }

    /**
     * Tests the ability of the DurableLog to add MetadataCheckpointOperations triggered by the number of operations processed.
     */
    @Test
    public void testMetadataCheckpointByCount() throws Exception {
        int checkpointEvery = 30;
        testMetadataCheckpoint(
                () -> ContainerSetup.createDurableLogConfig(checkpointEvery, null),
                checkpointEvery);
    }

    /**
     * Tests the ability of the DurableLog to add MetadataCheckpointOperations triggered by the length of the operations processed.
     */
    @Test
    public void testMetadataCheckpointByLength() throws Exception {
        int checkpointLengthThreshold = 257 * 1024;
        testMetadataCheckpoint(
                () -> ContainerSetup.createDurableLogConfig(null, (long) checkpointLengthThreshold),
                10);
    }

    /**
     * Tests the ability of the DurableLog to add MetadataCheckpointOperations.
     *
     * @param createDurableLogConfig     A Supplier that creates a DurableLogConfig object.
     * @param waitForProcessingFrequency The frequency at which to stop and wait for operations to be processed by the
     *                                   DurableLog before adding others.
     */
    private void testMetadataCheckpoint(Supplier<DurableLogConfig> createDurableLogConfig, int waitForProcessingFrequency) throws Exception {
        int streamSegmentCount = 500;
        int appendsPerStreamSegment = 20;

        // Setup a DurableLog and start it.
        @Cleanup
        ContainerSetup setup = new ContainerSetup(executorService());
        DurableLogConfig durableLogConfig = createDurableLogConfig.get();
        setup.setDurableLogConfig(durableLogConfig);

        @Cleanup
        DurableLog durableLog = setup.createDurableLog();
        durableLog.startAsync().awaitRunning();

        // Verify that on a freshly created DurableLog, it auto-adds a MetadataCheckpoint as the first operation.
        verifyFirstItemIsMetadataCheckpoint(durableLog.read(-1L, 1, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));

        // Generate some test data (we need to do this after we started the DurableLog because in the process of
        // recovery, it wipes away all existing metadata).
        HashSet<Long> streamSegmentIds = createStreamSegmentsInMetadata(streamSegmentCount, setup.metadata);
        List<Operation> operations = generateOperations(streamSegmentIds, Collections.emptyMap(), appendsPerStreamSegment, NO_METADATA_CHECKPOINT, false, false);

        // Process all generated operations.
        List<OperationWithCompletion> completionFutures = processOperations(operations, durableLog, waitForProcessingFrequency);

        // Wait for all such operations to complete. If any of them failed, this will fail too and report the exception.
        OperationWithCompletion.allOf(completionFutures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        List<Operation> readOperations = readUpToSequenceNumber(durableLog, setup.metadata.getOperationSequenceNumber());

        // Count the number of injected MetadataCheckpointOperations.
        int injectedOperationCount = 0;
        for (Operation o : readOperations) {
            if (o instanceof MetadataCheckpointOperation) {
                injectedOperationCount++;
            }
        }

        // Calculate how many we were expecting.
        int expectedCheckpoints = readOperations.size() - operations.size();

        if (expectedCheckpoints != injectedOperationCount) {
            Assert.assertEquals("Unexpected operations were injected. Expected only MetadataCheckpointOperations.",
                    expectedCheckpoints, injectedOperationCount);
        }

        // We expect at least 2 injected operations (one is the very first one (checked above), and then at least
        // one more based on written data.
        AssertExtensions.assertGreaterThan("Insufficient number of injected operations.", 1, injectedOperationCount);

        // Stop the processor.
        durableLog.stopAsync().awaitTerminated();
    }

    //endregion

    //region Recovery

    /**
     * Tests the DurableLog recovery process in a scenario when there are no failures during the process.
     */
    @Test
    public void testRecoveryWithNoFailures() throws Exception {
        int streamSegmentCount = 50;
        int transactionsPerStreamSegment = 2;
        int appendsPerStreamSegment = 20;
        boolean mergeTransactions = true;
        boolean sealStreamSegments = true;

        // Setup a DurableLog and start it.
        @Cleanup
        TestDurableDataLogFactory dataLogFactory = new TestDurableDataLogFactory(new InMemoryDurableDataLogFactory(MAX_DATA_LOG_APPEND_SIZE, executorService()));
        @Cleanup
        Storage storage = InMemoryStorageFactory.newStorage(executorService());
        storage.initialize(1);

        Set<Long> streamSegmentIds;
        AbstractMap<Long, Long> transactions;
        List<OperationWithCompletion> completionFutures;
        List<Operation> originalOperations;

        // First DurableLog. We use this for generating data.
        UpdateableContainerMetadata metadata = new MetadataBuilder(CONTAINER_ID).build();
        @Cleanup
        InMemoryCacheFactory cacheFactory = new InMemoryCacheFactory();
        @Cleanup
        CacheManager cacheManager = new CacheManager(CachePolicy.INFINITE, executorService());
        try (
                ReadIndex readIndex = new ContainerReadIndex(DEFAULT_READ_INDEX_CONFIG, metadata, cacheFactory, storage, cacheManager, executorService());
                DurableLog durableLog = new DurableLog(ContainerSetup.defaultDurableLogConfig(), metadata, dataLogFactory, readIndex, executorService())) {
            durableLog.startAsync().awaitRunning();

            // Generate some test data (we need to do this after we started the DurableLog because in the process of
            // recovery, it wipes away all existing metadata).
            streamSegmentIds = createStreamSegmentsWithOperations(streamSegmentCount, durableLog);
            transactions = createTransactionsWithOperations(streamSegmentIds, transactionsPerStreamSegment, metadata, durableLog);
            List<Operation> operations = generateOperations(streamSegmentIds, transactions, appendsPerStreamSegment, METADATA_CHECKPOINT_EVERY, mergeTransactions, sealStreamSegments);

            // Process all generated operations and wait for them to complete
            completionFutures = processOperations(operations, durableLog);
            OperationWithCompletion.allOf(completionFutures).join();

            // Get a list of all the operations, before recovery.
            originalOperations = readUpToSequenceNumber(durableLog, metadata.getOperationSequenceNumber());

            // Stop the processor.
            durableLog.stopAsync().awaitTerminated();
        }

        // Second DurableLog. We use this for recovery.
        metadata = new MetadataBuilder(CONTAINER_ID).build();
        try (
                ReadIndex readIndex = new ContainerReadIndex(DEFAULT_READ_INDEX_CONFIG, metadata, cacheFactory, storage, cacheManager, executorService());
                DurableLog durableLog = new DurableLog(ContainerSetup.defaultDurableLogConfig(), metadata, dataLogFactory, readIndex, executorService())) {
            durableLog.startAsync().awaitRunning();

            List<Operation> recoveredOperations = readUpToSequenceNumber(durableLog, metadata.getOperationSequenceNumber());
            assertRecoveredOperationsMatch(originalOperations, recoveredOperations);
            performMetadataChecks(streamSegmentIds, new HashSet<>(), transactions, completionFutures, metadata, mergeTransactions, sealStreamSegments);
            performReadIndexChecks(completionFutures, readIndex);

            // Stop the processor.
            durableLog.stopAsync().awaitTerminated();
        }
    }

    /**
     * Tests the DurableLog recovery process in a scenario when there are failures during the process
     * (these may or may not be DataCorruptionExceptions).
     */
    @Test
    public void testRecoveryFailures() throws Exception {
        int streamSegmentCount = 50;
        int appendsPerStreamSegment = 20;
        int failReadAfter = 2; // Fail DataLog reads after X reads.

        // Setup a DurableLog and start it.
        AtomicReference<TestDurableDataLog> dataLog = new AtomicReference<>();
        @Cleanup
        TestDurableDataLogFactory dataLogFactory = new TestDurableDataLogFactory(new InMemoryDurableDataLogFactory(MAX_DATA_LOG_APPEND_SIZE, executorService()), dataLog::set);
        @Cleanup
        Storage storage = InMemoryStorageFactory.newStorage(executorService());
        storage.initialize(1);

        Set<Long> streamSegmentIds;
        List<OperationWithCompletion> completionFutures;

        // First DurableLog. We use this for generating data.
        UpdateableContainerMetadata metadata = new MetadataBuilder(CONTAINER_ID).build();
        @Cleanup
        InMemoryCacheFactory cacheFactory = new InMemoryCacheFactory();
        @Cleanup
        CacheManager cacheManager = new CacheManager(CachePolicy.INFINITE, executorService());
        try (ReadIndex readIndex = new ContainerReadIndex(DEFAULT_READ_INDEX_CONFIG, metadata, cacheFactory, storage, cacheManager, executorService());
             DurableLog durableLog = new DurableLog(ContainerSetup.defaultDurableLogConfig(), metadata, dataLogFactory, readIndex, executorService())) {
            durableLog.startAsync().awaitRunning();

            // Generate some test data (we need to do this after we started the DurableLog because in the process of
            // recovery, it wipes away all existing metadata).
            streamSegmentIds = createStreamSegmentsWithOperations(streamSegmentCount, durableLog);
            List<Operation> operations = generateOperations(streamSegmentIds, new HashMap<>(), appendsPerStreamSegment, METADATA_CHECKPOINT_EVERY, false, false);

            // Process all generated operations and wait for them to complete
            completionFutures = processOperations(operations, durableLog);
            OperationWithCompletion.allOf(completionFutures).join();

            // Stop the processor.
            durableLog.stopAsync().awaitTerminated();
        }

        //Recovery failure due to DataLog Failures.
        metadata = new MetadataBuilder(CONTAINER_ID).build();
        dataLog.set(null);
        try (ReadIndex readIndex = new ContainerReadIndex(DEFAULT_READ_INDEX_CONFIG, metadata, cacheFactory, storage, cacheManager, executorService());
             DurableLog durableLog = new DurableLog(ContainerSetup.defaultDurableLogConfig(), metadata, dataLogFactory, readIndex, executorService())) {

            // Inject some artificial error into the DataLogRead after a few reads.
            ErrorInjector<Exception> readNextInjector = new ErrorInjector<>(
                    count -> count > failReadAfter,
                    () -> new DataLogNotAvailableException("intentional"));
            dataLog.get().setReadErrorInjectors(null, readNextInjector);

            // Verify the exception thrown from startAsync() is of the right kind. This exception will be wrapped in
            // multiple layers, so we need to dig deep into it.
            AssertExtensions.assertThrows(
                    "Recovery did not fail properly when expecting DurableDataLogException.",
                    () -> durableLog.startAsync().awaitRunning(),
                    ex -> {
                        if (ex instanceof IllegalStateException) {
                            ex = ex.getCause();
                        }

                        if (ex == null) {
                            try {
                                durableLog.awaitTerminated(); // We need this to enter a FAILED state to get its failure cause.
                            } catch (Exception ex2) {
                                ex = durableLog.failureCause();
                            }
                        }

                        ex = Exceptions.unwrap(ex);
                        return ex instanceof DataLogNotAvailableException && ex.getMessage().equals("intentional");
                    });
        }

        // Recovery failure due to DataCorruptionException.
        metadata = new MetadataBuilder(CONTAINER_ID).build();
        dataLog.set(null);
        try (ReadIndex readIndex = new ContainerReadIndex(DEFAULT_READ_INDEX_CONFIG, metadata, cacheFactory, storage, cacheManager, executorService());
             DurableLog durableLog = new DurableLog(ContainerSetup.defaultDurableLogConfig(), metadata, dataLogFactory, readIndex, executorService())) {

            // Reset error injectors to nothing.
            dataLog.get().setReadErrorInjectors(null, null);
            AtomicInteger readCounter = new AtomicInteger();
            dataLog.get().setReadInterceptor(
                    readItem -> {
                        if (readCounter.incrementAndGet() > failReadAfter && readItem.getLength() > DataFrame.MIN_ENTRY_LENGTH_NEEDED) {
                            // Mangle with the payload and overwrite its contents with a DataFrame having a bogus
                            // previous sequence number.
                            DataFrame df = DataFrame.ofSize(readItem.getLength());
                            df.seal();
                            ArrayView serialization = df.getData();
                            return new InjectedReadItem(serialization.getReader(), serialization.getLength(), readItem.getAddress());
                        }

                        return readItem;
                    }
            );

            // Verify the exception thrown from startAsync() is of the right kind. This exception will be wrapped in
            // multiple layers, so we need to dig deep into it.
            AssertExtensions.assertThrows(
                    "Recovery did not fail properly when expecting DataCorruptionException.",
                    () -> durableLog.startAsync().awaitRunning(),
                    ex -> {
                        if (ex instanceof IllegalStateException) {
                            ex = ex.getCause();
                        }

                        return Exceptions.unwrap(ex) instanceof DataCorruptionException;
                    });

            // Verify that the underlying DurableDataLog has been disabled.
            val disabledDataLog = dataLogFactory.createDurableDataLog(CONTAINER_ID);
            AssertExtensions.assertThrows(
                    "DurableDataLog has not been disabled following a recovery failure with DataCorruptionException.",
                    () -> disabledDataLog.initialize(TIMEOUT),
                    ex -> ex instanceof DataLogDisabledException);
        }
    }

    /**
     * Verifies the ability of hte DurableLog to recover (delayed start) using a disabled DurableDataLog. This verifies
     * the ability to shut down correctly while still waiting for the DataLog to become enabled as well as detecting that
     * it did become enabled and then resume normal operations.
     */
    @Test
    public void testRecoveryWithDisabledDataLog() throws Exception {
        int streamSegmentCount = 50;
        int appendsPerStreamSegment = 20;
        AtomicReference<TestDurableDataLog> dataLog = new AtomicReference<>();
        @Cleanup
        TestDurableDataLogFactory dataLogFactory = new TestDurableDataLogFactory(new InMemoryDurableDataLogFactory(MAX_DATA_LOG_APPEND_SIZE, executorService()), dataLog::set);
        @Cleanup
        Storage storage = InMemoryStorageFactory.newStorage(executorService());
        storage.initialize(1);

        @Cleanup
        InMemoryCacheFactory cacheFactory = new InMemoryCacheFactory();
        @Cleanup
        CacheManager cacheManager = new CacheManager(CachePolicy.INFINITE, executorService());

        // Write some data to the log. We'll read it later.
        Set<Long> streamSegmentIds;
        List<Operation> originalOperations;
        List<OperationWithCompletion> completionFutures;
        UpdateableContainerMetadata metadata = new MetadataBuilder(CONTAINER_ID).build();
        dataLog.set(null);
        try (ReadIndex readIndex = new ContainerReadIndex(DEFAULT_READ_INDEX_CONFIG, metadata, cacheFactory, storage, cacheManager, executorService());
             DurableLog durableLog = new DurableLog(ContainerSetup.defaultDurableLogConfig(), metadata, dataLogFactory, readIndex, executorService())) {

            // DurableLog should start properly.
            durableLog.startAsync().awaitRunning();
            streamSegmentIds = createStreamSegmentsWithOperations(streamSegmentCount, durableLog);
            List<Operation> operations = generateOperations(streamSegmentIds, new HashMap<>(), appendsPerStreamSegment, METADATA_CHECKPOINT_EVERY, false, false);
            completionFutures = processOperations(operations, durableLog);
            OperationWithCompletion.allOf(completionFutures).join();
            originalOperations = readUpToSequenceNumber(durableLog, metadata.getOperationSequenceNumber());
        }

        // Disable the DurableDataLog. This requires us to initialize the log, then disable it.
        metadata = new MetadataBuilder(CONTAINER_ID).build();
        dataLog.set(null);
        try (ReadIndex readIndex = new ContainerReadIndex(DEFAULT_READ_INDEX_CONFIG, metadata, cacheFactory, storage, cacheManager, executorService());
             DurableLog durableLog = new DurableLog(ContainerSetup.defaultDurableLogConfig(), metadata, dataLogFactory, readIndex, executorService())) {

            // DurableLog should start properly.
            durableLog.startAsync().awaitRunning();

            CompletableFuture<Void> online = durableLog.awaitOnline();
            Assert.assertTrue("awaitOnline() returned an incomplete future.", Futures.isSuccessful(online));
            Assert.assertFalse("Not expecting an offline DurableLog.", durableLog.isOffline());

            dataLog.get().disable();
        }

        // Verify that the DurableLog starts properly and that all operations throw appropriate exceptions.
        try (ReadIndex readIndex = new ContainerReadIndex(DEFAULT_READ_INDEX_CONFIG, metadata, cacheFactory, storage, cacheManager, executorService());
             DurableLog durableLog = new DurableLog(ContainerSetup.defaultDurableLogConfig(), metadata, dataLogFactory, readIndex, executorService())) {

            // DurableLog should start properly.
            durableLog.startAsync().awaitRunning();

            CompletableFuture<Void> online = durableLog.awaitOnline();
            Assert.assertFalse("awaitOnline() returned a completed future.", online.isDone());
            Assert.assertTrue("Expecting an offline DurableLog.", durableLog.isOffline());

            // Verify all operations fail with the right exception.
            AssertExtensions.assertSuppliedFutureThrows(
                    "add() did not fail with the right exception when offline.",
                    () -> durableLog.add(new StreamSegmentSealOperation(123), TIMEOUT),
                    ex -> ex instanceof ContainerOfflineException);
            AssertExtensions.assertSuppliedFutureThrows(
                    "read() did not fail with the right exception when offline.",
                    () -> durableLog.read(0, 1, TIMEOUT),
                    ex -> ex instanceof ContainerOfflineException);
            AssertExtensions.assertSuppliedFutureThrows(
                    "truncate() did not fail with the right exception when offline.",
                    () -> durableLog.truncate(0, TIMEOUT),
                    ex -> ex instanceof ContainerOfflineException);

            // Verify we can also shut it down properly from this state.
            durableLog.stopAsync().awaitTerminated();
            Assert.assertTrue("awaitOnline() returned future did not fail when DurableLog shut down.", online.isCompletedExceptionally());
        }

        // Verify that, when the DurableDataLog becomes enabled, the DurableLog can pick up the change and resume normal operations.
        // Verify that the DurableLog starts properly and that all operations throw appropriate exceptions.
        dataLog.set(null);
        try (ReadIndex readIndex = new ContainerReadIndex(DEFAULT_READ_INDEX_CONFIG, metadata, cacheFactory, storage, cacheManager, executorService());
             DurableLog durableLog = new DurableLog(ContainerSetup.defaultDurableLogConfig(), metadata, dataLogFactory, readIndex, executorService())) {

            // DurableLog should start properly.
            durableLog.startAsync().awaitRunning();
            CompletableFuture<Void> online = durableLog.awaitOnline();
            Assert.assertFalse("awaitOnline() returned a completed future.", online.isDone());

            // Enable the underlying data log and await for recovery to finish.
            dataLog.get().enable();
            online.get(START_RETRY_DELAY_MILLIS * 100, TimeUnit.MILLISECONDS);
            Assert.assertFalse("Not expecting an offline DurableLog after re-enabling.", durableLog.isOffline());

            // Verify we can still read the data that we wrote before the DataLog was disabled.
            List<Operation> recoveredOperations = readUpToSequenceNumber(durableLog, metadata.getOperationSequenceNumber());
            assertRecoveredOperationsMatch(originalOperations, recoveredOperations);
            performMetadataChecks(streamSegmentIds, new HashSet<>(), new HashMap<>(), completionFutures, metadata, false, false);
            performReadIndexChecks(completionFutures, readIndex);

            // Stop the processor.
            durableLog.stopAsync().awaitTerminated();
        }
    }

    /**
     * Tests the following recovery scenario:
     * 1. A Segment is created and recorded in the metadata with some optional operations executing on it.
     * 2. The segment is evicted from the metadata.
     * 3. The segment is reactivated (with a new metadata mapping) - possibly due to an append. No truncation since #2.
     * 4. Recovery.
     */
    @Test
    public void testRecoveryWithMetadataCleanup() throws Exception {
        final long truncatedSeqNo = Integer.MAX_VALUE;
        // Setup a DurableLog and start it.
        @Cleanup
        TestDurableDataLogFactory dataLogFactory = new TestDurableDataLogFactory(new InMemoryDurableDataLogFactory(MAX_DATA_LOG_APPEND_SIZE, executorService()));
        @Cleanup
        Storage storage = InMemoryStorageFactory.newStorage(executorService());
        storage.initialize(1);
        long segmentId;

        // First DurableLog. We use this for generating data.
        val metadata1 = (StreamSegmentContainerMetadata) new MetadataBuilder(CONTAINER_ID).build();
        @Cleanup
        InMemoryCacheFactory cacheFactory = new InMemoryCacheFactory();
        @Cleanup
        CacheManager cacheManager = new CacheManager(CachePolicy.INFINITE, executorService());
        SegmentProperties originalSegmentInfo;
        try (ReadIndex readIndex = new ContainerReadIndex(DEFAULT_READ_INDEX_CONFIG, metadata1, cacheFactory, storage, cacheManager, executorService());
             DurableLog durableLog = new DurableLog(ContainerSetup.defaultDurableLogConfig(), metadata1, dataLogFactory, readIndex, executorService())) {
            durableLog.startAsync().awaitRunning();

            // Create the segment.
            val segmentIds = createStreamSegmentsWithOperations(1, durableLog);
            segmentId = segmentIds.stream().findFirst().orElse(-1L);

            // Evict the segment.
            val sm1 = metadata1.getStreamSegmentMetadata(segmentId);
            originalSegmentInfo = sm1.getSnapshot();
            metadata1.removeTruncationMarkers(truncatedSeqNo); // Simulate a truncation. This is needed in order to trigger a cleanup.
            val cleanedUpSegments = metadata1.cleanup(Collections.singleton(sm1), truncatedSeqNo);
            Assert.assertEquals("Unexpected number of segments evicted.", 1, cleanedUpSegments.size());

            // Map the segment again.
            val reMapOp = new StreamSegmentMapOperation(originalSegmentInfo);
            reMapOp.setStreamSegmentId(segmentId);
            durableLog.add(reMapOp, TIMEOUT).join();

            // Stop.
            durableLog.stopAsync().awaitTerminated();
        }

        // Recovery #1. This should work well.
        val metadata2 = (StreamSegmentContainerMetadata) new MetadataBuilder(CONTAINER_ID).build();
        try (ReadIndex readIndex = new ContainerReadIndex(DEFAULT_READ_INDEX_CONFIG, metadata2, cacheFactory, storage, cacheManager, executorService());
             DurableLog durableLog = new DurableLog(ContainerSetup.defaultDurableLogConfig(), metadata2, dataLogFactory, readIndex, executorService())) {
            durableLog.startAsync().awaitRunning();

            // Get segment info
            val recoveredSegmentInfo = metadata1.getStreamSegmentMetadata(segmentId).getSnapshot();
            Assert.assertEquals("Unexpected length from recovered segment.", originalSegmentInfo.getLength(), recoveredSegmentInfo.getLength());

            // Now evict the segment again ...
            val sm = metadata2.getStreamSegmentMetadata(segmentId);
            metadata2.removeTruncationMarkers(truncatedSeqNo); // Simulate a truncation. This is needed in order to trigger a cleanup.
            val cleanedUpSegments = metadata2.cleanup(Collections.singleton(sm), truncatedSeqNo);
            Assert.assertEquals("Unexpected number of segments evicted.", 1, cleanedUpSegments.size());

            // ... and re-map it with a new Id. This is a perfectly valid operation, and we can't prevent it.
            durableLog.add(new StreamSegmentMapOperation(originalSegmentInfo), TIMEOUT).join();

            // Stop.
            durableLog.stopAsync().awaitTerminated();
        }

        // Recovery #2. This should fail due to the same segment mapped multiple times with different ids.
        val metadata3 = (StreamSegmentContainerMetadata) new MetadataBuilder(CONTAINER_ID).build();
        try (ReadIndex readIndex = new ContainerReadIndex(DEFAULT_READ_INDEX_CONFIG, metadata3, cacheFactory, storage, cacheManager, executorService());
             DurableLog durableLog = new DurableLog(ContainerSetup.defaultDurableLogConfig(), metadata3, dataLogFactory, readIndex, executorService())) {
            AssertExtensions.assertThrows(
                    "Recovery did not fail with the expected exception in case of multi-mapping",
                    () -> durableLog.startAsync().awaitRunning(),
                    ex -> ex instanceof IllegalStateException
                            && ex.getCause() instanceof DataCorruptionException
                            && ex.getCause().getCause() instanceof MetadataUpdateException);
        }
    }

    /**
     * Tests the ability of the DurableLog properly recover from situations where operations were split across multiple
     * DataFrames, but were not persisted in their entirety. These operations should be ignored as they are incomplete
     * and were never acknowledged to the upstream callers.
     */
    @Test
    public void testRecoveryPartialOperations() {
        // Setup the first Durable Log and create the segment.
        @Cleanup
        ContainerSetup setup = new ContainerSetup(executorService());
        @Cleanup
        DurableLog dl1 = setup.createDurableLog();
        dl1.startAsync().awaitRunning();
        Assert.assertNotNull("Internal error: could not grab a pointer to the created TestDurableDataLog.", setup.dataLog.get());
        val segmentId = createStreamSegmentsWithOperations(1, dl1).stream().findFirst().orElse(-1L);

        // Part of this operation should fail.
        ErrorInjector<Exception> asyncErrorInjector = new ErrorInjector<>(
                count -> count == 1,
                () -> new DurableDataLogException("intentional"));
        setup.dataLog.get().setAppendErrorInjectors(null, asyncErrorInjector);
        val append1 = new StreamSegmentAppendOperation(segmentId, new byte[MAX_DATA_LOG_APPEND_SIZE], null);
        AssertExtensions.assertSuppliedFutureThrows(
                "Expected the operation to have failed.",
                () -> dl1.add(append1, TIMEOUT),
                ex -> ex instanceof DurableDataLogException);

        AssertExtensions.assertThrows(
                "Expected the DurableLog to have failed after failed operation.",
                dl1::awaitTerminated,
                ex -> ex instanceof IllegalStateException);
        dl1.close();
        setup.dataLog.get().setAppendErrorInjectors(null, null);

        // Setup the second Durable Log. Ensure the recovery succeeds and that we don't see that failed operation.
        @Cleanup
        val dl2 = setup.createDurableLog();
        dl2.startAsync().awaitRunning();
        val ops2 = dl2.read(0, 10, TIMEOUT).join();
        Assert.assertTrue("Expected first operation to be a checkpoint.", ops2.hasNext() && ops2.next() instanceof MetadataCheckpointOperation);
        Assert.assertTrue("Expected second operation to be a segment map.", ops2.hasNext() && ops2.next() instanceof StreamSegmentMapOperation);
        Assert.assertFalse("Not expecting any other operations.", ops2.hasNext());

        // Add a new operation. This one should succeed.
        val append2 = new StreamSegmentAppendOperation(segmentId, new byte[10], null);
        dl2.add(append2, TIMEOUT).join();
        dl2.stopAsync().awaitTerminated();
        dl2.close();

        // Setup the third Durable Log. Ensure the recovery succeeds that we only see the operations we care about.
        @Cleanup
        val dl3 = setup.createDurableLog();
        dl3.startAsync().awaitRunning();
        val ops3 = dl3.read(0, 10, TIMEOUT).join();
        Assert.assertTrue("Expected first operation to be a checkpoint.", ops3.hasNext() && ops3.next() instanceof MetadataCheckpointOperation);
        Assert.assertTrue("Expected second operation to be a segment map.", ops3.hasNext() && ops3.next() instanceof StreamSegmentMapOperation);
        Assert.assertTrue("Expected third operation to be an append.", ops3.hasNext() && ops3.next() instanceof CachedStreamSegmentAppendOperation);
        Assert.assertFalse("Not expecting any other operations.", ops3.hasNext());
        dl2.stopAsync().awaitTerminated();
        dl3.close();
    }

    //endregion

    //region Truncation

    /**
     * Tests the truncate() method without doing any recovery.
     */
    @Test
    public void testTruncateWithoutRecovery() throws Exception {
        int streamSegmentCount = 50;
        int appendsPerStreamSegment = 20;

        // Setup a DurableLog and start it.
        AtomicReference<TestDurableDataLog> dataLog = new AtomicReference<>();
        AtomicReference<Boolean> truncationOccurred = new AtomicReference<>();
        @Cleanup
        TestDurableDataLogFactory dataLogFactory = new TestDurableDataLogFactory(new InMemoryDurableDataLogFactory(MAX_DATA_LOG_APPEND_SIZE, executorService()), dataLog::set);
        @Cleanup
        Storage storage = InMemoryStorageFactory.newStorage(executorService());
        storage.initialize(1);
        UpdateableContainerMetadata metadata = new MetadataBuilder(CONTAINER_ID).build();

        @Cleanup
        InMemoryCacheFactory cacheFactory = new InMemoryCacheFactory();
        @Cleanup
        CacheManager cacheManager = new CacheManager(CachePolicy.INFINITE, executorService());
        @Cleanup
        ReadIndex readIndex = new ContainerReadIndex(DEFAULT_READ_INDEX_CONFIG, metadata, cacheFactory, storage, cacheManager, executorService());

        // First DurableLog. We use this for generating data.
        try (DurableLog durableLog = new DurableLog(ContainerSetup.defaultDurableLogConfig(), metadata, dataLogFactory, readIndex, executorService())) {
            durableLog.startAsync().awaitRunning();

            // Hook up a listener to figure out when truncation actually happens.
            dataLog.get().setTruncateCallback(seqNo -> truncationOccurred.set(true));

            // Generate some test data (we need to do this after we started the DurableLog because in the process of
            // recovery, it wipes away all existing metadata).
            Set<Long> streamSegmentIds = createStreamSegmentsWithOperations(streamSegmentCount, durableLog);
            List<Operation> queuedOperations = generateOperations(streamSegmentIds, new HashMap<>(), appendsPerStreamSegment, METADATA_CHECKPOINT_EVERY, false, false);
            val lastOp = new MetadataCheckpointOperation();
            queuedOperations.add(lastOp); // Add one of these at the end to ensure we can truncate everything.

            List<OperationWithCompletion> completionFutures = processOperations(queuedOperations, durableLog);
            OperationWithCompletion.allOf(completionFutures).join();
            awaitLastOperationAdded(durableLog, metadata);

            // Get a list of all the operations, before truncation.
            List<Operation> originalOperations = readUpToSequenceNumber(durableLog, metadata.getOperationSequenceNumber());
            boolean fullTruncationPossible = false;

            // Truncate up to each operation and:
            // * If the DataLog was truncated:
            // ** Verify the appropriate operations were truncated from the DL
            // At the end, verify all operations and all entries in the DataLog were truncated.
            for (int i = 0; i < originalOperations.size(); i++) {
                Operation currentOperation = originalOperations.get(i);
                truncationOccurred.set(false);
                if (currentOperation instanceof MetadataCheckpointOperation) {
                    // Need to figure out if the operation we're about to truncate to is actually the first in the log;
                    // in that case, we should not be expecting any truncation.
                    boolean isTruncationPointFirstOperation = durableLog.read(-1, 1, TIMEOUT).join().next() instanceof MetadataCheckpointOperation;

                    // Perform the truncation.
                    durableLog.truncate(currentOperation.getSequenceNumber(), TIMEOUT).join();
                    awaitLastOperationAdded(durableLog, metadata);
                    if (!isTruncationPointFirstOperation) {
                        Assert.assertTrue("No truncation occurred even though a valid Truncation Point was passed: " + currentOperation.getSequenceNumber(), truncationOccurred.get());
                    }

                    // Verify all operations up to, and including this one have been removed.
                    Iterator<Operation> reader = durableLog.read(-1, 2, TIMEOUT).join();
                    Assert.assertTrue("Not expecting an empty log after truncating an operation (a MetadataCheckpoint must always exist).", reader.hasNext());
                    verifyFirstItemIsMetadataCheckpoint(reader);

                    if (i < originalOperations.size() - 1) {
                        Operation firstOp = reader.next();
                        OperationComparer.DEFAULT.assertEquals(String.format("Unexpected first operation after truncating SeqNo %d.", currentOperation.getSequenceNumber()), originalOperations.get(i + 1), firstOp);
                    } else {
                        // Sometimes the Truncation Point is on the same DataFrame as other data, and it's the last DataFrame;
                        // In that case, it cannot be truncated, since truncating the frame would mean losing the Checkpoint as well.
                        fullTruncationPossible = !reader.hasNext();
                    }
                } else {
                    // Verify we are not allowed to truncate on non-valid Truncation Points.
                    AssertExtensions.assertSuppliedFutureThrows(
                            "DurableLog allowed truncation on a non-MetadataCheckpointOperation.",
                            () -> durableLog.truncate(currentOperation.getSequenceNumber(), TIMEOUT),
                            ex -> ex instanceof IllegalArgumentException);

                    // Verify the Operation Log is still intact.
                    Iterator<Operation> reader = durableLog.read(-1, 1, TIMEOUT).join();
                    Assert.assertTrue("No elements left in the log even though no truncation occurred.", reader.hasNext());
                    Operation firstOp = reader.next();
                    AssertExtensions.assertLessThanOrEqual("It appears that Operations were removed from the Log even though no truncation happened.", currentOperation.getSequenceNumber(), firstOp.getSequenceNumber());
                }
            }

            // Verify that we can still queue operations to the DurableLog and they can be read.
            // In this case we'll just queue some StreamSegmentMapOperations.
            StreamSegmentMapOperation newOp = new StreamSegmentMapOperation(StreamSegmentInformation.builder().name("foo").build());
            if (!fullTruncationPossible) {
                // We were not able to do a full truncation before. Do one now, since we are guaranteed to have a new DataFrame available.
                MetadataCheckpointOperation lastCheckpoint = new MetadataCheckpointOperation();
                durableLog.add(lastCheckpoint, TIMEOUT).join();
                awaitLastOperationAdded(durableLog, metadata);
                durableLog.truncate(lastCheckpoint.getSequenceNumber(), TIMEOUT).join();
            }

            durableLog.add(newOp, TIMEOUT).join();
            awaitLastOperationAdded(durableLog, metadata);
            final int expectedOperationCount = 3; // Full Checkpoint + Storage Checkpoint (auto-added)+ new op
            List<Operation> newOperations = readUpToSequenceNumber(durableLog, metadata.getOperationSequenceNumber());
            Assert.assertEquals("Unexpected number of operations added after full truncation.", expectedOperationCount, newOperations.size());
            Assert.assertTrue("Expecting the first operation after full truncation to be a MetadataCheckpointOperation.",
                    newOperations.get(0) instanceof MetadataCheckpointOperation);
            Assert.assertTrue("Expecting a StorageMetadataCheckpointOperation to be auto-added after full truncation.",
                    newOperations.get(1) instanceof StorageMetadataCheckpointOperation);
            Assert.assertEquals("Unexpected Operation encountered after full truncation.", newOp, newOperations.get(2));

            // Stop the processor.
            durableLog.stopAsync().awaitTerminated();
        }
    }

    /**
     * Tests the truncate() method while performing recovery.
     */
    @Test
    public void testTruncateWithRecovery() {
        int streamSegmentCount = 50;
        int appendsPerStreamSegment = 20;

        // Setup a DurableLog and start it.
        AtomicReference<TestDurableDataLog> dataLog = new AtomicReference<>();
        AtomicReference<Boolean> truncationOccurred = new AtomicReference<>();
        @Cleanup
        TestDurableDataLogFactory dataLogFactory = new TestDurableDataLogFactory(new InMemoryDurableDataLogFactory(MAX_DATA_LOG_APPEND_SIZE, executorService()), dataLog::set);
        @Cleanup
        Storage storage = InMemoryStorageFactory.newStorage(executorService());
        storage.initialize(1);
        UpdateableContainerMetadata metadata = new MetadataBuilder(CONTAINER_ID).build();

        @Cleanup
        InMemoryCacheFactory cacheFactory = new InMemoryCacheFactory();
        @Cleanup
        CacheManager cacheManager = new CacheManager(CachePolicy.INFINITE, executorService());
        @Cleanup
        ReadIndex readIndex = new ContainerReadIndex(DEFAULT_READ_INDEX_CONFIG, metadata, cacheFactory, storage, cacheManager, executorService());
        Set<Long> streamSegmentIds;
        List<OperationWithCompletion> completionFutures;
        List<Operation> originalOperations;

        // First DurableLog. We use this for generating data.
        try (DurableLog durableLog = new DurableLog(ContainerSetup.defaultDurableLogConfig(), metadata, dataLogFactory, readIndex, executorService())) {
            durableLog.startAsync().awaitRunning();

            // Generate some test data (we need to do this after we started the DurableLog because in the process of
            // recovery, it wipes away all existing metadata).
            streamSegmentIds = createStreamSegmentsWithOperations(streamSegmentCount, durableLog);
            List<Operation> queuedOperations = generateOperations(streamSegmentIds, new HashMap<>(), appendsPerStreamSegment, METADATA_CHECKPOINT_EVERY, false, false);
            completionFutures = processOperations(queuedOperations, durableLog);
            OperationWithCompletion.allOf(completionFutures).join();

            // Get a list of all the operations, before any truncation.
            originalOperations = readUpToSequenceNumber(durableLog, metadata.getOperationSequenceNumber());

            // Stop the processor.
            durableLog.stopAsync().awaitTerminated();
        }

        // Truncate up to each MetadataCheckpointOperation and:
        // * If the DataLog was truncated:
        // ** Shut down DurableLog, re-start it (recovery) and verify the operations are as they should.
        // At the end, verify all operations and all entries in the DataLog were truncated.
        DurableLog durableLog = new DurableLog(ContainerSetup.defaultDurableLogConfig(), metadata, dataLogFactory, readIndex, executorService());
        try {
            durableLog.startAsync().awaitRunning();
            dataLog.get().setTruncateCallback(seqNo -> truncationOccurred.set(true));
            for (int i = 0; i < originalOperations.size(); i++) {
                Operation currentOperation = originalOperations.get(i);
                if (!(currentOperation instanceof MetadataCheckpointOperation)) {
                    // We can only truncate on MetadataCheckpointOperations.
                    continue;
                }

                truncationOccurred.set(false);
                durableLog.truncate(currentOperation.getSequenceNumber(), TIMEOUT).join();
                if (truncationOccurred.get()) {
                    // Close current DurableLog and start a brand new one, forcing recovery.
                    durableLog.close();
                    durableLog = new DurableLog(ContainerSetup.defaultDurableLogConfig(), metadata, dataLogFactory, readIndex, executorService());
                    durableLog.startAsync().awaitRunning();
                    dataLog.get().setTruncateCallback(seqNo -> truncationOccurred.set(true));

                    // Verify all operations up to, and including this one have been removed.
                    Iterator<Operation> reader = durableLog.read(-1, 2, TIMEOUT).join();
                    Assert.assertTrue("Not expecting an empty log after truncating an operation (a MetadataCheckpoint must always exist).", reader.hasNext());
                    verifyFirstItemIsMetadataCheckpoint(reader);

                    if (i < originalOperations.size() - 1) {
                        Operation firstOp = reader.next();
                        OperationComparer.DEFAULT.assertEquals(String.format("Unexpected first operation after truncating SeqNo %d.", currentOperation.getSequenceNumber()), originalOperations.get(i + 1), firstOp);
                    }
                }
            }
        } finally {
            // This closes whatever current instance this variable refers to, not necessarily the first one.
            durableLog.close();
        }
    }

    /**
     * Tests the ability of the truncate() method to auto-queue (and wait for) mini-metadata checkpoints containing items
     * that are not updated via normal operations. Such items include StorageLength and IsSealedInStorage.
     */
    @Test
    public void testTruncateWithStorageMetadataCheckpoints() {
        int streamSegmentCount = 50;
        int appendsPerStreamSegment = 20;

        // Setup a DurableLog and start it.
        @Cleanup
        TestDurableDataLogFactory dataLogFactory = new TestDurableDataLogFactory(new InMemoryDurableDataLogFactory(MAX_DATA_LOG_APPEND_SIZE, executorService()));
        @Cleanup
        Storage storage = InMemoryStorageFactory.newStorage(executorService());
        storage.initialize(1);
        val metadata1 = new MetadataBuilder(CONTAINER_ID).build();

        @Cleanup
        InMemoryCacheFactory cacheFactory = new InMemoryCacheFactory();
        @Cleanup
        CacheManager cacheManager = new CacheManager(CachePolicy.INFINITE, executorService());
        @Cleanup
        val readIndex1 = new ContainerReadIndex(DEFAULT_READ_INDEX_CONFIG, metadata1, cacheFactory, storage, cacheManager, executorService());
        Set<Long> streamSegmentIds;
        List<OperationWithCompletion> completionFutures;

        // First DurableLog. We use this for generating data.
        try (DurableLog durableLog = new DurableLog(ContainerSetup.defaultDurableLogConfig(), metadata1, dataLogFactory, readIndex1, executorService())) {
            durableLog.startAsync().awaitRunning();

            // Generate some test data (we need to do this after we started the DurableLog because in the process of
            // recovery, it wipes away all existing metadata).
            streamSegmentIds = createStreamSegmentsWithOperations(streamSegmentCount, durableLog);
            List<Operation> queuedOperations = generateOperations(streamSegmentIds, new HashMap<>(), appendsPerStreamSegment, METADATA_CHECKPOINT_EVERY, false, false);
            completionFutures = processOperations(queuedOperations, durableLog);
            OperationWithCompletion.allOf(completionFutures).join();

            // Update the metadata with Storage-related data. Set some arbitrary StorageOffsets and seal 50% of the segments in storage.
            long storageOffset = 0;
            for (long segmentId : streamSegmentIds) {
                val sm = metadata1.getStreamSegmentMetadata(segmentId);
                sm.setStorageLength(Math.min(storageOffset, sm.getLength()));
                storageOffset++;
                if (sm.isSealed() && storageOffset % 2 == 0) {
                    sm.markSealedInStorage();
                }
            }

            // Truncate at the last possible truncation point.
            val originalOperations = readUpToSequenceNumber(durableLog, metadata1.getOperationSequenceNumber());
            long lastCheckpointSeqNo = -1;
            for (Operation o : originalOperations) {
                if (o instanceof MetadataCheckpointOperation) {
                    lastCheckpointSeqNo = o.getSequenceNumber();
                }
            }

            AssertExtensions.assertGreaterThan("Could not find any truncation points.", 0, lastCheckpointSeqNo);
            durableLog.truncate(lastCheckpointSeqNo, TIMEOUT).join();

            // Stop the processor.
            durableLog.stopAsync().awaitTerminated();
        }

        // Start a second DurableLog and then verify the metadata.
        val metadata2 = new MetadataBuilder(CONTAINER_ID).build();
        @Cleanup
        val readIndex2 = new ContainerReadIndex(DEFAULT_READ_INDEX_CONFIG, metadata2, cacheFactory, storage, cacheManager, executorService());
        try (DurableLog durableLog = new DurableLog(ContainerSetup.defaultDurableLogConfig(), metadata2, dataLogFactory, readIndex2, executorService())) {
            durableLog.startAsync().awaitRunning();

            // Check Metadata1 vs Metadata2
            for (long segmentId : streamSegmentIds) {
                val sm1 = metadata1.getStreamSegmentMetadata(segmentId);
                val sm2 = metadata2.getStreamSegmentMetadata(segmentId);
                Assert.assertEquals("StorageLength differs for recovered segment " + segmentId,
                        sm1.getStorageLength(), sm2.getStorageLength());
                Assert.assertEquals("IsSealedInStorage differs for recovered segment " + segmentId,
                        sm1.isSealedInStorage(), sm2.isSealedInStorage());
            }

            // Stop the processor.
            durableLog.stopAsync().awaitTerminated();
        }
    }

    //endregion

    //region Helpers

    private void performLogOperationChecks(List<OperationWithCompletion> operations, DurableLog durableLog) {
        // Log Operation based checks
        long lastSeqNo = -1;
        val successfulOperations = operations.stream()
                                             .filter(oc -> !oc.completion.isCompletedExceptionally())
                                             .map(oc -> oc.operation)
                                             .collect(Collectors.toList());

        // Writing to the DurableLog is done asynchronously, so wait for the last operation to arrive there before reading.
        durableLog.read(successfulOperations.get(successfulOperations.size() - 1).getSequenceNumber() - 1, 1, TIMEOUT).join();

        // Issue the read for the entire log now.
        Iterator<Operation> logIterator = durableLog.read(-1L, operations.size() + 1, TIMEOUT).join();
        verifyFirstItemIsMetadataCheckpoint(logIterator);
        OperationComparer comparer = new OperationComparer(true);
        for (Operation expectedOp : successfulOperations) {
            // Verify that the operations have been completed and assigned sequential Sequence Numbers.
            AssertExtensions.assertGreaterThan("Operations were not assigned sequential Sequence Numbers.", lastSeqNo, expectedOp.getSequenceNumber());
            lastSeqNo = expectedOp.getSequenceNumber();

            // MemoryLog: verify that the operations match that of the expected list.
            Assert.assertTrue("No more items left to read from DurableLog. Expected: " + expectedOp, logIterator.hasNext());
            comparer.assertEquals("Unexpected Operation in MemoryLog.", expectedOp, logIterator.next()); // Ok to use assertEquals because we are actually expecting the same object here.
        }
    }

    /**
     * Reads the given OperationLog from the beginning up to the given Sequence Number. This method makes use of tail
     * reads in the OperationLog, which handle the case when the OperationProcessor asynchronously adds operations
     * to the log after ack-ing them.
     */
    @SneakyThrows
    private List<Operation> readUpToSequenceNumber(OperationLog durableLog, long seqNo) {
        ArrayList<Operation> result = new ArrayList<>();
        while (true) {
            // Figure out if we've already reached our limit.
            long afterSequence = result.size() == 0 ? -1 : result.get(result.size() - 1).getSequenceNumber();
            if (afterSequence >= seqNo) {
                break;
            }

            // Figure out how much to read. If we don't know, read at least one item so we see what's the first SeqNo
            // in the Log.
            int maxCount = result.size() == 0 ? 1 : (int) (seqNo - result.get(result.size() - 1).getSequenceNumber());
            Iterator<Operation> logIterator = durableLog.read(afterSequence, maxCount, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            while (logIterator.hasNext()) {
                result.add(logIterator.next());
            }
        }
        return result;
    }

    private void verifyFirstItemIsMetadataCheckpoint(Iterator<Operation> logIterator) {
        Assert.assertTrue("DurableLog is empty even though a MetadataCheckpointOperation was expected.", logIterator.hasNext());
        Operation firstOp = logIterator.next();
        Assert.assertTrue("First operation in DurableLog is not a MetadataCheckpointOperation: " + firstOp, firstOp instanceof MetadataCheckpointOperation);
    }

    private List<OperationWithCompletion> processOperations(Collection<Operation> operations, DurableLog durableLog) {
        return processOperations(operations, durableLog, operations.size() + 1);
    }

    @SneakyThrows
    private List<OperationWithCompletion> processOperations(Collection<Operation> operations, DurableLog durableLog, int waitEvery) {
        List<OperationWithCompletion> completionFutures = new ArrayList<>();
        int index = 0;
        for (Operation o : operations) {
            index++;
            CompletableFuture<Void> completionFuture;
            try {
                completionFuture = durableLog.add(o, TIMEOUT);
            } catch (Exception ex) {
                completionFuture = Futures.failedFuture(ex);
            }

            completionFutures.add(new OperationWithCompletion(o, completionFuture));
            if (index % waitEvery == 0) {
                completionFuture.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
        }

        return completionFutures;
    }

    private void assertRecoveredOperationsMatch(List<Operation> expected, List<Operation> actual) {
        Assert.assertEquals("Recovered operations do not match original ones. Collections differ in size.", expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            Operation expectedItem = expected.get(i);
            Operation actualItem = actual.get(i);
            if (expectedItem instanceof CheckpointOperationBase) {
                Assert.assertNull("Recovered Checkpoint Operation did not have contents cleared up.", ((CheckpointOperationBase) actualItem).getContents());
                Assert.assertEquals(" Unexpected Sequence Number", expectedItem.getSequenceNumber(), actualItem.getSequenceNumber());
            } else {
                OperationComparer.DEFAULT.assertEquals(
                        String.format("Recovered operations do not match original ones. Elements at index %d differ. Expected '%s', found '%s'.", i, expectedItem, actualItem),
                        expectedItem, actualItem);
            }
        }
    }

    /**
     * Blocks synchronously until an operation with a Sequence Number of at least
     * {@link UpdateableContainerMetadata#getOperationSequenceNumber()} is present in the given {@link OperationLog}'s
     * operations.
     *
     * This is helpful if we want to make sure that all the background async operations have completed before moving on
     * to a next step. The {@link OperationProcessor} (inside {@link DurableLog}) acknowledges operations before adding
     * them to the internal memory structures, so it is possible that we act on an ack before an operation that we need
     * is present in the {@link DurableLog}.
     */
    @SneakyThrows(TimeoutException.class)
    private void awaitLastOperationAdded(OperationLog durableLog, UpdateableContainerMetadata metadata) {
        long sn = metadata.getOperationSequenceNumber();
        TestUtils.await(
                () -> {
                    val allOps = readUpToSequenceNumber(durableLog, sn);
                    return allOps.size() > 0 && allOps.get(allOps.size() - 1).getSequenceNumber() >= sn;
                }, 10, TIMEOUT.toMillis());
    }

    //endregion

    //region InjectedReadItem

    @Data
    private static class InjectedReadItem implements TestDurableDataLog.ReadItem {
        private final InputStream payload;
        private final int length;
        private final LogAddress address;
    }

    //endregion

    //region ContainerSetup

    private static class ContainerSetup implements AutoCloseable {
        final ScheduledExecutorService executorService;
        final TestDurableDataLogFactory dataLogFactory;
        final AtomicReference<TestDurableDataLog> dataLog;
        final UpdateableContainerMetadata metadata;
        final ReadIndex readIndex;
        final CacheManager cacheManager;
        final Storage storage;
        DurableLogConfig durableLogConfig;
        final CacheFactory cacheFactory;

        ContainerSetup(ScheduledExecutorService executorService) {
            this.dataLog = new AtomicReference<>();
            this.executorService = executorService;
            this.dataLogFactory = new TestDurableDataLogFactory(new InMemoryDurableDataLogFactory(MAX_DATA_LOG_APPEND_SIZE, this.executorService), this.dataLog::set);
            this.metadata = new MetadataBuilder(CONTAINER_ID).build();
            this.cacheFactory = new InMemoryCacheFactory();
            this.storage = InMemoryStorageFactory.newStorage(executorService);
            this.storage.initialize(1);
            this.cacheManager = new CacheManager(CachePolicy.INFINITE, this.executorService);
            this.readIndex = new ContainerReadIndex(DEFAULT_READ_INDEX_CONFIG, metadata, this.cacheFactory, this.storage, this.cacheManager, this.executorService);
        }

        @Override
        public void close() {
            this.readIndex.close();
            this.dataLogFactory.close();
            this.storage.close();
            this.cacheFactory.close();
            this.cacheManager.close();
        }

        DurableLog createDurableLog() {
            DurableLogConfig config = this.durableLogConfig == null ? defaultDurableLogConfig() : this.durableLogConfig;
            return new DurableLog(config, this.metadata, this.dataLogFactory, this.readIndex, this.executorService);
        }

        void setDurableLogConfig(DurableLogConfig config) {
            this.durableLogConfig = config;
        }

        static DurableLogConfig defaultDurableLogConfig() {
            return createDurableLogConfig(null, null);
        }

        static DurableLogConfig createDurableLogConfig(Integer checkpointMinCommitCount, Long checkpointMinTotalCommitLength) {
            if (checkpointMinCommitCount == null) {
                checkpointMinCommitCount = Integer.MAX_VALUE;
            }

            if (checkpointMinTotalCommitLength == null) {
                checkpointMinTotalCommitLength = Long.MAX_VALUE;
            }

            return DurableLogConfig
                    .builder()
                    .with(DurableLogConfig.CHECKPOINT_MIN_COMMIT_COUNT, CHECKPOINT_MIN_COMMIT_COUNT)
                    .with(DurableLogConfig.CHECKPOINT_COMMIT_COUNT, checkpointMinCommitCount)
                    .with(DurableLogConfig.CHECKPOINT_TOTAL_COMMIT_LENGTH, checkpointMinTotalCommitLength)
                    .with(DurableLogConfig.START_RETRY_DELAY_MILLIS, START_RETRY_DELAY_MILLIS)
                    .build();
        }
    }

    //endregion

    //region CorruptedDurableLog

    private static class CorruptedDurableLog extends DurableLog {
        private static final AtomicInteger FAIL_AT_INDEX = new AtomicInteger();

        CorruptedDurableLog(DurableLogConfig config, ContainerSetup setup) {
            super(config, setup.metadata, setup.dataLogFactory, setup.readIndex, setup.executorService);
        }

        @Override
        protected SequencedItemList<Operation> createInMemoryLog() {
            return new CorruptedMemoryOperationLog(FAIL_AT_INDEX.get());
        }
    }

    //endregion

}
