/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.distributedlog.impl.logsegment;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.distributedlog.BookKeeperClient;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.LogSegmentMetadata;
import org.apache.distributedlog.ZooKeeperClient;
import org.apache.distributedlog.bk.DynamicQuorumConfigProvider;
import org.apache.distributedlog.bk.LedgerAllocator;
import org.apache.distributedlog.bk.LedgerAllocatorDelegator;
import org.apache.distributedlog.bk.QuorumConfigProvider;
import org.apache.distributedlog.bk.SimpleLedgerAllocator;
import org.apache.distributedlog.config.DynamicDistributedLogConfiguration;
import org.apache.distributedlog.exceptions.BKTransmitException;
import org.apache.distributedlog.injector.AsyncFailureInjector;
import org.apache.distributedlog.logsegment.LogSegmentEntryReader;
import org.apache.distributedlog.logsegment.LogSegmentEntryStore;
import org.apache.distributedlog.logsegment.LogSegmentEntryWriter;
import org.apache.distributedlog.logsegment.LogSegmentRandomAccessEntryReader;
import org.apache.distributedlog.metadata.LogMetadataForWriter;
import org.apache.distributedlog.util.Allocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * BookKeeper Based Entry Store.
 */
public class BKLogSegmentEntryStore implements
        LogSegmentEntryStore,
        AsyncCallback.DeleteCallback {

    private static final Logger logger = LoggerFactory.getLogger(BKLogSegmentEntryReader.class);

    private static class DeleteLogSegmentRequest {

        private final LogSegmentMetadata segment;
        private final CompletableFuture<LogSegmentMetadata> deletePromise;

        DeleteLogSegmentRequest(LogSegmentMetadata segment) {
            this.segment = segment;
            this.deletePromise = new CompletableFuture<LogSegmentMetadata>();
        }

    }

    private final ZooKeeperClient zkc;
    private final BookKeeperClient bkc;
    private final OrderedScheduler scheduler;
    private final DistributedLogConfiguration conf;
    private final DynamicDistributedLogConfiguration dynConf;
    private final StatsLogger statsLogger;
    private final AsyncFailureInjector failureInjector;
    // ledger allocator
    private final LedgerAllocator allocator;

    public BKLogSegmentEntryStore(DistributedLogConfiguration conf,
                                  DynamicDistributedLogConfiguration dynConf,
                                  ZooKeeperClient zkc,
                                  BookKeeperClient bkc,
                                  OrderedScheduler scheduler,
                                  LedgerAllocator allocator,
                                  StatsLogger statsLogger,
                                  AsyncFailureInjector failureInjector) {
        this.conf = conf;
        this.dynConf = dynConf;
        this.zkc = zkc;
        this.bkc = bkc;
        this.scheduler = scheduler;
        this.allocator = allocator;
        this.statsLogger = statsLogger;
        this.failureInjector = failureInjector;
    }

    @Override
    public CompletableFuture<LogSegmentMetadata> deleteLogSegment(LogSegmentMetadata segment) {
        DeleteLogSegmentRequest request = new DeleteLogSegmentRequest(segment);
        BookKeeper bk;
        try {
            bk = this.bkc.get();
        } catch (IOException e) {
            return FutureUtils.exception(e);
        }
        bk.asyncDeleteLedger(segment.getLogSegmentId(), this, request);
        return request.deletePromise;
    }

    @Override
    public void deleteComplete(int rc, Object ctx) {
        DeleteLogSegmentRequest deleteRequest = (DeleteLogSegmentRequest) ctx;
        if (BKException.Code.NoSuchLedgerExistsException == rc) {
            logger.warn("No ledger {} found to delete for {}.",
                    deleteRequest.segment.getLogSegmentId(), deleteRequest.segment);
        } else if (BKException.Code.OK != rc) {
            logger.error("Couldn't delete ledger {} from bookkeeper for {} : {}",
                    new Object[]{ deleteRequest.segment.getLogSegmentId(), deleteRequest.segment,
                            BKException.getMessage(rc) });
            FutureUtils.completeExceptionally(deleteRequest.deletePromise,
                    new BKTransmitException("Couldn't delete log segment " + deleteRequest.segment, rc));
            return;
        }
        FutureUtils.complete(deleteRequest.deletePromise, deleteRequest.segment);
    }

    //
    // Writers
    //

    LedgerAllocator createLedgerAllocator(LogMetadataForWriter logMetadata,
                                          DynamicDistributedLogConfiguration dynConf)
            throws IOException {
        LedgerAllocator ledgerAllocatorDelegator;
        if (null == allocator || !dynConf.getEnableLedgerAllocatorPool()) {
            QuorumConfigProvider quorumConfigProvider =
                    new DynamicQuorumConfigProvider(dynConf);
            LedgerAllocator allocator = new SimpleLedgerAllocator(
                    logMetadata.getAllocationPath(),
                    logMetadata.getAllocationData(),
                    quorumConfigProvider,
                    zkc,
                    bkc);
            ledgerAllocatorDelegator = new LedgerAllocatorDelegator(allocator, true);
        } else {
            ledgerAllocatorDelegator = allocator;
        }
        return ledgerAllocatorDelegator;
    }

    @Override
    public Allocator<LogSegmentEntryWriter, Object> newLogSegmentAllocator(
            LogMetadataForWriter logMetadata,
            DynamicDistributedLogConfiguration dynConf) throws IOException {
        // Build the ledger allocator
        LedgerAllocator allocator = createLedgerAllocator(logMetadata, dynConf);
        return new BKLogSegmentAllocator(allocator);
    }

    //
    // Readers
    //

    @Override
    public CompletableFuture<LogSegmentEntryReader> openReader(LogSegmentMetadata segment,
                                                    long startEntryId) {
        BookKeeper bk;
        try {
            bk = this.bkc.get();
        } catch (IOException e) {
            return FutureUtils.exception(e);
        }

        CompletableFuture<LedgerHandle> ledgerF;
        if (segment.isInProgress()) {
            ledgerF = bkc.openLedgerNoRecovery(segment.getLogSegmentId());
        } else {
            ledgerF = bkc.openLedger(segment.getLogSegmentId());
        }

        CompletableFuture<LogSegmentEntryReader> promise = new CompletableFuture<>();
        ledgerF.thenApply((ledger) -> {
                return new BKLogSegmentEntryReader(
                        segment,
                        ledger,
                        startEntryId,
                        bkc,
                        scheduler,
                        conf,
                        statsLogger,
                        failureInjector);
            })
            .whenComplete((reader, exception) -> {
                    if (exception != null) {
                        promise.completeExceptionally(
                                new BKTransmitException(
                                        "Failed to open ledger handle for log segment " + segment, exception));
                    } else {
                        promise.complete(reader);
                    }
                });
        return promise;
    }

    @Override
    public CompletableFuture<LogSegmentRandomAccessEntryReader> openRandomAccessReader(final LogSegmentMetadata segment,
                                                                            final boolean fence) {
        final BookKeeper bk;
        try {
            bk = this.bkc.get();
        } catch (IOException e) {
            return FutureUtils.exception(e);
        }

        CompletableFuture<LedgerHandle> ledgerF;
        if (segment.isInProgress() && !fence) {
            ledgerF = bkc.openLedgerNoRecovery(segment.getLogSegmentId());
        } else {
            ledgerF = bkc.openLedger(segment.getLogSegmentId());
        }

        CompletableFuture<LogSegmentRandomAccessEntryReader> promise = new CompletableFuture<>();
        ledgerF.thenApply((ledger) -> {
                return new BKLogSegmentRandomAccessEntryReader(segment, ledger, conf);
            })
            .whenComplete((reader, exception) -> {
                    if (exception != null) {
                        promise.completeExceptionally(
                                new BKTransmitException(
                                        "Failed to open ledger handle for log segment " + segment, exception));
                    } else {
                        promise.complete(reader);
                    }
                });
        return promise;
    }
}
