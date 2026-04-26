/*
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

package org.apache.flink.connector.kinesis.source.reader.fanout;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.kinesis.source.exception.KinesisStreamsSourceException;
import org.apache.flink.connector.kinesis.source.proxy.AsyncStreamProxy;
import org.apache.flink.connector.kinesis.source.split.StartingPosition;
import org.apache.flink.util.ExceptionUtils;

import io.netty.handler.timeout.ReadTimeoutException;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.model.InternalFailureException;
import software.amazon.awssdk.services.kinesis.model.KinesisException;
import software.amazon.awssdk.services.kinesis.model.LimitExceededException;
import software.amazon.awssdk.services.kinesis.model.ResourceInUseException;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEventStream;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * FanOutSubscription class responsible for handling the subscription to a single shard of the
 * Kinesis stream. Given a shardId, it will manage the lifecycle of the subscription, and eagerly
 * keep the next batch of records available for consumption when next polled.
 */
@Internal
public class FanOutKinesisShardSubscription {
    private static final Logger LOG = LoggerFactory.getLogger(FanOutKinesisShardSubscription.class);
    private static final List<Class<? extends Throwable>> RECOVERABLE_EXCEPTIONS =
            Arrays.asList(
                    InternalFailureException.class,
                    ResourceNotFoundException.class,
                    KinesisException.class,
                    ResourceInUseException.class,
                    ReadTimeoutException.class,
                    TimeoutException.class,
                    IOException.class,
                    LimitExceededException.class);

    private final AsyncStreamProxy kinesis;
    private final String consumerArn;
    private final String shardId;

    private final Duration subscriptionTimeout;

    // Queue is meant for eager retrieval of records from the Kinesis stream. We will always have 2
    // record batches available on next read.
    private final BlockingQueue<SubscribeToShardEvent> eventQueue = new LinkedBlockingQueue<>(2);
    private final AtomicBoolean subscriptionActive = new AtomicBoolean(false);
    private final AtomicReference<Throwable> subscriptionException = new AtomicReference<>();
    private final AtomicBoolean activating = new AtomicBoolean(false);
    private static final ScheduledExecutorService TIMEOUT_SCHEDULER =
            new ScheduledThreadPoolExecutor(
                    1,
                    r -> {
                        Thread t = new Thread(r, "subscription-timeout-scheduler");
                        t.setDaemon(true);
                        return t;
                    });
    private volatile ScheduledFuture<?> timeoutFuture;

    // Store the current starting position for this subscription. Will be updated each time new
    // batch of records is consumed
    private volatile StartingPosition startingPosition;
    private FanOutShardSubscriber shardSubscriber;

    public FanOutKinesisShardSubscription(
            AsyncStreamProxy kinesis,
            String consumerArn,
            String shardId,
            StartingPosition startingPosition,
            Duration subscriptionTimeout) {
        this.kinesis = kinesis;
        this.consumerArn = consumerArn;
        this.shardId = shardId;
        this.startingPosition = startingPosition;
        this.subscriptionTimeout = subscriptionTimeout;
    }

    /** Method to allow eager activation of the subscription. */
    public void activateSubscription() {
        LOG.info(
                "Activating subscription to shard {} with starting position {} for consumer {}.",
                shardId,
                startingPosition,
                consumerArn);
        if (subscriptionActive.get()) {
            LOG.warn("Skipping activation of subscription since it is already active.");
            return;
        }
        if (!activating.compareAndSet(false, true)) {
            LOG.warn("Skipping activation of subscription since one is already in progress.");
            return;
        }

        shardSubscriber = new FanOutShardSubscriber();
        SubscribeToShardResponseHandler responseHandler =
                SubscribeToShardResponseHandler.builder()
                        .subscriber(() -> shardSubscriber)
                        .onError(
                                throwable -> {
                                    cancelTimeoutFuture();
                                    if (activating.compareAndSet(true, false)) {
                                        terminateSubscription(throwable);
                                    }
                                })
                        .build();

        // Cancel any previous timeout and schedule a new non-blocking timeout
        cancelTimeoutFuture();
        timeoutFuture =
                TIMEOUT_SCHEDULER.schedule(
                        () -> {
                            if (activating.compareAndSet(true, false)) {
                                String errorMessage =
                                        "Timeout when subscribing to shard "
                                                + shardId
                                                + " with starting position "
                                                + startingPosition
                                                + " for consumer "
                                                + consumerArn
                                                + ".";
                                LOG.error(errorMessage);
                                terminateSubscription(new TimeoutException(errorMessage));
                            }
                        },
                        subscriptionTimeout.toMillis(),
                        TimeUnit.MILLISECONDS);

        kinesis.subscribeToShard(consumerArn, shardId, startingPosition, responseHandler);
    }

    private void cancelTimeoutFuture() {
        if (timeoutFuture != null) {
            timeoutFuture.cancel(false);
        }
    }

    private void terminateSubscription(Throwable t) {
        if (!subscriptionException.compareAndSet(null, t)) {
            LOG.warn(
                    "Another subscription exception has been queued for shardId {}, ignoring subsequent exceptions",
                    shardId,
                    t);
        }
        shardSubscriber.cancel();
    }

    /**
     * This is the main entrypoint for this subscription class. It will retrieve the next batch of
     * records from the Kinesis stream shard. It will throw any unrecoverable exceptions encountered
     * during the subscription process.
     *
     * @return next FanOut subscription event containing records. Returns null if subscription is
     *     not yet active and fetching should be retried at a later time.
     */
    public SubscribeToShardEvent nextEvent() {
        Throwable throwable = subscriptionException.getAndSet(null);
        if (throwable != null) {
            // We don't want to wrap ResourceNotFoundExceptions because it is handled via a
            // try-catch loop
            if (throwable instanceof ResourceNotFoundException) {
                throw (ResourceNotFoundException) throwable;
            }
            Optional<? extends Throwable> recoverableException =
                    RECOVERABLE_EXCEPTIONS.stream()
                            .map(clazz -> ExceptionUtils.findThrowable(throwable, clazz))
                            .filter(Optional::isPresent)
                            .map(Optional::get)
                            .findFirst();
            if (recoverableException.isPresent()) {
                LOG.warn(
                        "Recoverable exception encountered for shard {} while subscribing to shard. Ignoring.",
                        shardId,
                        recoverableException.get());
                shardSubscriber.cancel();
                activateSubscription();
                return null;
            }
            LOG.error("Subscription encountered unrecoverable exception.", throwable);
            throw new KinesisStreamsSourceException(
                    "Subscription encountered unrecoverable exception.", throwable);
        }

        SubscribeToShardEvent event = eventQueue.poll();
        if (event != null) {
            return event;
        }

        if (!subscriptionActive.get()) {
            LOG.debug(
                    "Subscription to shard {} for consumer {} is not yet active. Skipping.",
                    shardId,
                    consumerArn);
        }
        return null;
    }

    /**
     * Implementation of {@link Subscriber} to retrieve events from Kinesis stream using Reactive
     * Streams.
     */
    private class FanOutShardSubscriber implements Subscriber<SubscribeToShardEventStream> {

        private Subscription subscription;

        public void requestRecords() {
            subscription.request(1);
        }

        public void cancel() {
            if (!subscriptionActive.get()) {
                LOG.warn("Trying to cancel inactive subscription. Ignoring.");
                return;
            }
            subscriptionActive.set(false);
            if (subscription != null) {
                subscription.cancel();
            }
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            LOG.info(
                    "Successfully subscribed to shard {} at {} using consumer {}.",
                    shardId,
                    startingPosition,
                    consumerArn);
            if (activating.compareAndSet(true, false)) {
                this.subscription = subscription;
                cancelTimeoutFuture();
                subscriptionActive.set(true);
                requestRecords();
            } else {
                // Another path (timeout/error) already won — cancel this subscription
                subscription.cancel();
            }
        }

        @Override
        public void onNext(SubscribeToShardEventStream subscribeToShardEventStream) {
            subscribeToShardEventStream.accept(
                    new SubscribeToShardResponseHandler.Visitor() {
                        @Override
                        public void visit(SubscribeToShardEvent event) {
                            try {
                                LOG.debug(
                                        "Received event: {}, {}",
                                        event.getClass().getSimpleName(),
                                        event);
                                eventQueue.put(event);

                                // Update the starting position in case we have to recreate the
                                // subscription
                                if (event.continuationSequenceNumber() == null) {
                                    // shard has ended
                                    LOG.info("Shard ended {}", shardId);
                                    startingPosition = null;
                                } else {
                                    startingPosition =
                                            StartingPosition.continueFromSequenceNumber(
                                                    event.continuationSequenceNumber());
                                }
                                // Replace the record just consumed in the Queue
                                requestRecords();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new KinesisStreamsSourceException(
                                        "Interrupted while adding Kinesis record to internal buffer.",
                                        e);
                            }
                        }
                    });
        }

        @Override
        public void onError(Throwable throwable) {
            if (!subscriptionException.compareAndSet(null, throwable)) {
                LOG.warn(
                        "Another subscription exception has been queued, ignoring subsequent exceptions",
                        throwable);
            }
        }

        @Override
        public void onComplete() {
            LOG.info("Subscription complete - {} ({})", shardId, consumerArn);
            cancel();
            if (startingPosition != null) {
                activateSubscription();
            }
        }
    }
}
