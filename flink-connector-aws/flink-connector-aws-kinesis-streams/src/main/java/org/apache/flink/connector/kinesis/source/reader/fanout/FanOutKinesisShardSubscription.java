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
    private final AtomicReference<Throwable> subscriptionException = new AtomicReference<>();
    private static final ScheduledExecutorService TIMEOUT_SCHEDULER =
            new ScheduledThreadPoolExecutor(
                    1,
                    r -> {
                        Thread t = new Thread(r, "subscription-timeout-scheduler");
                        t.setDaemon(true);
                        return t;
                    });

    // All fields below are guarded by lockObject
    private final Object lockObject = new Object();
    private ScheduledFuture<?> timeoutFuture;
    private FanOutShardSubscriber shardSubscriber;

    // Written by onNext (Netty thread), read by activateSubscription (Data Fetcher thread)
    private volatile StartingPosition startingPosition;

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

    /**
     * Method to allow eager activation of the subscription.
     */
    public void activateSubscription() {
        synchronized (lockObject) {
            if (startingPosition == null) {
                LOG.info(
                        "Shard {} has been completely consumed (shard end). Skipping re-subscription.",
                        shardId);
                return;
            }
            if (shardSubscriber != null) {
                LOG.warn(
                        "Shard {} Skipping activation of subscription since one is already active or in progress.",
                        shardId);
                return;
            }

            LOG.info(
                    "Activating subscription to shard {} with starting position {} for consumer {}.",
                    shardId,
                    startingPosition,
                    consumerArn);

            FanOutShardSubscriber subscriber = new FanOutShardSubscriber();
            shardSubscriber = subscriber;

            SubscribeToShardResponseHandler responseHandler =
                    SubscribeToShardResponseHandler.builder()
                            .subscriber(() -> subscriber)
                            .onError(
                                    throwable -> {
                                        LOG.error(
                                                "Error (OnError) subscribing to shard {} with "
                                                        + "starting position {} for consumer {} {}.",
                                                shardId,
                                                startingPosition,
                                                consumerArn,
                                                subscriber,
                                                throwable);
                                        synchronized (lockObject) {
                                            if (!disposeIfActive(subscriber)) {
                                                return;
                                            }
                                        }
                                        terminateSubscription(throwable);
                                    })
                            .build();

            cancelTimeoutFuture();
            timeoutFuture =
                    TIMEOUT_SCHEDULER.schedule(
                            () -> {
                                String errorMessage =
                                        "Timeout when subscribing to shard "
                                                + shardId
                                                + " with starting position "
                                                + startingPosition
                                                + " for consumer "
                                                + consumerArn
                                                + " for sub "
                                                + subscriber
                                                + ".";
                                LOG.error(errorMessage);
                                synchronized (lockObject) {
                                    if (!disposeIfActive(subscriber)) {
                                        return;
                                    }
                                }
                                terminateSubscription(new TimeoutException(errorMessage));
                            },
                            subscriptionTimeout.toMillis(),
                            TimeUnit.MILLISECONDS);

            kinesis.subscribeToShard(consumerArn, shardId, startingPosition, responseHandler)
                    .exceptionally(
                            throwable -> {
                                LOG.error(
                                        "Error subscribing to shard {} with starting position {} for consumer {}. {}",
                                        shardId,
                                        startingPosition,
                                        consumerArn,
                                        subscriber,
                                        throwable);
                                synchronized (lockObject) {
                                    if (!disposeIfActive(subscriber)) {
                                        return null;
                                    }
                                }
                                terminateSubscription(throwable);
                                return null;
                            });
        }
    }

    // Must be called while holding lockObject
    private void cancelTimeoutFuture() {
        if (timeoutFuture != null) {
            timeoutFuture.cancel(false);
            timeoutFuture = null;
        }
    }

    // Must be called while holding lockObject
    private boolean disposeIfActive(FanOutShardSubscriber subscriber) {
        if (shardSubscriber != subscriber) {
            return false;
        }
        cancelTimeoutFuture();
        shardSubscriber.cancelSubscription();
        shardSubscriber = null;
        return true;
    }

    private void terminateSubscription(Throwable t) {
        if (!subscriptionException.compareAndSet(null, t)) {
            LOG.warn(
                    "Another subscription exception has been queued for shardId {}, ignoring subsequent exceptions",
                    shardId,
                    t);
        }
    }

    /**
     * This is the main entrypoint for this subscription class. It will retrieve the next batch of
     * records from the Kinesis stream shard. It will throw any unrecoverable exceptions encountered
     * during the subscription process.
     *
     * @return next FanOut subscription event containing records. Returns null if subscription is
     * not yet active and fetching should be retried at a later time.
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
                        "Recoverable exception encountered for shard {} while subscribing to shard. Ignoring: {}",
                        shardId,
                        recoverableException.get());
                // TODO: add backoff for LimitExceededException and ResourceInUseException
                activateSubscription();
                return null;
            }
            LOG.error("Subscription encountered unrecoverable exception. {}", shardId, throwable);
            throw new KinesisStreamsSourceException(
                    "Subscription encountered unrecoverable exception.", throwable);
        }

        return eventQueue.poll();
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

        public void cancelSubscription() {
            if (subscription != null) {
                subscription.cancel();
            }
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            synchronized (lockObject) {
                LOG.info(
                        "Successfully subscribed to shard {} at {} using consumer {}.",
                        shardId,
                        startingPosition,
                        consumerArn);
                if (shardSubscriber != this) {
                    // Another path (timeout/error) already won — cancel this subscription
                    subscription.cancel();
                    return;
                }
                cancelTimeoutFuture();
                this.subscription = subscription;
                requestRecords();
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
            synchronized (lockObject) {
                LOG.info("Subscription complete - {} ({})", shardId, consumerArn);
                shardSubscriber = null;
            }
            activateSubscription();
        }
    }
}
