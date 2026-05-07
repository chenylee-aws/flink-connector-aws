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
    private static final ScheduledExecutorService TIMEOUT_SCHEDULER =
            new ScheduledThreadPoolExecutor(
                    1,
                    r -> {
                        Thread t = new Thread(r, "subscription-timeout-scheduler");
                        t.setDaemon(true);
                        return t;
                    });

    private final AsyncStreamProxy kinesis;
    private final String consumerArn;
    private final String shardId;
    private final Duration subscriptionTimeout;

    // Queue is meant for eager retrieval of records from the Kinesis stream. We will always have 2
    // record batches available on next read.
    private final BlockingQueue<SubscribeToShardEvent> eventQueue = new LinkedBlockingQueue<>(2);
    private final AtomicReference<Throwable> subscriptionException = new AtomicReference<>();

    // All fields below are guarded by lockObject
    private final Object lockObject = new Object();
    private ScheduledFuture<?> timeoutFuture;
    private FanOutShardSubscriber shardSubscriber;
    private boolean closed = false;

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

    /** Method to allow eager activation of the subscription. */
    public void activateSubscription() {
        synchronized (lockObject) {
            if (closed) {
                LOG.debug(
                        "Subscription for shard {} is closed; skipping activation.",
                        shardId);
                return;
            }
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
                                                "Error onError subscribing to shard {} with "
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
                                        setSubscriptionException(throwable);
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
                                    // The timeout future was cancelled between firing and
                                    // acquiring the lock (e.g. onSubscribe succeeded, or another
                                    // error path disposed the subscriber). Do nothing.
                                    if (timeoutFuture == null) {
                                        return;
                                    }
                                    if (!disposeIfActive(subscriber)) {
                                        return;
                                    }
                                }
                                setSubscriptionException(new TimeoutException(errorMessage));
                            },
                            subscriptionTimeout.toMillis(),
                            TimeUnit.MILLISECONDS);

            kinesis.subscribeToShard(consumerArn, shardId, startingPosition, responseHandler)
                    .exceptionally(
                            throwable -> {
                                LOG.error(
                                        "Error exceptionally subscribing to shard {} with starting position {} for "
                                                + "consumer {}. {}",
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
                                setSubscriptionException(throwable);
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

    private void setSubscriptionException(Throwable t) {
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

        return pollAndRequestNext();
    }

    /**
     * Poll the next buffered event and, if one was available, request the next event from the
     * server. This implements pull-based backpressure: the server is only asked for another event
     * once the consumer has made room in the queue, so the Netty event loop thread handling
     * {@code onNext} never needs to block.
     *
     * <p>Both the {@code poll()} and the {@code requestRecords()} must happen atomically under
     * {@code lockObject} with respect to {@code onSubscribe}. Otherwise the following race can
     * inflate pipeline depth beyond 1: (1) the consumer polls the leftover event outside the
     * lock, emptying the queue; (2) {@code onSubscribe} acquires the lock first, observes
     * {@code eventQueue.isEmpty() == true}, and issues its priming {@code request(1)};
     * (3) the consumer then acquires the lock and issues a second {@code request(1)}. Holding
     * the lock across both the poll and the request closes this window: either the consumer
     * drains+requests atomically (and {@code onSubscribe} later sees empty queue, skips its
     * request because by then {@code pollAndRequestNext} already requested), or
     * {@code onSubscribe} sees the non-empty queue and skips its request, leaving the request
     * to the subsequent consumer drain.
     */
    private SubscribeToShardEvent pollAndRequestNext() {
        synchronized (lockObject) {
            SubscribeToShardEvent event = eventQueue.poll();
            // if shardSubscriber it means that it either completed or disposed. In both case don't
            // request more record
            if (event != null && shardSubscriber != null) {
                shardSubscriber.requestRecords();
            }
            return event;
        }
    }

    /**
     * Implementation of {@link Subscriber} to retrieve events from Kinesis stream using Reactive
     * Streams.
     */
    private class FanOutShardSubscriber implements Subscriber<SubscribeToShardEventStream> {

        private Subscription subscription;

        public void requestRecords() {
            // subscription can be null if onSubscribe has not yet fired on a freshly activated
            // subscriber. In that case the initial request(1) will be issued from onSubscribe
            // itself, so it is safe to skip here.
            if (subscription != null) {
                subscription.request(1);
            }
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
                    // Timeout/error disposed this subscriber and a new one was created before SDK
                    // called onSubscribe
                    subscription.cancel();
                    return;
                }
                cancelTimeoutFuture();
                this.subscription = subscription;

                // Only issue the initial request(1) if the queue is empty. If this is a
                // reactivation after an error while leftover events from the previous
                // subscription are still buffered, we MUST NOT prime the pump here, otherwise
                // the pipeline depth would inflate: this request plus the request that
                // pollAndRequestNext() will issue when the consumer drains a leftover event
                // would both be outstanding, so the server could deliver more events than the
                // queue can hold. Skipping the request here is safe because the consumer's next
                // successful poll of a leftover event will trigger requestRecords() via
                // pollAndRequestNext(), which will issue the request(1) at that point.
                if (eventQueue.isEmpty()) {
                    requestRecords();
                } else {
                    LOG.debug(
                            "Shard {} reactivated with {} buffered event(s). Deferring initial "
                                    + "request(1) to the consumer-drain path to preserve the "
                                    + "max-one-in-flight invariant.",
                            shardId,
                            eventQueue.size());
                }
            }
        }

        @Override
        public void onNext(SubscribeToShardEventStream subscribeToShardEventStream) {
            subscribeToShardEventStream.accept(
                    new SubscribeToShardResponseHandler.Visitor() {
                        @Override
                        public void visit(SubscribeToShardEvent event) {
                            // Critical section: identity check, queue offer, and
                            // startingPosition update must all be atomic with respect to
                            // disposeIfActive()/activateSubscription() so that:
                            //   (1) we do not accept events from a subscriber that has already
                            //       been disposed (the AWS SDK can deliver onNext briefly after
                            //       subscription.cancel() until the cancel is honored on the
                            //       network side), and
                            //   (2) an in-progress offer is never observed by a newly activated
                            //       subscriber as an unexpected queue entry.
                            // Accepted events always belong to the currently active subscriber.
                            // Events from disposed subscribers are silently dropped; they will
                            // be re-delivered by the server when the replacement subscription
                            // resumes from the last confirmed startingPosition, so no data is
                            // lost (at-least-once semantics preserved).
                            synchronized (lockObject) {
                                if (shardSubscriber != FanOutShardSubscriber.this) {
                                    LOG.warn(
                                            "Ignoring late event for shard {} from a disposed "
                                                    + "subscriber; it will be re-delivered after "
                                                    + "reactivation.",
                                            shardId);
                                    return;
                                }

                                LOG.debug(
                                        "Received event: {}, {}",
                                        event.getClass().getSimpleName(),
                                        event);

                                // Non-blocking offer. Under the request-one-after-drain
                                // discipline (onSubscribe requests only when the queue is empty,
                                // and subsequent request(1) calls come from the consumer drain
                                // path in nextEvent()), there is at most one event in flight per
                                // subscriber at any time, so the queue is guaranteed to have
                                // room. If offer() ever returns false it indicates a protocol /
                                // state invariant violation (e.g. the server delivered an
                                // unrequested event) - fail loud rather than block the Netty
                                // event loop. The subscription will be reactivated from the
                                // previous startingPosition (which has not yet been advanced
                                // below) and the server will re-deliver this event.
                                if (!eventQueue.offer(event)) {
                                    LOG.error(
                                            "Event queue overflow for shard {}; server delivered "
                                                    + "an unrequested event. Failing subscription "
                                                    + "to recover.",
                                            shardId);
                                    // Dispose first, then set the exception only if disposal
                                    // succeeded (i.e., this subscriber was still the active one).
                                    // This matches the convention used by onError, the timeout
                                    // path, and the activateSubscription exceptionally handler,
                                    // and prevents a stale subscriber from overwriting the
                                    // subscriptionException slot with a no-longer-relevant error.
                                    if (disposeIfActive(FanOutShardSubscriber.this)) {
                                        setSubscriptionException(
                                                new IOException(
                                                        "Event queue overflow for shard "
                                                                + shardId
                                                                + "; server delivered an "
                                                                + "unrequested event."));
                                    }
                                    return;
                                }

                                // Update the starting position in case we have to recreate the
                                // subscription. Done only after a successful offer so that an
                                // overflow triggers a resubscribe from the previous position and
                                // the rejected event is re-delivered by the server.
                                if (event.continuationSequenceNumber() == null) {
                                    startingPosition = null;
                                } else {
                                    startingPosition =
                                            StartingPosition.continueFromSequenceNumber(
                                                    event.continuationSequenceNumber());
                                }

                                // NOTE: we intentionally do NOT call requestRecords() here. The
                                // next request(1) is issued from nextEvent() after the consumer
                                // drains the queue. This implements pull-based backpressure and
                                // prevents this Netty event-loop thread from blocking, which
                                // would otherwise stall every other HTTP/2 stream multiplexed
                                // onto the same channel.
                            }
                        }
                    });
        }

        @Override
        public void onError(Throwable throwable) {
            synchronized (lockObject) {
                if (!disposeIfActive(this)) {
                    return;
                }
            }
            setSubscriptionException(throwable);
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

    public void close() {
        synchronized (lockObject) {
            closed = true;
            if (shardSubscriber != null) {
                disposeIfActive(shardSubscriber);
            }
        }
    }
}
