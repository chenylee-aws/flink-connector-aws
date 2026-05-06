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

import org.apache.flink.connector.kinesis.source.exception.KinesisStreamsSourceException;
import org.apache.flink.connector.kinesis.source.proxy.AsyncStreamProxy;
import org.apache.flink.connector.kinesis.source.split.StartingPosition;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEventStream;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.connector.kinesis.source.util.TestUtil.CONSUMER_ARN;
import static org.apache.flink.connector.kinesis.source.util.TestUtil.SHARD_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Tests for {@link FanOutKinesisShardSubscription}.
 *
 * <p>These tests focus on the concurrent lifecycle of a subscription: activation guards, error
 * disposal, timeout handling, and the identity-based cleanup that prevents the connection-leak
 * bug.
 */
class FanOutKinesisShardSubscriptionTest {

    private static final Duration DEFAULT_TIMEOUT = Duration.ofMillis(500);
    private static final Duration LONG_TIMEOUT = Duration.ofSeconds(10);

    // ----- Happy path -----

    @Test
    void nextEventReturnsNullBeforeActivation() {
        ScriptedProxy proxy = new ScriptedProxy();
        FanOutKinesisShardSubscription subscription = newSubscription(proxy);

        // Before activateSubscription is called, nextEvent returns null
        assertThat(subscription.nextEvent()).isNull();
        assertThat(proxy.subscribeCallCount()).isEqualTo(0);
    }

    @Test
    void activatesAndDeliversEvents() throws Exception {
        ScriptedProxy proxy = new ScriptedProxy();
        FanOutKinesisShardSubscription subscription = newSubscription(proxy);

        subscription.activateSubscription();
        ScriptedSubscription s1 = proxy.awaitSubscription();

        // Deliver onSubscribe and an event with records
        s1.onSubscribeDelivered();
        SubscribeToShardEvent event =
                SubscribeToShardEvent.builder()
                        .records(record("seq-1"))
                        .continuationSequenceNumber("cont-1")
                        .build();
        s1.deliverEvent(event);

        SubscribeToShardEvent received = pollEvent(subscription);
        assertThat(received).isNotNull();
        assertThat(received.continuationSequenceNumber()).isEqualTo("cont-1");
    }

    @Test
    void onCompleteTriggersReactivationForOngoingShard() throws Exception {
        ScriptedProxy proxy = new ScriptedProxy();
        FanOutKinesisShardSubscription subscription = newSubscription(proxy);

        subscription.activateSubscription();
        ScriptedSubscription s1 = proxy.awaitSubscription();
        s1.onSubscribeDelivered();
        // deliver an event with a non-null continuation so the shard is not at end
        s1.deliverEvent(
                SubscribeToShardEvent.builder()
                        .records(record("seq-1"))
                        .continuationSequenceNumber("cont-1")
                        .build());
        pollEvent(subscription);

        // Natural end of subscription triggers EFO rotation
        s1.onComplete();

        // A second subscribeToShard call must occur
        ScriptedSubscription s2 = proxy.awaitSubscription();
        assertThat(proxy.subscribeCallCount()).isEqualTo(2);
        // The new activation should resume from where s1 left off
        assertThat(s2.startingPosition).isEqualTo(
                StartingPosition.continueFromSequenceNumber("cont-1"));
    }

    @Test
    void shardEndPreventsReactivation() throws Exception {
        ScriptedProxy proxy = new ScriptedProxy();
        FanOutKinesisShardSubscription subscription = newSubscription(proxy);

        subscription.activateSubscription();
        ScriptedSubscription s1 = proxy.awaitSubscription();
        s1.onSubscribeDelivered();
        // Deliver event with null continuation number to signal shard end
        s1.deliverEvent(
                SubscribeToShardEvent.builder()
                        .records(record("seq-1"))
                        .continuationSequenceNumber(null)
                        .build());
        pollEvent(subscription);

        // onComplete normally triggers reactivation, but not when startingPosition is null
        s1.onComplete();

        // There should be no further subscribe attempts.
        // We give the scheduler a moment to make any mistake visible.
        waitShort();
        assertThat(proxy.subscribeCallCount()).isEqualTo(1);
    }

    // ----- Concurrent activation prevention (main bug fix) -----

    @Test
    void activateIsNoOpWhenSubscriptionAlreadyInFlight() throws Exception {
        ScriptedProxy proxy = new ScriptedProxy();
        // Important: use a long timeout so the first subscribe doesn't time out mid-test
        FanOutKinesisShardSubscription subscription = newSubscription(proxy, LONG_TIMEOUT);

        subscription.activateSubscription();
        proxy.awaitSubscription();
        assertThat(proxy.subscribeCallCount()).isEqualTo(1);

        // Now call activateSubscription again. The first subscription has not received
        // onSubscribe yet, so the broken main-branch guard (subscriptionActive) would
        // allow a second subscribe to fire. The fixed guard (shardSubscriber != null)
        // must block it.
        subscription.activateSubscription();
        subscription.activateSubscription();
        subscription.activateSubscription();

        waitShort();
        assertThat(proxy.subscribeCallCount()).isEqualTo(1);
    }

    @Test
    void activateIsNoOpAfterOnSubscribeSucceeds() throws Exception {
        ScriptedProxy proxy = new ScriptedProxy();
        FanOutKinesisShardSubscription subscription = newSubscription(proxy, LONG_TIMEOUT);

        subscription.activateSubscription();
        ScriptedSubscription s1 = proxy.awaitSubscription();
        s1.onSubscribeDelivered();

        subscription.activateSubscription();
        subscription.activateSubscription();

        waitShort();
        assertThat(proxy.subscribeCallCount()).isEqualTo(1);
    }

    // ----- Error handling and retries -----

    @Test
    void recoverableErrorTriggersRetry() throws Exception {
        ScriptedProxy proxy = new ScriptedProxy();
        FanOutKinesisShardSubscription subscription = newSubscription(proxy);

        subscription.activateSubscription();
        ScriptedSubscription s1 = proxy.awaitSubscription();

        // Fail the subscription via the future path (common for acquire timeouts)
        s1.completeExceptionally(new IOException("simulated connection reset"));

        // nextEvent must drain the exception, classify as recoverable, and retry
        SubscribeToShardEvent e = subscription.nextEvent();
        assertThat(e).isNull();

        ScriptedSubscription s2 = proxy.awaitSubscription();
        assertThat(s2).isNotSameAs(s1);
        assertThat(proxy.subscribeCallCount()).isEqualTo(2);
    }

    @Test
    void unrecoverableErrorPropagatesFromNextEvent() throws Exception {
        ScriptedProxy proxy = new ScriptedProxy();
        FanOutKinesisShardSubscription subscription = newSubscription(proxy);

        subscription.activateSubscription();
        ScriptedSubscription s1 = proxy.awaitSubscription();

        // Use a non-recoverable runtime exception
        s1.completeExceptionally(new RuntimeException("nope"));

        assertThatThrownBy(subscription::nextEvent)
                .isInstanceOf(KinesisStreamsSourceException.class)
                .hasMessageContaining("unrecoverable");
    }

    @Test
    void resourceNotFoundIsRethrownDirectly() throws Exception {
        ScriptedProxy proxy = new ScriptedProxy();
        FanOutKinesisShardSubscription subscription = newSubscription(proxy);

        subscription.activateSubscription();
        ScriptedSubscription s1 = proxy.awaitSubscription();

        s1.completeExceptionally(ResourceNotFoundException.builder().message("gone").build());

        assertThatThrownBy(subscription::nextEvent)
                .isInstanceOf(ResourceNotFoundException.class);
    }

    // ----- Dual error path dedup (disposeIfActive identity check) -----

    @Test
    void dualErrorPathQueuesAtMostOneException() throws Exception {
        ScriptedProxy proxy = new ScriptedProxy();
        FanOutKinesisShardSubscription subscription = newSubscription(proxy);

        subscription.activateSubscription();
        ScriptedSubscription s1 = proxy.awaitSubscription();

        // Both error signals fire for the same subscribe (as AWS SDK does in practice)
        s1.fireHandlerError(new IOException("handler error"));
        s1.completeExceptionally(new IOException("future error"));

        // First nextEvent drains exactly one exception and retries
        assertThat(subscription.nextEvent()).isNull();

        // After retry, there must be exactly ONE new subscribe (not one per error signal)
        ScriptedSubscription s2 = proxy.awaitSubscription();
        assertThat(s2).isNotSameAs(s1);
        assertThat(proxy.subscribeCallCount()).isEqualTo(2);

        // Second nextEvent must not find a lingering exception from the losing error path
        assertThat(subscription.nextEvent()).isNull();
        waitShort();
        // Still only 2 subscribes (no runaway retry)
        assertThat(proxy.subscribeCallCount()).isEqualTo(2);
    }

    // ----- Subscription timeout handling -----

    @Test
    void subscriptionTimeoutTriggersCleanupAndRetry() throws Exception {
        ScriptedProxy proxy = new ScriptedProxy();
        FanOutKinesisShardSubscription subscription =
                newSubscription(proxy, Duration.ofMillis(100));

        subscription.activateSubscription();
        ScriptedSubscription s1 = proxy.awaitSubscription();
        // Deliberately do NOT call s1.onSubscribeDelivered() — the latch will time out

        // Wait for the timeout to fire and queue a TimeoutException
        await().atMost(Duration.ofSeconds(2)).untilAsserted(
                () -> {
                    SubscribeToShardEvent e = subscription.nextEvent();
                    // After the timeout, nextEvent should drain the exception and fire a retry.
                    // Keep polling until the retry shows up.
                    assertThat(proxy.subscribeCallCount()).isGreaterThanOrEqualTo(2);
                    assertThat(e).isNull();
                });
    }

    // ----- Stale onSubscribe (race: timeout before onSubscribe) -----

    @Test
    void lateOnSubscribeOnStaleSubscriberIsCancelled() throws Exception {
        ScriptedProxy proxy = new ScriptedProxy();
        FanOutKinesisShardSubscription subscription =
                newSubscription(proxy, Duration.ofMillis(100));

        subscription.activateSubscription();
        ScriptedSubscription s1 = proxy.awaitSubscription();
        // Let the timeout fire (no onSubscribe)
        await().atMost(Duration.ofSeconds(2))
                .until(() -> subscription.nextEvent() == null
                        && proxy.subscribeCallCount() >= 2);

        // Now s1 is stale; a new subscription s2 has been created
        ScriptedSubscription s2 = proxy.latestSubscription();
        assertThat(s2).isNotSameAs(s1);

        // Late onSubscribe delivery on the STALE subscriber must result in its
        // Subscription being cancelled (to free the underlying HTTP/2 stream slot).
        s1.onSubscribeDelivered();
        assertThat(s1.subscription.isCancelled()).isTrue();
    }

    // ----- Test helpers -----

    private static FanOutKinesisShardSubscription newSubscription(AsyncStreamProxy proxy) {
        return newSubscription(proxy, DEFAULT_TIMEOUT);
    }

    private static FanOutKinesisShardSubscription newSubscription(
            AsyncStreamProxy proxy, Duration subscriptionTimeout) {
        return new FanOutKinesisShardSubscription(
                proxy, CONSUMER_ARN, SHARD_ID, StartingPosition.fromStart(), subscriptionTimeout);
    }

    private static Record record(String sequenceNumber) {
        return Record.builder().sequenceNumber(sequenceNumber).partitionKey("pk").build();
    }

    private static SubscribeToShardEvent pollEvent(FanOutKinesisShardSubscription subscription) {
        return await().atMost(Duration.ofSeconds(2))
                .until(subscription::nextEvent, e -> e != null);
    }

    private static void waitShort() throws InterruptedException {
        // Give the async machinery a window to do anything incorrect.
        // Not a timing assertion — just bounded patience for "nothing should happen".
        Thread.sleep(100);
    }

    // ----- Programmable fake AsyncStreamProxy -----

    /**
     * A fake {@link AsyncStreamProxy} that records subscribe calls and exposes each one via a
     * {@link ScriptedSubscription} handle for the test to drive events and errors.
     */
    private static final class ScriptedProxy implements AsyncStreamProxy {
        private final ConcurrentLinkedQueue<ScriptedSubscription> calls = new ConcurrentLinkedQueue<>();

        @Override
        public CompletableFuture<Void> subscribeToShard(
                String consumerArn,
                String shardId,
                StartingPosition startingPosition,
                SubscribeToShardResponseHandler responseHandler) {
            ScriptedSubscription call =
                    new ScriptedSubscription(startingPosition, responseHandler);
            calls.add(call);
            return call.future;
        }

        @Override
        public void close() {}

        int subscribeCallCount() {
            return calls.size();
        }

        ScriptedSubscription awaitSubscription() {
            return await().atMost(Duration.ofSeconds(2))
                    .until(
                            () -> {
                                // Return the last unseen call
                                for (ScriptedSubscription c : calls) {
                                    if (!c.observed) {
                                        c.observed = true;
                                        return c;
                                    }
                                }
                                return null;
                            },
                            c -> c != null);
        }

        ScriptedSubscription latestSubscription() {
            ScriptedSubscription last = null;
            for (ScriptedSubscription c : calls) {
                last = c;
            }
            return last;
        }
    }

    /** A handle for the test to drive a specific {@code subscribeToShard} call. */
    private static final class ScriptedSubscription {
        final StartingPosition startingPosition;
        final SubscribeToShardResponseHandler handler;
        final CompletableFuture<Void> future = new CompletableFuture<>();
        final TestSubscription subscription = new TestSubscription();
        volatile boolean observed = false;

        private final AtomicReference<Subscriber<? super SubscribeToShardEventStream>> subscriberRef =
                new AtomicReference<>();

        ScriptedSubscription(
                StartingPosition startingPosition, SubscribeToShardResponseHandler handler) {
            this.startingPosition = startingPosition;
            this.handler = handler;
            // Start the event stream handshake: the handler.onEventStream callback tells us
            // the Subscriber instance we should deliver signals to.
            handler.onEventStream(subscriberRef::set);
        }

        void onSubscribeDelivered() {
            Subscriber<? super SubscribeToShardEventStream> s = awaitSubscriber();
            s.onSubscribe(subscription);
        }

        void deliverEvent(SubscribeToShardEvent event) {
            Subscriber<? super SubscribeToShardEventStream> s = awaitSubscriber();
            // SubscribeToShardEvent itself implements SubscribeToShardEventStream, so we can
            // pass it directly. Its accept() dispatches to visitor.visit(this).
            s.onNext(event);
        }

        void onComplete() {
            Subscriber<? super SubscribeToShardEventStream> s = awaitSubscriber();
            s.onComplete();
        }

        void fireHandlerError(Throwable t) {
            handler.exceptionOccurred(t);
        }

        void completeExceptionally(Throwable t) {
            future.completeExceptionally(t);
        }

        private Subscriber<? super SubscribeToShardEventStream> awaitSubscriber() {
            return await().atMost(Duration.ofSeconds(2)).until(subscriberRef::get, s -> s != null);
        }
    }

    /** A {@link Subscription} that records whether cancel was called. */
    private static final class TestSubscription implements Subscription {
        private final AtomicInteger requested = new AtomicInteger(0);
        private final AtomicBoolean cancelled = new AtomicBoolean(false);

        @Override
        public void request(long n) {
            requested.addAndGet((int) n);
        }

        @Override
        public void cancel() {
            cancelled.set(true);
        }

        boolean isCancelled() {
            return cancelled.get();
        }
    }
}
