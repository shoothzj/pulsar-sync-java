/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.github.shoothzj.pulsar.sync.core;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
public class PulsarPartitionSyncWorker {

    private final PulsarHandle pulsarHandle;

    private final SyncConfig syncConfig;

    private final ScheduledExecutorService scheduledExecutor;

    private final TenantNamespaceTopic tenantNamespaceTopic;

    private ScheduledFuture<?> syncFuture;

    private ScheduledFuture<?> syncCursorFuture;

    private volatile boolean subscribeSuccess;

    private volatile Consumer<byte[]> consumer;

    private volatile boolean produceSuccess;

    private volatile Producer<byte[]> producer;

    private volatile long lastRecordTime;

    private final Map<String, String> cursorSet = new ConcurrentHashMap<>();

    private final Cache<MessageId, MessageId> messageIdMap = CacheBuilder.newBuilder()
            .expireAfterWrite(Duration.ofMinutes(1)).build();

    private final SparseMap<MessageId, MessageId> sparseMessageIdMap;

    private final Map<CursorPosition, CursorPosition> cursorPositionSet = new ConcurrentHashMap<>();

    public PulsarPartitionSyncWorker(PulsarHandle pulsarHandle, SyncConfig syncConfig,
                                     ScheduledExecutorService scheduledExecutor,
                                     TenantNamespaceTopic tenantNamespaceTopic) {
        this.pulsarHandle = pulsarHandle;
        this.syncConfig = syncConfig;
        this.scheduledExecutor = scheduledExecutor;
        this.tenantNamespaceTopic = tenantNamespaceTopic;
        this.sparseMessageIdMap = new SparseMap<>(TimeUnit.HOURS.toNanos(1));
    }

    public void start() {
        this.syncFuture =
                this.scheduledExecutor.scheduleWithFixedDelay(this::sync, 0, 1, TimeUnit.MINUTES);
        this.syncCursorFuture =
                this.scheduledExecutor.scheduleWithFixedDelay(this::syncCursor, 0, 10, TimeUnit.SECONDS);
    }

    private void sync() {
        try {
            if (!produceSuccess) {
                Producer<byte[]> producer = pulsarHandle.dstClient().newProducer()
                        .topic(tenantNamespaceTopic.topic())
                        .create();
                log.info("{} create sync producer success", tenantNamespaceTopic);
                this.producer = producer;
                produceSuccess = true;
            }
            if (produceSuccess) {
                if (!subscribeSuccess) {
                    Consumer<byte[]> consumer = pulsarHandle.srcClient().newConsumer()
                            .topic(tenantNamespaceTopic.topic())
                            .subscriptionName(syncConfig.getSubscriptionName())
                            .subscriptionType(SubscriptionType.Failover)
                            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                            .autoUpdatePartitions(false)
                            .messageListener((MessageListener<byte[]>) this::received)
                            .subscribe();
                    log.info("{} subscribe sync consumer success", tenantNamespaceTopic);
                    this.consumer = consumer;
                    subscribeSuccess = true;
                    this.syncFuture.cancel(true);
                }
            }
        } catch (Exception e) {
            log.error("{} failed to init ", tenantNamespaceTopic, e);
        }
    }

    private void syncCursor() {
        pulsarHandle.srcAdmin().topics()
                .getInternalStatsAsync(tenantNamespaceTopic.topic())
                .whenComplete((internalStats, throwable) -> {
                    if (throwable != null) {
                        log.error("{} failed to get internal info", tenantNamespaceTopic, throwable);
                        return;
                    }
                    if (internalStats == null) {
                        log.error("{} internal info is null", tenantNamespaceTopic);
                        return;
                    }
                    if (internalStats.cursors == null) {
                        log.error("{} internal info cursor is null", tenantNamespaceTopic);
                        return;
                    }
                    for (Map.Entry<String, ManagedLedgerInternalStats.CursorStats> entry :
                            internalStats.cursors.entrySet()) {
                        updateCursors(entry.getKey(), entry.getValue());
                    }
                });
    }

    private void updateCursors(String cursor, ManagedLedgerInternalStats.CursorStats cursorStats) {
        MessageId messageId;
        try {
            messageId = MessageId.fromByteArray(cursorStats.readPosition.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            log.error("{} failed to parse cursor {} position {}", tenantNamespaceTopic, cursor,
                    cursorStats.readPosition, e);
            return;
        }
        cursorSet.computeIfAbsent(cursor, k -> {
            String topic = tenantNamespaceTopic.topic();
            MessageId newMsgId = findMessageId(messageId);
            if (newMsgId == null) {
                return null;
            }
            pulsarHandle.dstAdmin().topics().createSubscriptionAsync(topic, cursor, newMsgId)
                    .whenComplete((ignore, throwable) -> {
                if (throwable != null) {
                    log.error("{} failed to create cursor {} position {}", tenantNamespaceTopic, cursor,
                            cursorStats.readPosition, throwable);
                    return;
                }
                log.info("{} create cursor {} position {}", tenantNamespaceTopic, cursor,
                        cursorStats.readPosition);
            });
            cursorPositionSet.put(new CursorPosition(cursor, messageId), new CursorPosition(cursor, newMsgId));
            return cursor;
        });
        if (cursorPositionSet.containsKey(new CursorPosition(cursor, messageId))) {
            return;
        }
        pulsarHandle.dstAdmin().topics().getSubscriptionsAsync(tenantNamespaceTopic.topic())
                .whenComplete((subscriptions, throwable) -> {
                    if (throwable != null) {
                        log.error("{} failed to get subscriptions", tenantNamespaceTopic, throwable);
                        return;
                    }
                    if (subscriptions == null) {
                        log.error("{} subscriptions is null", tenantNamespaceTopic);
                        return;
                    }
                    if (subscriptions.contains(cursor)) {
                        // if the dst topic has the cursor, we don't need to update the cursor position
                        return;
                    }
                    MessageId newMsgId = findMessageId(messageId);
                    if (newMsgId == null) {
                        return;
                    }
                    pulsarHandle.dstAdmin().topics().resetCursorAsync(tenantNamespaceTopic.topic(), cursor, newMsgId)
                            .whenComplete((ignore, throwable1) -> {
                        if (throwable1 != null) {
                            log.error("{} failed to create cursor {} position {}", tenantNamespaceTopic, cursor,
                                    cursorStats.readPosition, throwable1);
                            return;
                        }
                        log.info("{} create cursor {} position {}", tenantNamespaceTopic, cursor,
                                cursorStats.readPosition);
                    });
                });
    }

    private MessageId findMessageId(MessageId messageId) {
        MessageId result = messageIdMap.getIfPresent(messageId);
        if (result != null) {
            return result;
        }
        // expire sparse map by time
        return sparseMessageIdMap.get(messageId);
    }

    private void received(Consumer<byte[]> consumer, Message<byte[]> msg) {
        TypedMessageBuilder<byte[]> typedMessageBuilder = producer.newMessage();
        typedMessageBuilder.value(msg.getValue());
        if (msg.getEventTime() != 0) {
            typedMessageBuilder.eventTime(msg.getEventTime());
        }
        if (msg.getKey() != null) {
            typedMessageBuilder.key(msg.getKey());
        }
        typedMessageBuilder
                .properties(msg.getProperties())
                .sendAsync()
                .whenComplete((messageId, throwable) -> {
                    if (throwable != null) {
                        log.error("{} failed to send message", tenantNamespaceTopic, throwable);
                    } else {
                        if (System.nanoTime() - lastRecordTime > TimeUnit.SECONDS.toNanos(60)) {
                            sparseMessageIdMap.put(msg.getMessageId(), messageId);
                            lastRecordTime = System.nanoTime();
                        }
                        messageIdMap.put(msg.getMessageId(), messageId);
                        log.debug("{} send message success", tenantNamespaceTopic);
                        consumer.acknowledgeAsync(msg);
                    }
                });
    }

    public void close() {
        if (syncFuture != null) {
            syncFuture.cancel(true);
        }
        if (syncCursorFuture != null) {
            syncCursorFuture.cancel(true);
        }
        if (consumer != null) {
            consumer.closeAsync();
        }
        if (producer != null) {
            producer.closeAsync();
        }
    }
}
