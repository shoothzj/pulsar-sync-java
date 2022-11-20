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

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
public class PulsarPartitionTopicSyncManager {

    private final PulsarHandle pulsarHandle;

    private final SyncConfig syncConfig;

    private final ScheduledExecutorService scheduledExecutor;

    private final TenantNamespace tenantNamespace;

    private ScheduledFuture<?> scheduledFuture;

    private final Map<TenantNamespaceTopic, PulsarPartitionedTopicSyncWorker> map = new ConcurrentHashMap<>();

    public PulsarPartitionTopicSyncManager(PulsarHandle pulsarHandle,
                                           SyncConfig syncConfig, ScheduledExecutorService scheduledExecutor,
                                           TenantNamespace tenantNamespace) {
        this.pulsarHandle = pulsarHandle;
        this.syncConfig = syncConfig;
        this.scheduledExecutor = scheduledExecutor;
        this.tenantNamespace = tenantNamespace;
    }

    public void start() {
        if (syncConfig.isAutoUpdateTopic()) {
            this.scheduledFuture = scheduledExecutor
                    .scheduleWithFixedDelay(this::sync, 0, 3, TimeUnit.MINUTES);
        } else {
            sync();
        }
    }

    public void sync() {
        getPartitionedTopicListAsync(tenantNamespace.namespace()).exceptionally(throwable -> {
            log.error("failed to get [{}] partitioned topic list", tenantNamespace.namespace(), throwable);
            return null;
        }).thenAccept(topics -> {
            for (String topic : topics) {
                getPartitionedTopicMetadataAsync(topic).exceptionally(throwable -> {
                    log.error("Failed to get partitioned topic [{}] metadata", topic, throwable);
                    return null;
                }).thenAccept(metadata -> {
                    CompletableFuture<Void> partitionedTopicAsync = createPartitionedTopicAsync(topic,
                            metadata.partitions, metadata.properties);
                    partitionedTopicAsync.whenComplete((unused, throwable) -> {
                        if (throwable == null || throwable instanceof PulsarAdminException.ConflictException) {
                            TenantNamespaceTopic topicObj = new TenantNamespaceTopic(tenantNamespace, topic);
                            log.info("begin to start partitioned topic sync worker [{}]", topicObj);
                            map.computeIfAbsent(topicObj, k -> startTopicSyncWorker(topicObj));
                        } else {
                            log.error("Failed to create partitioned topic [{}]", topic, throwable);
                        }
                    });
                });
            }
        });
    }

    private CompletableFuture<List<String>> getPartitionedTopicListAsync(String namespace) {
        return pulsarHandle.srcAdmin().topics().getPartitionedTopicListAsync(namespace);
    }

    private CompletableFuture<PartitionedTopicMetadata> getPartitionedTopicMetadataAsync(String topic) {
        return pulsarHandle.srcAdmin().topics().getPartitionedTopicMetadataAsync(topic);
    }

    private CompletableFuture<Void> createPartitionedTopicAsync(String topic, int numPartitions,
                                                                Map<String, String> properties) {
        return pulsarHandle.dstAdmin().topics().createPartitionedTopicAsync(topic, numPartitions, properties);
    }

    public PulsarPartitionedTopicSyncWorker startTopicSyncWorker(TenantNamespaceTopic tenantNamespaceTopic) {
        PulsarPartitionedTopicSyncWorker worker = new PulsarPartitionedTopicSyncWorker(pulsarHandle,
                syncConfig, scheduledExecutor, tenantNamespaceTopic);
        worker.start();
        return worker;
    }

    public void close() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(true);
        }
        map.values().forEach(PulsarPartitionedTopicSyncWorker::close);
    }
}
