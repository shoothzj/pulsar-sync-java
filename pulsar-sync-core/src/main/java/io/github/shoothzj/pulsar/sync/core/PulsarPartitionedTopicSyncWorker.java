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
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
public class PulsarPartitionedTopicSyncWorker {

    private final PulsarHandle pulsarHandle;

    private final SyncConfig syncConfig;

    private final ScheduledExecutorService scheduledExecutor;

    private final TenantNamespaceTopic tenantNamespaceTopic;

    private ScheduledFuture<?> scheduledFuture;

    private final Map<Integer, PulsarPartitionSyncWorker> map = new ConcurrentHashMap<>();

    public PulsarPartitionedTopicSyncWorker(PulsarHandle pulsarHandle, SyncConfig syncConfig,
                                            ScheduledExecutorService scheduledExecutor,
                                            TenantNamespaceTopic tenantNamespaceTopic) {
        this.pulsarHandle = pulsarHandle;
        this.syncConfig = syncConfig;
        this.scheduledExecutor = scheduledExecutor;
        this.tenantNamespaceTopic = tenantNamespaceTopic;
    }

    public void start() {
        if (syncConfig.isAutoUpdateTopic()) {
            this.scheduledFuture =
                    this.scheduledExecutor.scheduleWithFixedDelay(this::sync, 0, 1, TimeUnit.MINUTES);
        } else {
            sync();
        }
    }

    private void sync() {
        try {
            PartitionedTopicMetadata partitionedTopicMetadata =
                    this.pulsarHandle.srcAdmin().topics().getPartitionedTopicMetadata(tenantNamespaceTopic.topic());
            for (int i = 0; i < partitionedTopicMetadata.partitions; i++) {
                int idx = i;
                map.computeIfAbsent(i, k -> {
                    TenantNamespaceTopic aux = new TenantNamespaceTopic(tenantNamespaceTopic.tenant(),
                            tenantNamespaceTopic.namespace(),
                            tenantNamespaceTopic.topic() + Const.PARTITION_SUFFIX + idx);
                    PulsarPartitionSyncWorker pulsarPartitionSyncWorker = new PulsarPartitionSyncWorker(pulsarHandle,
                            syncConfig, scheduledExecutor, aux);
                    pulsarPartitionSyncWorker.start();
                    return pulsarPartitionSyncWorker;
                });
            }
        } catch (Exception e) {
            log.error("{} sync topic partition failed", tenantNamespaceTopic, e);
        }
    }

    public void close() {
        if (this.scheduledFuture != null) {
            this.scheduledFuture.cancel(true);
        }
        this.map.forEach((k, v) -> v.close());
    }
}
