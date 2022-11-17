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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
public class PulsarNamespaceSyncManager {

    private final PulsarHandle pulsarHandle;

    private final SyncConfig syncConfig;

    private final ScheduledExecutorService scheduledExecutor;

    private final String tenant;

    private ScheduledFuture<?> scheduledFuture;

    private final Map<TenantNamespace, PulsarPartitionTopicSyncManager> partitionMap = new ConcurrentHashMap<>();

    private final Map<TenantNamespace, PulsarTopicSyncManager> map = new ConcurrentHashMap<>();

    public PulsarNamespaceSyncManager(PulsarHandle pulsarHandle, SyncConfig syncConfig,
                                      ScheduledExecutorService scheduledExecutor,
                                      String tenant) {
        this.pulsarHandle = pulsarHandle;
        this.syncConfig = syncConfig;
        this.scheduledExecutor = scheduledExecutor;
        this.tenant = tenant;
    }

    public void start() {
        if (syncConfig.isAutoUpdateNamespace()) {
            this.scheduledFuture = scheduledExecutor
                    .scheduleWithFixedDelay(this::sync, 0, 3, TimeUnit.MINUTES);
        } else {
            sync();
        }
    }

    public void sync() {
        pulsarHandle.srcAdmin().namespaces().getNamespacesAsync(tenant).whenComplete((namespaces, throwable) -> {
            if (throwable != null) {
                log.error("Failed to get {} namespaces", tenant, throwable);
                return;
            }
            for (String namespace : namespaces) {
                TenantNamespace tenantNamespace = new TenantNamespace(tenant, namespace);
                partitionMap.computeIfAbsent(tenantNamespace, k -> startPartitionedTopicSyncManager(tenantNamespace));
                map.computeIfAbsent(tenantNamespace, k -> startTopicSyncManager(tenantNamespace));
            }
        });
    }

    private PulsarPartitionTopicSyncManager startPartitionedTopicSyncManager(TenantNamespace tenantNamespace) {
        PulsarPartitionTopicSyncManager pulsarPartitionTopicSyncManager = new PulsarPartitionTopicSyncManager(
                pulsarHandle, syncConfig, scheduledExecutor, tenantNamespace);
        log.info("begin to start partitioned topic sync manager for tenant [{}], namespace [{}]",
                tenantNamespace.tenant(), tenantNamespace.namespace());
        pulsarPartitionTopicSyncManager.start();
        return pulsarPartitionTopicSyncManager;
    }

    private PulsarTopicSyncManager startTopicSyncManager(TenantNamespace tenantNamespace) {
        PulsarTopicSyncManager pulsarTopicSyncManager = new PulsarTopicSyncManager(pulsarHandle,
                syncConfig, scheduledExecutor, tenantNamespace);
        log.info("begin to start topic sync manager for tenant [{}], namespace [{}]",
                tenantNamespace.tenant(), tenantNamespace.namespace());
        pulsarTopicSyncManager.start();
        return pulsarTopicSyncManager;
    }

    public void close() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(true);
        }
        partitionMap.values().forEach(PulsarPartitionTopicSyncManager::close);
        map.values().forEach(PulsarTopicSyncManager::close);
    }
}
