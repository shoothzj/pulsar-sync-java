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
import org.apache.pulsar.common.policies.data.TenantInfo;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
public class PulsarTenantSyncManager {

    private final PulsarHandle pulsarHandle;

    private final SyncConfig syncConfig;

    private final ScheduledExecutorService scheduledExecutor;

    private ScheduledFuture<?> scheduledFuture;

    private final Map<String, PulsarNamespaceSyncManager> map = new ConcurrentHashMap<>();

    public PulsarTenantSyncManager(PulsarHandle pulsarHandle,
                                   SyncConfig syncConfig, ScheduledExecutorService scheduledExecutor) {
        this.pulsarHandle = pulsarHandle;
        this.syncConfig = syncConfig;
        this.scheduledExecutor = scheduledExecutor;
    }

    public void start() {
        if (syncConfig.isAutoUpdateTenant()) {
            this.scheduledFuture = scheduledExecutor
                    .scheduleWithFixedDelay(this::sync, 0, 3, TimeUnit.MINUTES);
        } else {
            sync();
        }
    }

    private void sync() {
        CompletableFuture<List<String>> completableFuture = pulsarHandle.srcAdmin().tenants().getTenantsAsync();
        completableFuture.exceptionally(throwable -> {
            log.error("Failed to get tenants", throwable);
            return null;
        });
        completableFuture.thenAccept(tenants -> {
            pulsarHandle.dstAdmin().clusters().getClustersAsync().exceptionally(getClusterInfoThrowable -> {
                log.error("Failed to get cluster info from destination pulsar", getClusterInfoThrowable);
                return null;
            }).thenAccept(clusters -> {
                TenantInfo tenantInfo = TenantInfo.builder().allowedClusters(new HashSet<>(clusters)).build();
                for (String tenant : tenants) {
                    pulsarHandle.dstAdmin().tenants()
                            .createTenantAsync(tenant, tenantInfo).whenComplete((unused, throwable1) -> {
                        if (throwable1 == null || (throwable1 instanceof PulsarAdminException.ConflictException)) {
                            map.computeIfAbsent(tenant, k -> startNamespaceSyncManager(tenant));
                        } else {
                            log.error("Failed to sync tenant {}, and skip it.", tenant, throwable1);
                        }
                    });
                }
            });
        });
    }

    private PulsarNamespaceSyncManager startNamespaceSyncManager(String tenant) {
        PulsarNamespaceSyncManager pulsarNamespaceSyncManager = new PulsarNamespaceSyncManager(pulsarHandle,
                syncConfig, scheduledExecutor, tenant);
        log.info("begin to start namespace sync manager for tenant [{}]", tenant);
        pulsarNamespaceSyncManager.start();
        return pulsarNamespaceSyncManager;
    }

    public void close() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(true);
        }
        for (PulsarNamespaceSyncManager pulsarNamespaceSyncManager : map.values()) {
            pulsarNamespaceSyncManager.close();
        }
    }
}
