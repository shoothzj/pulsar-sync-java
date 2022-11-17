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
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@Slf4j
public class PulsarSync {

    private final PulsarTenantSyncManager tenantSyncManager;

    private final ScheduledExecutorService scheduledExecutor;

    public PulsarSync(PulsarConfig srcConfig, PulsarConfig dstConfig,
                      SyncConfig syncConfig) throws PulsarClientException {
        PulsarAdmin srcAdmin = createPulsarAdminFromConfig(srcConfig);
        PulsarAdmin dstAdmin = createPulsarAdminFromConfig(dstConfig);
        PulsarClient srcClient = createPulsarClientFromConfig(srcConfig);
        PulsarClient dstClient = createPulsarClientFromConfig(dstConfig);
        PulsarHandle pulsarHandle = new PulsarHandle(srcAdmin, dstAdmin, srcClient, dstClient);
        this.scheduledExecutor = Executors.newScheduledThreadPool(10);
        this.tenantSyncManager = new PulsarTenantSyncManager(pulsarHandle, syncConfig, scheduledExecutor);
    }

    public void start() {
        log.info("begin to start tenant sync manager");
        this.tenantSyncManager.start();
    }

    private PulsarAdmin createPulsarAdminFromConfig(PulsarConfig config) throws PulsarClientException {
        String httpUrl = String.format("http://%s:%d", config.getBrokerHost(), config.getHttpPort());
        return PulsarAdmin.builder().serviceHttpUrl(httpUrl).build();
    }

    private PulsarClient createPulsarClientFromConfig(PulsarConfig config) throws PulsarClientException {
        String serviceUrl = String.format("pulsar://%s:%d", config.getBrokerHost(), config.getTcpPort());
        return PulsarClient.builder().serviceUrl(serviceUrl).build();
    }

    public void close() {
        log.info("begin to close tenant sync manager");
        this.tenantSyncManager.close();
        this.scheduledExecutor.shutdown();
    }
}
