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

package io.github.shoothzj.pulsar.sync.tests;

import io.github.embedded.pulsar.core.EmbeddedPulsarConfig;
import io.github.embedded.pulsar.core.EmbeddedPulsarServer;
import io.github.shoothzj.pulsar.sync.core.PulsarConfig;
import io.github.shoothzj.pulsar.sync.core.PulsarSync;
import io.github.shoothzj.pulsar.sync.core.SyncConfig;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

public class PulsarMessageSyncTest {

    @Test
    public void testSyncTopicMessage() throws Exception {
        EmbeddedPulsarConfig embeddedPulsarConfig = new EmbeddedPulsarConfig().allowAutoTopicCreation(true);
        EmbeddedPulsarServer srcServer = new EmbeddedPulsarServer(embeddedPulsarConfig);
        EmbeddedPulsarServer dstServer = new EmbeddedPulsarServer(embeddedPulsarConfig);
        srcServer.start();
        dstServer.start();
        PulsarClient srcClient = PulsarClient.builder()
                .serviceUrl(String.format("pulsar://localhost:%d", srcServer.getTcpPort())).build();
        srcClient.newProducer().topic("test").create().send("hello".getBytes());
        PulsarConfig srcConfig = PulsarConfig.builder().brokerHost("localhost")
                .httpPort(srcServer.getWebPort()).tcpPort(srcServer.getTcpPort()).build();
        PulsarConfig dstConfig = PulsarConfig.builder().brokerHost("localhost")
                .httpPort(dstServer.getWebPort()).tcpPort(dstServer.getTcpPort()).build();
        PulsarSync pulsarSync = new PulsarSync(srcConfig, dstConfig, SyncConfig.builder().subscriptionName("test")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).build());
        pulsarSync.start();
        Consumer<byte[]> consumer = PulsarClient.builder()
                .serviceUrl(String.format("pulsar://localhost:%d", dstServer.getTcpPort())).build()
                .newConsumer().topic("test")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName("test").subscribe();
        Assertions.assertNotNull(consumer.receive(5, TimeUnit.SECONDS));
        pulsarSync.close();
        srcClient.close();
        srcServer.close();
        dstServer.close();
    }

}
