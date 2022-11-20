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
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class PulsarTopicSyncTest {

    @Test
    public void testSyncPartitionTopic() throws Exception {
        EmbeddedPulsarConfig embeddedPulsarConfig = new EmbeddedPulsarConfig().allowAutoTopicCreation(false);
        EmbeddedPulsarServer srcServer = new EmbeddedPulsarServer(embeddedPulsarConfig);
        EmbeddedPulsarServer dstServer = new EmbeddedPulsarServer(embeddedPulsarConfig);
        srcServer.start();
        dstServer.start();
        srcServer.createPulsarAdmin().topics().createPartitionedTopic("public/default/test-topic", 2);
        PulsarConfig srcConfig = PulsarConfig.builder().brokerHost("localhost")
                .httpPort(srcServer.getWebPort()).tcpPort(srcServer.getTcpPort()).build();
        PulsarConfig dstConfig = PulsarConfig.builder().brokerHost("localhost")
                .httpPort(dstServer.getWebPort()).tcpPort(dstServer.getTcpPort()).build();
        PulsarSync pulsarSync = new PulsarSync(srcConfig, dstConfig, SyncConfig.builder().subscriptionName("test")
                .build());
        pulsarSync.start();
        PulsarAdmin dstPulsarAdmin = dstServer.createPulsarAdmin();
        Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> {
            List<String> topics = dstPulsarAdmin.topics().getPartitionedTopicList("public/default");
            boolean containsPartitionTopic = topics.contains("persistent://public/default/test-topic");
            List<String> partitions = dstPulsarAdmin.topics().getList("public/default");
            boolean containsPartition0 = partitions.contains("persistent://public/default/test-topic-partition-0");
            boolean containsPartition1 = partitions.contains("persistent://public/default/test-topic-partition-1");
            return containsPartitionTopic && containsPartition0 && containsPartition1;
        });
        pulsarSync.close();
        srcServer.close();
        dstServer.close();
    }

    @Test
    public void testSyncNonPartitionTopic() throws Exception {
        EmbeddedPulsarConfig embeddedPulsarConfig = new EmbeddedPulsarConfig().allowAutoTopicCreation(false);
        EmbeddedPulsarServer srcServer = new EmbeddedPulsarServer(embeddedPulsarConfig);
        EmbeddedPulsarServer dstServer = new EmbeddedPulsarServer(embeddedPulsarConfig);
        srcServer.start();
        dstServer.start();
        srcServer.createPulsarAdmin().topics().createNonPartitionedTopic("public/default/test-topic");
        PulsarConfig srcConfig = PulsarConfig.builder().brokerHost("localhost")
                .httpPort(srcServer.getWebPort()).tcpPort(srcServer.getTcpPort()).build();
        PulsarConfig dstConfig = PulsarConfig.builder().brokerHost("localhost")
                .httpPort(dstServer.getWebPort()).tcpPort(dstServer.getTcpPort()).build();
        PulsarSync pulsarSync = new PulsarSync(srcConfig, dstConfig, SyncConfig.builder().subscriptionName("test")
                .build());
        pulsarSync.start();
        PulsarAdmin dstPulsarAdmin = dstServer.createPulsarAdmin();
        Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> {
            List<String> topics = dstPulsarAdmin.topics().getList("public/default");
            return topics.contains("persistent://public/default/test-topic");
        });
        pulsarSync.close();
        srcServer.close();
        dstServer.close();
    }

}
