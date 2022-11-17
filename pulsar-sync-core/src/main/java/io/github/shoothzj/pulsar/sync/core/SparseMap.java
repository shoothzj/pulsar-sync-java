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

import org.apache.commons.lang3.tuple.Pair;

import java.util.concurrent.ConcurrentSkipListMap;

public class SparseMap<K, V> {

    private ConcurrentSkipListMap<K, Pair<V, Long>> map = new ConcurrentSkipListMap<>();

    private final long expireNanoTime;

    public SparseMap(long expireNanoTime) {
        this.expireNanoTime = expireNanoTime;
    }

    public void put(K key, V value) {
        while (true) {
            final K firstKey = map.firstKey();
            if (firstKey == null) {
                break;
            }
            final Pair<V, Long> pair = map.get(firstKey);
            if (pair == null) {
                break;
            }
            if (System.nanoTime() - pair.getRight() > expireNanoTime) {
                map.remove(firstKey);
            } else {
                break;
            }
        }
        map.put(key, Pair.of(value, System.nanoTime()));
    }

    public V get(K messageId) {
        // binary search in concurrentSkipListMap
        Pair<V, Long> pair = map.get(map.floorKey(messageId));
        if (pair == null) {
            return null;
        }
        return pair.getLeft();
    }
}
