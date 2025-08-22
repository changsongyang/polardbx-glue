/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.rpc.perf;

import lombok.Getter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Getter
public class SwitchoverPerfCollection {
    private final AtomicLong successCount = new AtomicLong(0);
    private final Map<Integer, AtomicLong> errorCount = new ConcurrentHashMap<>();

    private long startTime = System.nanoTime();
    private long connectWaitCnt = 0;
    private long connectWaitTime = 0;
    private long connectWaitMin = 0;
    private long connectWaitMax = 0;
    private long rescheduleWaitCnt = 0;
    private long rescheduleWaitTime = 0;
    private long rescheduleWaitMin = 0;
    private long rescheduleWaitMax = 0;

    public synchronized void reset() {
        successCount.set(0);
        errorCount.clear();
        startTime = System.nanoTime();
        connectWaitCnt = 0;
        connectWaitTime = 0;
        connectWaitMin = 0;
        connectWaitMax = 0;
        rescheduleWaitCnt = 0;
        rescheduleWaitTime = 0;
        rescheduleWaitMin = 0;
        rescheduleWaitMax = 0;
    }

    public void record(int errorCode) {
        if (0 == errorCode) {
            successCount.incrementAndGet();
        } else {
            errorCount.computeIfAbsent(errorCode, k -> new AtomicLong(0)).incrementAndGet();
        }
    }

    public synchronized void recordWait(boolean connect, long waitTime) {
        if (connect) {
            ++connectWaitCnt;
            connectWaitTime += waitTime;
            if (0 == connectWaitMin || waitTime < connectWaitMin) {
                connectWaitMin = waitTime;
            }
            if (0 == connectWaitMax || waitTime > connectWaitMax) {
                connectWaitMax = waitTime;
            }
        } else {
            ++rescheduleWaitCnt;
            rescheduleWaitTime += waitTime;
            if (0 == rescheduleWaitMin || waitTime < rescheduleWaitMin) {
                rescheduleWaitMin = waitTime;
            }
            if (0 == rescheduleWaitMax || waitTime > rescheduleWaitMax) {
                rescheduleWaitMax = waitTime;
            }
        }
    }

    public synchronized String toString() {
        final long now = System.nanoTime();
        return String.valueOf(TimeUnit.NANOSECONDS.toMillis(now - startTime)) + ',' + connectWaitCnt + ',' + (
            connectWaitCnt > 0 ? (float) connectWaitTime / connectWaitCnt : 0) + ',' + connectWaitMin + ','
            + connectWaitMax + ',' + rescheduleWaitCnt + ',' + (rescheduleWaitCnt > 0 ?
            (float) rescheduleWaitTime / rescheduleWaitCnt : 0) + ',' + rescheduleWaitMin + ',' + rescheduleWaitMax
            + ',' + successCount.get() + ",\"" + errorCount.entrySet().stream()
            .map(e -> e.getKey() + ":" + e.getValue().get()).collect(Collectors.joining(";")) + '\"';
    }
}
