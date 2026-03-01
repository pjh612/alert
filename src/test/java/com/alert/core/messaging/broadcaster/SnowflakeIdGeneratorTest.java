package com.alert.core.messaging.broadcaster;

import com.alert.core.messaging.id.SnowflakeIdGenerator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class SnowflakeIdGeneratorTest {

    private static final double MAX_SAFE_DOUBLE = Math.pow(2, 53) - 1;

    @Test
    @DisplayName("생성된 ID는 double 정밀도 범위(2^53 - 1) 이하여야 한다")
    void generatedId_withinDoublePrecisionRange() {
        SnowflakeIdGenerator generator = new SnowflakeIdGenerator(0);

        for (int i = 0; i < 1000; i++) {
            long id = generator.nextId();
            assertThat((double) id).isLessThanOrEqualTo(MAX_SAFE_DOUBLE);
            // double 변환 후 역변환 시 손실 없음
            assertThat((long)(double) id).isEqualTo(id);
        }
    }

    @Test
    @DisplayName("같은 노드에서 연속 생성된 ID는 단조 증가해야 한다")
    void consecutiveIds_areMonotonicallyIncreasing() {
        SnowflakeIdGenerator generator = new SnowflakeIdGenerator(1);
        long prev = generator.nextId();

        for (int i = 0; i < 500; i++) {
            long next = generator.nextId();
            assertThat(next).isGreaterThan(prev);
            prev = next;
        }
    }

    @Test
    @DisplayName("서로 다른 노드에서 생성된 동시 ID는 중복 없이 고유해야 한다")
    void differentNodes_generateUniqueIds() throws InterruptedException {
        int nodeCount = 4;
        int idsPerNode = 200;
        Set<Long> allIds = java.util.Collections.synchronizedSet(new HashSet<>());
        CountDownLatch latch = new CountDownLatch(nodeCount);
        ExecutorService executor = Executors.newFixedThreadPool(nodeCount);

        for (int node = 0; node < nodeCount; node++) {
            final int nodeId = node;
            executor.submit(() -> {
                try {
                    SnowflakeIdGenerator generator = new SnowflakeIdGenerator(nodeId);
                    for (int i = 0; i < idsPerNode; i++) {
                        allIds.add(generator.nextId());
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();

        assertThat(allIds).hasSize(nodeCount * idsPerNode);
    }

    @Test
    @DisplayName("유효하지 않은 nodeId(음수 또는 63 초과)는 예외가 발생해야 한다")
    void invalidNodeId_throwsException() {
        assertThatThrownBy(() -> new SnowflakeIdGenerator(-1))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> new SnowflakeIdGenerator(64))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("경계 nodeId(0, 63)는 정상 동작해야 한다")
    void boundaryNodeIds_workCorrectly() {
        SnowflakeIdGenerator min = new SnowflakeIdGenerator(0);
        SnowflakeIdGenerator max = new SnowflakeIdGenerator(63);

        assertThat(min.nextId()).isPositive();
        assertThat(max.nextId()).isPositive();
        assertThat(min.nextId()).isNotEqualTo(max.nextId());
    }

    @Test
    void multiThread_uniqueness() throws Exception {
        SnowflakeIdGenerator generator = new SnowflakeIdGenerator(2);
        int threads = 8;
        int perThread = 10_000;


        ExecutorService executor = Executors.newFixedThreadPool(threads);
        Set<Long> ids = ConcurrentHashMap.newKeySet();
        AtomicBoolean duplicateFound = new AtomicBoolean(false);

        CountDownLatch latch = new CountDownLatch(threads);

        for (int t = 0; t < threads; t++) {
            executor.submit(() -> {
                for (int i = 0; i < perThread; i++) {
                    long id = generator.nextId();
                    if (!ids.add(id)) {
                        duplicateFound.set(true);
                    }
                }
                latch.countDown();
            });
        }

        latch.await();
        executor.shutdown();

        assertFalse(duplicateFound.get(), "Duplicate ID detected");
        assertEquals(threads * perThread, ids.size());
    }
}
