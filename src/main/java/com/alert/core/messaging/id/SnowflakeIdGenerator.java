package com.alert.core.messaging.id;

/**
 * 53비트 Snowflake ID 생성기.
 *
 * <p>Redis ZSet의 score는 IEEE 754 double(53비트 가수부)로 저장되므로
 * 표준 63비트 Snowflake를 score로 사용하면 정밀도 손실이 발생한다.
 * 41+6+6 = 53비트로 설계하여 double 표현 범위를 초과하지 않도록 한다.
 *
 * <pre>
 * [ 41비트 타임스탬프 | 6비트 노드ID | 6비트 시퀀스 ] = 최대 2^53 - 1
 *   ~70년 지원        최대 63 노드   63 msg/ms/노드
 * </pre>
 *
 * <ul>
 *   <li>다중 서버 환경에서 노드마다 고유한 {@code nodeId}(0~63)를 설정해야 한다.</li>
 *   <li>같은 밀리초에 63개를 초과하면 다음 밀리초까지 블로킹된다.</li>
 *   <li>{@code nextId()}는 {@code synchronized}이므로 스레드 안전하다.</li>
 * </ul>
 */
public class SnowflakeIdGenerator implements IdGenerator<Long> {

    /**
     * 커스텀 에포크: 2024-01-01 00:00:00 UTC
     */
    private static final long CUSTOM_EPOCH = 1704067200000L;

    private static final int SEQUENCE_BITS = 6;
    private static final int NODE_BITS = 6;

    private static final long MAX_NODE = (1L << NODE_BITS) - 1;      // 63
    private static final long MAX_SEQUENCE = (1L << SEQUENCE_BITS) - 1;  // 63

    private static final int NODE_SHIFT = SEQUENCE_BITS;
    private static final int TIMESTAMP_SHIFT = NODE_BITS + SEQUENCE_BITS; // 12

    private final long nodeId;
    private long lastTimestamp = -1L;
    private long sequence = 0L;

    public SnowflakeIdGenerator(long nodeId) {
        if (nodeId < 0 || nodeId > MAX_NODE) {
            throw new IllegalArgumentException(
                    "nodeId는 0~" + MAX_NODE + " 범위여야 합니다. 입력값: " + nodeId);
        }
        this.nodeId = nodeId;
    }

    public synchronized Long nextId() {
        long now = currentTime();

        if (now < lastTimestamp) {
            now = waitNextMillis(lastTimestamp);
        }

        if (now == lastTimestamp) {
            sequence = (sequence + 1) & MAX_SEQUENCE;
            if (sequence == 0) {
                now = waitNextMillis(lastTimestamp);
            }
        } else {
            sequence = 0;
        }

        lastTimestamp = now;
        return (now << TIMESTAMP_SHIFT) | (nodeId << NODE_SHIFT) | sequence;
    }

    private long currentTime() {
        long ts = System.currentTimeMillis() - CUSTOM_EPOCH;
        if (ts < 0) throw new IllegalStateException("시스템 시각이 커스텀 에포크(2024-01-01)보다 이전입니다.");
        return ts;
    }

    private long waitNextMillis(long last) {
        long now = currentTime();
        while (now <= last) {
            Thread.onSpinWait(); // CPU 스핀 루프 최적화 힌트 (Java 9+)
            now = currentTime();
        }
        return now;
    }
}
