package com.alert.sse;

import com.alert.core.session.AlertSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.http.codec.ServerSentEvent;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class ReactiveInMemoryEmitterRepositoryTest {

    private static final String NS = "test-ns";
    private static final String OTHER_NS = "other-ns";

    ReactiveInMemoryEmitterRepository repository;

    @BeforeEach
    void setUp() {
        repository = new ReactiveInMemoryEmitterRepository();
    }

    private Sinks.Many<ServerSentEvent<Object>> newSink() {
        return Sinks.many().multicast().onBackpressureBuffer();
    }

    @Nested
    @DisplayName("put / getById")
    class PutAndGetById {

        @Test
        @DisplayName("put 후 getById로 동일 세션 조회")
        void put_thenGetById_returnsSession() {
            Sinks.Many<ServerSentEvent<Object>> sink = newSink();
            repository.put(NS, "user1", sink);

            Optional<AlertSession<Sinks.Many<ServerSentEvent<Object>>>> result = repository.getById(NS, "user1");

            assertThat(result).isPresent();
            assertThat(result.get().id()).isEqualTo("user1");
            assertThat(result.get().engine()).isSameAs(sink);
        }

        @Test
        @DisplayName("존재하지 않는 namespace → Optional.empty()")
        void getById_unknownNamespace_returnsEmpty() {
            assertThat(repository.getById("no-such-ns", "user1")).isEmpty();
        }

        @Test
        @DisplayName("존재하지 않는 id → Optional.empty()")
        void getById_unknownId_returnsEmpty() {
            repository.put(NS, "user1", newSink());

            assertThat(repository.getById(NS, "user2")).isEmpty();
        }

        @Test
        @DisplayName("동일 id로 덮어쓰기 시 최신 sink 반환")
        void put_sameId_overwritesPreviousSession() {
            Sinks.Many<ServerSentEvent<Object>> first = newSink();
            Sinks.Many<ServerSentEvent<Object>> second = newSink();
            repository.put(NS, "user1", first);
            repository.put(NS, "user1", second);

            assertThat(repository.getById(NS, "user1").get().engine()).isSameAs(second);
        }

        @Test
        @DisplayName("namespace가 다르면 동일 id도 격리")
        void put_differentNamespaces_areIsolated() {
            repository.put(NS, "user1", newSink());

            assertThat(repository.getById(OTHER_NS, "user1")).isEmpty();
        }
    }

    @Nested
    @DisplayName("getAll")
    class GetAll {

        @Test
        @DisplayName("namespace 내 모든 세션 반환")
        void getAll_returnsAllSessions() {
            repository.put(NS, "user1", newSink());
            repository.put(NS, "user2", newSink());

            List<AlertSession<Sinks.Many<ServerSentEvent<Object>>>> all = repository.getAll(NS);

            assertThat(all).hasSize(2);
            assertThat(all).extracting(AlertSession::id).containsExactlyInAnyOrder("user1", "user2");
        }

        @Test
        @DisplayName("존재하지 않는 namespace → 빈 리스트")
        void getAll_unknownNamespace_returnsEmptyList() {
            assertThat(repository.getAll("no-such-ns")).isEmpty();
        }

        @Test
        @DisplayName("다른 namespace 세션은 포함되지 않음")
        void getAll_excludesOtherNamespace() {
            repository.put(NS, "user1", newSink());
            repository.put(OTHER_NS, "user2", newSink());

            assertThat(repository.getAll(NS)).hasSize(1);
        }
    }

    @Nested
    @DisplayName("deleteById")
    class DeleteById {

        @Test
        @DisplayName("삭제 후 getById → Optional.empty()")
        void deleteById_removesSession() {
            repository.put(NS, "user1", newSink());
            repository.deleteById(NS, "user1");

            assertThat(repository.getById(NS, "user1")).isEmpty();
        }

        @Test
        @DisplayName("삭제 시 sink에 완료 신호 전송")
        void deleteById_completesTheSink() {
            Sinks.Many<ServerSentEvent<Object>> sink = newSink();
            repository.put(NS, "user1", sink);

            StepVerifier.create(sink.asFlux())
                    .then(() -> repository.deleteById(NS, "user1"))
                    .expectComplete()
                    .verify();
        }

        @Test
        @DisplayName("삭제된 세션 객체 반환")
        void deleteById_returnsRemovedSession() {
            Sinks.Many<ServerSentEvent<Object>> sink = newSink();
            repository.put(NS, "user1", sink);

            AlertSession<Sinks.Many<ServerSentEvent<Object>>> removed = repository.deleteById(NS, "user1");

            assertThat(removed).isNotNull();
            assertThat(removed.id()).isEqualTo("user1");
            assertThat(removed.engine()).isSameAs(sink);
        }

        @Test
        @DisplayName("존재하지 않는 namespace 삭제 → null 반환")
        void deleteById_unknownNamespace_returnsNull() {
            assertThat(repository.deleteById("no-such-ns", "user1")).isNull();
        }

        @Test
        @DisplayName("존재하지 않는 id 삭제 → null 반환")
        void deleteById_unknownId_returnsNull() {
            repository.put(NS, "user1", newSink());

            assertThat(repository.deleteById(NS, "user2")).isNull();
        }

        @Test
        @DisplayName("삭제는 다른 namespace에 영향을 주지 않음")
        void deleteById_doesNotAffectOtherNamespace() {
            repository.put(NS, "user1", newSink());
            repository.put(OTHER_NS, "user1", newSink());

            repository.deleteById(NS, "user1");

            assertThat(repository.getById(OTHER_NS, "user1")).isPresent();
        }
    }

    @Nested
    @DisplayName("size()")
    class SizeAll {

        @Test
        @DisplayName("구독자 없는 세션은 size()에 포함되지 않음")
        void size_withNoSubscriber_notCounted() {
            repository.put(NS, "user1", newSink());

            assertThat(repository.size()).isEqualTo(0);
        }

        @Test
        @DisplayName("구독자 있는 세션만 size()에 포함")
        void size_withActiveSubscriber_counted() {
            Sinks.Many<ServerSentEvent<Object>> sink = newSink();
            sink.asFlux().subscribe();
            repository.put(NS, "user1", sink);

            assertThat(repository.size()).isEqualTo(1);
        }

        @Test
        @DisplayName("모든 namespace의 활성 세션 합산")
        void size_aggregatesAllNamespaces() {
            Sinks.Many<ServerSentEvent<Object>> s1 = newSink();
            Sinks.Many<ServerSentEvent<Object>> s2 = newSink();
            s1.asFlux().subscribe();
            s2.asFlux().subscribe();
            repository.put(NS, "user1", s1);
            repository.put(OTHER_NS, "user2", s2);

            assertThat(repository.size()).isEqualTo(2);
        }
    }

    @Nested
    @DisplayName("size(namespace)")
    class SizeByNamespace {

        @Test
        @DisplayName("존재하지 않는 namespace → 0")
        void sizeByNamespace_unknownNamespace_returnsZero() {
            assertThat(repository.size("no-such-ns")).isEqualTo(0);
        }

        @Test
        @DisplayName("namespace 내 활성 세션만 카운트")
        void sizeByNamespace_countsOnlyActiveInNamespace() {
            Sinks.Many<ServerSentEvent<Object>> active = newSink();
            active.asFlux().subscribe();
            repository.put(NS, "user1", active);
            repository.put(NS, "user2", newSink()); // 구독자 없음

            assertThat(repository.size(NS)).isEqualTo(1);
        }

        @Test
        @DisplayName("다른 namespace 세션은 카운트에 포함되지 않음")
        void sizeByNamespace_excludesOtherNamespaces() {
            Sinks.Many<ServerSentEvent<Object>> sink = newSink();
            sink.asFlux().subscribe();
            repository.put(OTHER_NS, "user1", sink);

            assertThat(repository.size(NS)).isEqualTo(0);
        }
    }
}
