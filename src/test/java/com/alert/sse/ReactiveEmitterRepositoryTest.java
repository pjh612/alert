package com.alert.sse;

import com.alert.core.session.AlertSession;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.codec.ServerSentEvent;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class ReactiveEmitterRepositoryTest {

    @Test
    @DisplayName("존재하지 않는 id를 삭제하면 null을 반환한다")
    void deleteById_whenSessionNotExists_returnNull() {
        // given
        ReactiveEmitterRepository repository = new ReactiveEmitterRepository();

        // when
        AlertSession<Sinks.Many<ServerSentEvent<Object>>> result =
                repository.deleteById("ns", "unknown");

        // then
        assertThat(result).isNull();
    }

    @Test
    @DisplayName("세션이 존재하면 sink에 complete를 emit한다")
    void deleteById_whenSessionExists_emitComplete() {
        // given
        ReactiveEmitterRepository repository = new ReactiveEmitterRepository();

        Sinks.Many<ServerSentEvent<Object>> sink =
                Sinks.many().multicast().onBackpressureBuffer();

        repository.put("ns", "user1", Set.of("tag1"), sink);

        var flux = sink.asFlux();

        // when
        AlertSession<Sinks.Many<ServerSentEvent<Object>>> deleted =
                repository.deleteById("ns", "user1");

        // then
        assertThat(deleted).isNotNull();

        StepVerifier.create(flux)
                .verifyComplete();
    }

    @Test
    @DisplayName("세션 삭제 후 동일 id로 다시 삭제하면 null을 반환한다")
    void deleteById_whenAlreadyDeleted_returnNull() {
        // given
        ReactiveEmitterRepository repository = new ReactiveEmitterRepository();

        Sinks.Many<ServerSentEvent<Object>> sink =
                Sinks.many().multicast().onBackpressureBuffer();

        repository.put("ns", "user1", Set.of("tag1"), sink);

        // first delete
        repository.deleteById("ns", "user1");

        // when
        AlertSession<Sinks.Many<ServerSentEvent<Object>>> result =
                repository.deleteById("ns", "user1");

        // then
        assertThat(result).isNull();
    }
}