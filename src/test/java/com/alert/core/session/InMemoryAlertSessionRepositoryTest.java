package com.alert.core.session;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryAlertSessionRepositoryTest {

    InMemoryAlertSessionRepository<String> repository;

    private static final String NAMESPACE = "test-ns";

    @BeforeEach
    void setUp() {
        repository = new InMemoryAlertSessionRepository<>();
    }

    @Test
    @DisplayName("put: 세션 저장 후 반환값은 null")
    void put_storesSession_returnsNull() {
        AlertSession<String> result = repository.put(NAMESPACE, "user1", "emitter1");

        assertThat(result).isNull();
    }

    @Test
    @DisplayName("getById: 저장된 세션 조회")
    void getById_returnsStoredSession() {
        repository.put(NAMESPACE, "user1", "emitter1");

        Optional<AlertSession<String>> result = repository.getById(NAMESPACE, "user1");

        assertThat(result).isPresent();
        assertThat(result.get().id()).isEqualTo("user1");
        assertThat(result.get().engine()).isEqualTo("emitter1");
    }

    @Test
    @DisplayName("getById: 존재하지 않는 세션 조회 시 빈 Optional")
    void getById_returnsEmptyWhenNotFound() {
        Optional<AlertSession<String>> result = repository.getById(NAMESPACE, "unknown");

        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("getById: 다른 namespace의 세션은 조회 안됨")
    void getById_namespaceIsolation() {
        repository.put("other-ns", "user1", "emitter1");

        Optional<AlertSession<String>> result = repository.getById(NAMESPACE, "user1");

        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("getAll: namespace 내 모든 세션 조회")
    void getAll_returnsAllSessionsInNamespace() {
        repository.put(NAMESPACE, "user1", "emitter1");
        repository.put(NAMESPACE, "user2", "emitter2");
        repository.put(NAMESPACE, "user3", "emitter3");

        List<AlertSession<String>> result = repository.getAll(NAMESPACE);

        assertThat(result).hasSize(3);
    }

    @Test
    @DisplayName("getAll: 빈 namespace는 빈 리스트 반환")
    void getAll_returnsEmptyListWhenEmpty() {
        List<AlertSession<String>> result = repository.getAll(NAMESPACE);

        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("getAll: 존재하지 않는 namespace는 빈 리스트 반환")
    void getAll_returnsEmptyListWhenNamespaceNotFound() {
        List<AlertSession<String>> result = repository.getAll("unknown-ns");

        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("deleteById: 세션 삭제 후 반환")
    void deleteById_removesAndReturnsSession() {
        repository.put(NAMESPACE, "user1", "emitter1");

        AlertSession<String> result = repository.deleteById(NAMESPACE, "user1");

        assertThat(result).isNotNull();
        assertThat(result.id()).isEqualTo("user1");
        assertThat(repository.getById(NAMESPACE, "user1")).isEmpty();
    }

    @Test
    @DisplayName("deleteById: 존재하지 않는 세션 삭제 시 null 반환")
    void deleteById_returnsNullWhenNotFound() {
        AlertSession<String> result = repository.deleteById(NAMESPACE, "unknown");

        assertThat(result).isNull();
    }

    @Test
    @DisplayName("size: 전체 세션 수 조회")
    void size_returnsTotalSessionCount() {
        repository.put(NAMESPACE, "user1", "emitter1");
        repository.put(NAMESPACE, "user2", "emitter2");
        repository.put("other-ns", "user3", "emitter3");

        long result = repository.size();

        assertThat(result).isEqualTo(3);
    }

    @Test
    @DisplayName("size: 빈 저장소는 0 반환")
    void size_returnsZeroWhenEmpty() {
        long result = repository.size();

        assertThat(result).isEqualTo(0);
    }

    @Test
    @DisplayName("size: namespace별 세션 수 조회")
    void size_namespace_returnsCountInNamespace() {
        repository.put(NAMESPACE, "user1", "emitter1");
        repository.put(NAMESPACE, "user2", "emitter2");
        repository.put("other-ns", "user3", "emitter3");

        long result = repository.size(NAMESPACE);

        assertThat(result).isEqualTo(2);
    }

    @Test
    @DisplayName("size: 존재하지 않는 namespace는 0 반환")
    void size_unknownNamespace_returnsZero() {
        long result = repository.size("unknown-ns");

        assertThat(result).isEqualTo(0);
    }
}
