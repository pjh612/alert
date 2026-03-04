package com.alert.core.session;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryTagBasedAlertSessionRepositoryTest {

    private InMemoryTagBasedAlertSessionRepository<String> repository;

    private static final String NS_A = "queue-A";
    private static final String NS_B = "queue-B";

    @BeforeEach
    void setUp() {
        repository = new InMemoryTagBasedAlertSessionRepository<>();
    }

    @Test
    @DisplayName("단일 ID로 세션을 저장하고 조회할 수 있어야 한다")
    void putAndGetById() {
        // given
        String userId = "user123";
        String mockEngine = "Engine-1";

        // when
        repository.put(NS_A, userId, mockEngine);

        // then
        Optional<AlertSession<String>> result = repository.getById(NS_A, userId);
        assertThat(result).isPresent();
        assertThat(result.get().engine()).isEqualTo(mockEngine);
        assertThat(result.get().tags()).isEmpty();
    }

    @Test
    @DisplayName("여러 태그를 가진 세션을 저장하고 태그별로 조회할 수 있어야 한다")
    void putWithTagsAndGetByTag() {
        // given
        String userId = "user1";
        String engine = "Engine-1";
        Set<String> tags = Set.of("GROUP_A", "RANK_GOLD");

        // when
        repository.put(NS_A, userId, tags, engine);

        // then
        List<AlertSession<String>> groupA = repository.getByTag(NS_A, "GROUP_A");
        List<AlertSession<String>> rankGold = repository.getByTag(NS_A, "RANK_GOLD");

        assertThat(groupA).hasSize(1);
        assertThat(groupA.get(0).id()).isEqualTo(userId);
        assertThat(rankGold).hasSize(1);
        assertThat(rankGold.get(0).engine()).isEqualTo(engine);
    }

    @Test
    @DisplayName("동일 ID로 재등록 시 기존 태그 인덱스가 완전히 정리되어야 한다")
    void overwriteSessionCleansIndex() {
        // given
        repository.put(NS_A, "user1", Set.of("OLD_TAG"), "Old-Engine");

        // when (새로운 엔진과 태그로 덮어쓰기)
        repository.put(NS_A, "user1", Set.of("NEW_TAG"), "New-Engine");

        // then
        assertThat(repository.getByTag(NS_A, "OLD_TAG")).isEmpty();
        assertThat(repository.getByTag(NS_A, "NEW_TAG")).hasSize(1);
        assertThat(repository.getById(NS_A, "user1").get().engine()).isEqualTo("New-Engine");
    }

    @Test
    @DisplayName("addTag를 통해 동적으로 태그를 추가하고 인덱싱할 수 있어야 한다")
    void dynamicAddTag() {
        // given
        repository.put(NS_A, "user1", "Engine-1");

        // when
        repository.addTag(NS_A, "user1", "DYNAMIC_TAG");

        // then
        assertThat(repository.getByTag(NS_A, "DYNAMIC_TAG")).hasSize(1);
        assertThat(repository.getById(NS_A, "user1").get().tags()).contains("DYNAMIC_TAG");
    }

    @Test
    @DisplayName("deleteById 호출 시 세션과 태그 인덱스가 모두 삭제되어야 한다")
    void deleteByIdCleansEverything() {
        // given
        repository.put(NS_A, "user1", Set.of("TAG1"), "Engine-1");

        // when
        AlertSession<String> deleted = repository.deleteById(NS_A, "user1");

        // then
        assertThat(deleted).isNotNull();
        assertThat(repository.getById(NS_A, "user1")).isEmpty();
        assertThat(repository.getByTag(NS_A, "TAG1")).isEmpty();
    }

    @Test
    @DisplayName("deleteByTag 호출 시 해당 태그를 가진 모든 세션이 삭제되어야 한다")
    void deleteByTagTest() {
        // given
        repository.put(NS_A, "u1", Set.of("TARGET"), "E1");
        repository.put(NS_A, "u2", Set.of("TARGET"), "E2");
        repository.put(NS_A, "u3", Set.of("OTHER"), "E3");

        // when
        repository.deleteByTag(NS_A, "TARGET");

        // then
        assertThat(repository.getByTag(NS_A, "TARGET")).isEmpty();
        assertThat(repository.getById(NS_A, "u1")).isEmpty();
        assertThat(repository.getById(NS_A, "u2")).isEmpty();
        assertThat(repository.getById(NS_A, "u3")).isPresent(); // 다른 태그 유저는 유지
    }

    @Test
    @DisplayName("namespace가 다르면 동일한 ID라도 세션이 격리되어야 한다")
    void namespaceShouldIsolateSessionsWithSameId() {
        // given
        repository.put(NS_A, "user1", Set.of("TAG1"), "Engine-A");
        repository.put(NS_B, "user1", Set.of("TAG1"), "Engine-B");

        // then - 각 namespace에서 독립적으로 조회 가능
        assertThat(repository.getById(NS_A, "user1").get().engine()).isEqualTo("Engine-A");
        assertThat(repository.getById(NS_B, "user1").get().engine()).isEqualTo("Engine-B");

        // NS_A의 TAG1은 NS_A 세션만 포함
        assertThat(repository.getByTag(NS_A, "TAG1")).hasSize(1);
        assertThat(repository.getByTag(NS_A, "TAG1").get(0).engine()).isEqualTo("Engine-A");

        // NS_B의 TAG1은 NS_B 세션만 포함
        assertThat(repository.getByTag(NS_B, "TAG1")).hasSize(1);
        assertThat(repository.getByTag(NS_B, "TAG1").get(0).engine()).isEqualTo("Engine-B");
    }

    @Test
    @DisplayName("namespace 삭제는 다른 namespace에 영향을 주지 않아야 한다")
    void deleteInOneNamespaceDoesNotAffectOthers() {
        // given
        repository.put(NS_A, "user1", Set.of("TAG1"), "Engine-A");
        repository.put(NS_B, "user1", Set.of("TAG1"), "Engine-B");

        // when - NS_A의 user1 삭제
        repository.deleteById(NS_A, "user1");

        // then
        assertThat(repository.getById(NS_A, "user1")).isEmpty();
        assertThat(repository.getById(NS_B, "user1")).isPresent(); // NS_B는 영향 없음
    }

    @Test
    @DisplayName("size(namespace)는 해당 namespace의 세션 수만 반환해야 한다")
    void sizeByNamespace() {
        // given
        repository.put(NS_A, "u1", "E1");
        repository.put(NS_A, "u2", "E2");
        repository.put(NS_B, "u3", "E3");

        // then
        assertThat(repository.size(NS_A)).isEqualTo(2);
        assertThat(repository.size(NS_B)).isEqualTo(1);
        assertThat(repository.size()).isEqualTo(3);
    }

    @Test
    @DisplayName("getAll: namespace 내 모든 세션 조회")
    void getAll_returnsAllSessions() {
        repository.put(NS_A, "user1", "E1");
        repository.put(NS_A, "user2", Set.of("TAG1"), "E2");
        repository.put(NS_B, "user3", "E3");

        List<AlertSession<String>> result = repository.getAll(NS_A);

        assertThat(result).hasSize(2);
    }

    @Test
    @DisplayName("getAll: 빈 namespace는 빈 리스트 반환")
    void getAll_returnsEmptyWhenNamespaceEmpty() {
        repository.put(NS_A, "user1", "E1");

        List<AlertSession<String>> result = repository.getAll("empty-ns");

        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("getAll: 존재하지 않는 namespace는 빈 리스트 반환")
    void getAll_returnsEmptyWhenNamespaceNotFound() {
        List<AlertSession<String>> result = repository.getAll("unknown-ns");

        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("getById: 존재하지 않는 세션은 빈 Optional 반환")
    void getById_returnsEmptyWhenNotFound() {
        Optional<AlertSession<String>> result = repository.getById(NS_A, "unknown");

        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("getById: 존재하지 않는 namespace는 빈 Optional 반환")
    void getById_returnsEmptyWhenNamespaceNotFound() {
        Optional<AlertSession<String>> result = repository.getById("unknown-ns", "user1");

        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("addTag: 존재하지 않는 namespace에는 추가 안됨")
    void addTag_doesNothingWhenNamespaceNotFound() {
        repository.addTag("unknown-ns", "user1", "TAG1");

        assertThat(repository.size("unknown-ns")).isEqualTo(0);
    }

    @Test
    @DisplayName("addTag: 존재하지 않는 세션에는 추가 안됨")
    void addTag_doesNothingWhenSessionNotFound() {
        repository.put(NS_A, "user1", "E1");

        repository.addTag(NS_A, "unknown", "TAG1");

        assertThat(repository.getByTag(NS_A, "TAG1")).isEmpty();
    }

    @Test
    @DisplayName("put: null 태그는 빈 Set으로 처리")
    void put_handlesNullTags() {
        repository.put(NS_A, "user1", (Set<String>) null, "E1");

        AlertSession<String> session = repository.getById(NS_A, "user1").get();
        assertThat(session.engine()).isEqualTo("E1");
    }

    @Test
    @DisplayName("size: 빈 저장소는 0 반환")
    void size_returnsZeroWhenEmpty() {
        assertThat(repository.size()).isEqualTo(0);
    }

    @Test
    @DisplayName("size: 존재하지 않는 namespace는 0 반환")
    void size_unknownNamespace_returnsZero() {
        assertThat(repository.size("unknown-ns")).isEqualTo(0);
    }

    @Test
    @DisplayName("deleteById: 존재하지 않는 세션은 null 반환")
    void deleteById_returnsNullWhenNotFound() {
        AlertSession<String> result = repository.deleteById(NS_A, "unknown");

        assertThat(result).isNull();
    }

    @Test
    @DisplayName("deleteById: 존재하지 않는 namespace는 null 반환")
    void deleteById_unknownNamespace_returnsNull() {
        AlertSession<String> result = repository.deleteById("unknown-ns", "user1");

        assertThat(result).isNull();
    }

    @Test
    @DisplayName("deleteByTag: 존재하지 않는 namespace는 아무 작업 안함")
    void deleteByTag_unknownNamespace_doesNothing() {
        repository.deleteByTag("unknown-ns", "TAG1");

        assertThat(repository.size()).isEqualTo(0);
    }

    @Test
    @DisplayName("deleteByTag: 존재하지 않는 태그는 아무 작업 안함")
    void deleteByTag_unknownTag_doesNothing() {
        repository.put(NS_A, "user1", "E1");

        repository.deleteByTag(NS_A, "UNKNOWN");

        assertThat(repository.size(NS_A)).isEqualTo(1);
    }
}
