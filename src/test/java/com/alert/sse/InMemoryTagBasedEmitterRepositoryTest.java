package com.alert.sse;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryTagBasedAlertSessionRepositoryTest {

    private InMemoryTagBasedAlertSessionRepository<String> repository;

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
        repository.put(userId, mockEngine);

        // then
        Optional<AlertSession<String>> result = repository.getById(userId);
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
        repository.put(userId, tags, engine);

        // then
        List<AlertSession<String>> groupA = repository.getByTag("GROUP_A");
        List<AlertSession<String>> rankGold = repository.getByTag("RANK_GOLD");

        assertThat(groupA).hasSize(1);
        assertThat(groupA.get(0).id()).isEqualTo(userId);
        assertThat(rankGold).hasSize(1);
        assertThat(rankGold.get(0).engine()).isEqualTo(engine);
    }

    @Test
    @DisplayName("동일 ID로 재등록 시 기존 태그 인덱스가 완전히 정리되어야 한다")
    void overwriteSessionCleansIndex() {
        // given
        repository.put("user1", Set.of("OLD_TAG"), "Old-Engine");

        // when (새로운 엔진과 태그로 덮어쓰기)
        repository.put("user1", Set.of("NEW_TAG"), "New-Engine");

        // then
        assertThat(repository.getByTag("OLD_TAG")).isEmpty();
        assertThat(repository.getByTag("NEW_TAG")).hasSize(1);
        assertThat(repository.getById("user1").get().engine()).isEqualTo("New-Engine");
    }

    @Test
    @DisplayName("addTag를 통해 동적으로 태그를 추가하고 인덱싱할 수 있어야 한다")
    void dynamicAddTag() {
        // given
        repository.put("user1", "Engine-1");

        // when
        repository.addTag("user1", "DYNAMIC_TAG");

        // then
        assertThat(repository.getByTag("DYNAMIC_TAG")).hasSize(1);
        assertThat(repository.getById("user1").get().tags()).contains("DYNAMIC_TAG");
    }

    @Test
    @DisplayName("deleteById 호출 시 세션과 태그 인덱스가 모두 삭제되어야 한다")
    void deleteByIdCleansEverything() {
        // given
        repository.put("user1", Set.of("TAG1"), "Engine-1");

        // when
        AlertSession<String> deleted = repository.deleteById("user1");

        // then
        assertThat(deleted).isNotNull();
        assertThat(repository.getById("user1")).isEmpty();
        assertThat(repository.getByTag("TAG1")).isEmpty();
    }

    @Test
    @DisplayName("deleteByTag 호출 시 해당 태그를 가진 모든 세션이 삭제되어야 한다")
    void deleteByTagTest() {
        // given
        repository.put("u1", Set.of("TARGET"), "E1");
        repository.put("u2", Set.of("TARGET"), "E2");
        repository.put("u3", Set.of("OTHER"), "E3");

        // when
        repository.deleteByTag("TARGET");

        // then
        assertThat(repository.getByTag("TARGET")).isEmpty();
        assertThat(repository.getById("u1")).isEmpty();
        assertThat(repository.getById("u2")).isEmpty();
        assertThat(repository.getById("u3")).isPresent(); // 다른 태그 유저는 유지
    }
}