package com.alert.infra.micrometer;

import com.alert.core.session.AlertSession;
import com.alert.core.session.TagBasedAlertSessionRepository;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MeteredAlertSessionRepositoryTest {

    @Mock
    private TagBasedAlertSessionRepository<Object> delegate;

    private MeterRegistry meterRegistry;
    private MeteredAlertSessionRepository<Object> meteredRepository;

    private final String NS = "test-namespace";
    private final String ID = "test-id";
    private final String TAG = "test-tag";

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        meteredRepository = new MeteredAlertSessionRepository<>(delegate, meterRegistry);
    }

    @Test
    @DisplayName("put: 세션 저장 시 원본 저장소의 put을 호출하고 반환값을 전달해야 함")
    void put_sessionData_callsDelegatePutAndReturnsSession() {
        Object engine = new Object();
        AlertSession<Object> expected = mock(AlertSession.class);
        when(delegate.put(NS, ID, engine)).thenReturn(expected);

        AlertSession<Object> result = meteredRepository.put(NS, ID, engine);

        assertThat(result).isEqualTo(expected);
        verify(delegate).put(NS, ID, engine);
    }

    @Test
    @DisplayName("put(with tags): 태그 포함 세션 저장 시 원본 저장소의 put을 호출해야 함")
    void put_sessionWithTags_callsDelegatePutWithTags() {
        Object engine = new Object();
        Set<String> tags = Set.of("tag1", "tag2");
        AlertSession<Object> expected = mock(AlertSession.class);
        when(delegate.put(NS, ID, tags, engine)).thenReturn(expected);

        AlertSession<Object> result = meteredRepository.put(NS, ID, tags, engine);

        assertThat(result).isEqualTo(expected);
        verify(delegate).put(NS, ID, tags, engine);
    }

    @Test
    @DisplayName("addTag: 태그 추가 시 원본 저장소의 addTag를 호출해야 함")
    void addTag_tagTarget_callsDelegateAddTag() {
        meteredRepository.addTag(NS, ID, TAG);
        verify(delegate).addTag(NS, ID, TAG);
    }

    @Test
    @DisplayName("getByTag: 태그로 조회 시 원본 저장소에서 리스트를 가져와야 함")
    void getByTag_tagTarget_returnsListFromDelegate() {
        List<AlertSession<Object>> expectedList = List.of(mock(AlertSession.class));
        when(delegate.getByTag(NS, TAG)).thenReturn(expectedList);

        List<AlertSession<Object>> result = meteredRepository.getByTag(NS, TAG);

        assertThat(result).isEqualTo(expectedList);
        verify(delegate).getByTag(NS, TAG);
    }

    @Test
    @DisplayName("deleteByTag: 태그로 삭제 시 원본 저장소의 deleteByTag를 호출해야 함")
    void deleteByTag_tagTarget_callsDelegateDeleteByTag() {
        meteredRepository.deleteByTag(NS, TAG);
        verify(delegate).deleteByTag(NS, TAG);
    }

    @Test
    @DisplayName("getById: ID로 조회 시 원본 저장소의 결과를 반환해야 함")
    void getById_idTarget_returnsOptionalFromDelegate() {
        AlertSession<Object> expected = mock(AlertSession.class);
        when(delegate.getById(NS, ID)).thenReturn(Optional.of(expected));

        Optional<AlertSession<Object>> result = meteredRepository.getById(NS, ID);

        assertThat(result).contains(expected);
        verify(delegate).getById(NS, ID);
    }

    @Test
    @DisplayName("getAll: 네임스페이스 전체 조회 시 원본 저장소의 결과를 반환해야 함")
    void getAll_namespaceTarget_returnsAllSessionsFromDelegate() {
        List<AlertSession<Object>> expectedList = List.of(mock(AlertSession.class));
        when(delegate.getAll(NS)).thenReturn(expectedList);

        List<AlertSession<Object>> result = meteredRepository.getAll(NS);

        assertThat(result).isEqualTo(expectedList);
        verify(delegate).getAll(NS);
    }

    @Test
    @DisplayName("deleteById: ID로 삭제 시 원본 저장소의 deleteById를 호출해야 함")
    void deleteById_idTarget_callsDelegateDeleteById() {
        AlertSession<Object> expected = mock(AlertSession.class);
        when(delegate.deleteById(NS, ID)).thenReturn(expected);

        AlertSession<Object> result = meteredRepository.deleteById(NS, ID);

        assertThat(result).isEqualTo(expected);
        verify(delegate).deleteById(NS, ID);
    }

    @Test
    @DisplayName("size: 전체 사이즈 조회 시 메트릭 원본 데이터인 size를 호출해야 함")
    void size_noArgs_callsDelegateSize() {
        when(delegate.size()).thenReturn(10L);

        assertThat(meteredRepository.size()).isEqualTo(10L);

        verify(delegate).size();
    }

    @Test
    @DisplayName("size(namespace): 네임스페이스별 사이즈 조회 시 원본 저장소의 size(namespace)를 호출해야 함")
    void size_namespaceTarget_callsDelegateSizeWithNamespace() {
        // Given
        when(delegate.size(NS)).thenReturn(5L);

        // When
        long result = meteredRepository.size(NS);

        // Then
        assertThat(result).isEqualTo(5L);
        verify(delegate).size(NS);
    }

    @Test
    @DisplayName("gauge: 활성 세션 메트릭이 원본 저장소의 크기를 동적으로 반영해야 함")
    void gauge_activeSessions_reflectsDelegateSizeDynamically() {
        Gauge gauge = meterRegistry.get("alert.sessions.active").gauge();

        when(delegate.size()).thenReturn(5L);
        assertThat(gauge.value()).isEqualTo(5.0);

        when(delegate.size()).thenReturn(42L);
        assertThat(gauge.value()).isEqualTo(42.0);
    }
}