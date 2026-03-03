package com.alert.infra.micrometer;

import com.alert.core.session.AlertSession;
import com.alert.core.session.TagBasedAlertSessionRepository;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * {@link TagBasedAlertSessionRepository}의 Decorator 구현체.
 * <p>
 * 핵심 세션 저장소에 Micrometer 메트릭 계측을 추가한다.
 * 원본 저장소({@code delegate})는 수정 없이 순수 세션 관리에 집중한다.
 * <p>
 * 계측 항목:
 * <ul>
 *   <li>{@code alert.sessions.active} - 현재 활성 SSE 세션 수 (Gauge)</li>
 * </ul>
 */
public class MeteredAlertSessionRepository<T> implements TagBasedAlertSessionRepository<T> {

    private final TagBasedAlertSessionRepository<T> delegate;

    public MeteredAlertSessionRepository(TagBasedAlertSessionRepository<T> delegate,
                                         MeterRegistry meterRegistry) {
        this.delegate = delegate;
        Gauge.builder("alert.sessions.active", delegate, repo -> (double) repo.size())
                .description("Active SSE sessions")
                .register(meterRegistry);
    }

    @Override
    public void put(String namespace, String id, T engine) {
        delegate.put(namespace, id, engine);
    }

    @Override
    public void put(String namespace, String id, Set<String> tags, T engine) {
        delegate.put(namespace, id, tags, engine);
    }

    @Override
    public void addTag(String namespace, String id, String tag) {
        delegate.addTag(namespace, id, tag);
    }

    @Override
    public List<AlertSession<T>> getByTag(String namespace, String tag) {
        return delegate.getByTag(namespace, tag);
    }

    @Override
    public void deleteByTag(String namespace, String tag) {
        delegate.deleteByTag(namespace, tag);
    }

    @Override
    public Optional<AlertSession<T>> getById(String namespace, String id) {
        return delegate.getById(namespace, id);
    }

    @Override
    public List<AlertSession<T>> getAll(String namespace) {
        return delegate.getAll(namespace);
    }

    @Override
    public AlertSession<T> deleteById(String namespace, String id) {
        return delegate.deleteById(namespace, id);
    }

    @Override
    public long size() {
        return delegate.size();
    }

    @Override
    public long size(String namespace) {
        return delegate.size(namespace);
    }
}
