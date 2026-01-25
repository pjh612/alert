package com.alert.sse;

import java.util.Set;

public record AlertSession<T>(String id, T engine, Set<String> tags) {

    public void addTag(String tag) {
        this.tags.add(tag);
    }
}
