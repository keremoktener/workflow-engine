package com.workflow.engine;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Mutable, thread-safe key/value bag passed through every step of a workflow run.
 *
 * <p>Design choice: a heterogeneous map keyed by String rather than a user-defined
 * POJO. This keeps the engine domain-agnostic — steps from different concerns can
 * share data without the engine knowing their types — at the cost of compile-time
 * safety. {@link #get(String, Class)} restores type safety at the read site.</p>
 */
public final class WorkflowContext {

    private final Map<String, Object> data = new ConcurrentHashMap<>();

    public static WorkflowContext empty() {
        return new WorkflowContext();
    }

    public static WorkflowContext of(Map<String, Object> initial) {
        WorkflowContext ctx = new WorkflowContext();
        ctx.data.putAll(initial);
        return ctx;
    }

    public WorkflowContext put(String key, Object value) {
        data.put(key, value);
        return this;
    }

    public <T> Optional<T> get(String key, Class<T> type) {
        Object v = data.get(key);
        if (v == null) {
            return Optional.empty();
        }
        if (!type.isInstance(v)) {
            throw new ClassCastException(
                "Context key '" + key + "' is of type " + v.getClass().getName()
                    + ", not " + type.getName());
        }
        return Optional.of(type.cast(v));
    }

    public <T> T require(String key, Class<T> type) {
        return get(key, type).orElseThrow(
            () -> new IllegalStateException("Required context key '" + key + "' is missing"));
    }

    public boolean contains(String key) {
        return data.containsKey(key);
    }

    public Map<String, Object> asMap() {
        return Collections.unmodifiableMap(data);
    }
}
