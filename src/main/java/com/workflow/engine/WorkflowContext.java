package com.workflow.engine;

import java.util.Collections;
import java.util.HashMap;
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
 *
 * <p>For parallel stages the engine creates a <em>branch</em> context per child
 * via {@link #branchOf(WorkflowContext)}. A branch reads through to an immutable
 * snapshot of the parent taken at branch time, and writes only to a local overlay.
 * Branches therefore never observe each other's writes. After the group completes
 * successfully, the engine folds each branch's {@link #localWrites()} back into the
 * parent; on any failure the overlays are discarded so the parent is unchanged.</p>
 */
public final class WorkflowContext {

    /** Immutable snapshot of the parent context at branch time; empty for a root context. */
    private final Map<String, Object> parentSnapshot;
    /** This context's own writes. */
    private final Map<String, Object> data = new ConcurrentHashMap<>();

    private WorkflowContext(Map<String, Object> parentSnapshot) {
        this.parentSnapshot = parentSnapshot;
    }

    public static WorkflowContext empty() {
        return new WorkflowContext(Map.of());
    }

    public static WorkflowContext of(Map<String, Object> initial) {
        WorkflowContext ctx = empty();
        ctx.data.putAll(initial);
        return ctx;
    }

    /**
     * Create a branch context that reads from an immutable snapshot of {@code parent}
     * (as of this call) and writes to a fresh local overlay. Used by the engine to give
     * each child of a parallel stage write isolation.
     */
    static WorkflowContext branchOf(WorkflowContext parent) {
        return new WorkflowContext(Map.copyOf(parent.asMap()));
    }

    /** The writes made to this context only (excludes inherited parent entries). */
    Map<String, Object> localWrites() {
        return Map.copyOf(data);
    }

    public WorkflowContext put(String key, Object value) {
        data.put(key, value);
        return this;
    }

    public <T> Optional<T> get(String key, Class<T> type) {
        Object v = data.get(key);
        if (v == null) {
            v = parentSnapshot.get(key);
        }
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
        return data.containsKey(key) || parentSnapshot.containsKey(key);
    }

    /** A read-only merged view: parent snapshot overlaid with this context's own writes. */
    public Map<String, Object> asMap() {
        if (parentSnapshot.isEmpty()) {
            return Collections.unmodifiableMap(data);
        }
        Map<String, Object> merged = new HashMap<>(parentSnapshot);
        merged.putAll(data);
        return Collections.unmodifiableMap(merged);
    }
}
