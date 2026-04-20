package com.workflow.engine;

import java.util.Objects;

/**
 * Binds a {@link Step} to its name and {@link RetryPolicy}.
 *
 * <p>Design choice: the name and retry policy live outside the {@code Step} interface
 * so that the same step implementation (e.g. a lambda) can be reused under different
 * names or retry settings without subclassing.</p>
 */
public record StepDefinition(String name, Step step, RetryPolicy retryPolicy) {

    public StepDefinition {
        Objects.requireNonNull(name, "name");
        if (name.isBlank()) {
            throw new IllegalArgumentException("step name must not be blank");
        }
        Objects.requireNonNull(step, "step");
        Objects.requireNonNull(retryPolicy, "retryPolicy");
    }
}
