package com.workflow.engine;

import java.time.Duration;

/**
 * Abstraction over {@code Thread.sleep} so retry delays can be stubbed in tests
 * and swapped for a scheduler later without touching the engine.
 */
@FunctionalInterface
public interface Sleeper {

    void sleep(Duration duration) throws InterruptedException;

    Sleeper SYSTEM = d -> {
        if (!d.isZero() && !d.isNegative()) {
            Thread.sleep(d.toMillis());
        }
    };
}
