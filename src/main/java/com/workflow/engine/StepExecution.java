package com.workflow.engine;

import java.time.Duration;
import java.util.Optional;

/**
 * Immutable record of what happened when one step ran: outcome, attempt count,
 * wall-clock duration, and the terminal failure (if any).
 */
public record StepExecution(
    String stepName,
    Status status,
    int attempts,
    Duration duration,
    Throwable failure
) {
    public enum Status { SUCCEEDED, FAILED, SKIPPED }

    public Optional<Throwable> failureCause() {
        return Optional.ofNullable(failure);
    }

    static StepExecution succeeded(String name, int attempts, Duration duration) {
        return new StepExecution(name, Status.SUCCEEDED, attempts, duration, null);
    }

    static StepExecution failed(String name, int attempts, Duration duration, Throwable cause) {
        return new StepExecution(name, Status.FAILED, attempts, duration, cause);
    }

    static StepExecution skipped(String name) {
        return new StepExecution(name, Status.SKIPPED, 0, Duration.ZERO, null);
    }
}
