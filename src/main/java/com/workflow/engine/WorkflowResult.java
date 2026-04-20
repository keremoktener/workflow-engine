package com.workflow.engine;

import java.util.List;
import java.util.Optional;

/**
 * Outcome of a single {@link WorkflowEngine#run} invocation.
 *
 * <p>Design choice: the engine never throws on step failure. It returns a result
 * object so callers always get the full execution trace (which steps ran, how many
 * attempts, what was skipped) alongside the failure. Callers who prefer exceptions
 * can inspect {@link #failedStep()} and rethrow.</p>
 */
public record WorkflowResult(
    String workflowName,
    Status status,
    WorkflowContext context,
    List<StepExecution> stepExecutions
) {
    public enum Status { SUCCEEDED, FAILED }

    public boolean isSuccess() {
        return status == Status.SUCCEEDED;
    }

    public Optional<StepExecution> failedStep() {
        return stepExecutions.stream()
            .filter(e -> e.status() == StepExecution.Status.FAILED)
            .findFirst();
    }

    public Optional<Throwable> failureCause() {
        return failedStep().flatMap(StepExecution::failureCause);
    }
}
