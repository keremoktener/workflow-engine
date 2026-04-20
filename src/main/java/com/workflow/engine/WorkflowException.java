package com.workflow.engine;

/**
 * Wraps a step's terminal failure with the workflow/step identity and attempt count,
 * so a single catch site has everything needed for diagnostics. Stored as the
 * {@code failure} on the {@link StepExecution} of the failed step.
 */
public final class WorkflowException extends RuntimeException {

    private final String workflowName;
    private final String stepName;
    private final int attempts;

    public WorkflowException(String workflowName, String stepName, int attempts, Throwable cause) {
        super("Workflow '" + workflowName + "' failed at step '" + stepName
              + "' after " + attempts + " attempt(s): " + cause.getMessage(), cause);
        this.workflowName = workflowName;
        this.stepName = stepName;
        this.attempts = attempts;
    }

    public String workflowName() {
        return workflowName;
    }

    public String stepName() {
        return stepName;
    }

    public int attempts() {
        return attempts;
    }
}
