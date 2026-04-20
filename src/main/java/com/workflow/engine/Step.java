package com.workflow.engine;

/**
 * A single unit of work in a workflow.
 *
 * <p>Design choice: a functional interface taking only the shared {@link WorkflowContext}
 * and returning {@code void}. Steps signal failure by throwing; any {@link Exception}
 * is caught by the engine and counted against the step's retry budget. Results that
 * downstream steps need are written into the context rather than returned, which keeps
 * the step signature uniform regardless of payload type.</p>
 */
@FunctionalInterface
public interface Step {

    void execute(WorkflowContext context) throws Exception;

    static Step of(ThrowingConsumer body) {
        return body::accept;
    }

    @FunctionalInterface
    interface ThrowingConsumer {
        void accept(WorkflowContext ctx) throws Exception;
    }
}
