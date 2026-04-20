package com.workflow.engine;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Executes a {@link Workflow} sequentially against a {@link WorkflowContext}.
 *
 * <p>Design choices:</p>
 * <ul>
 *   <li>Synchronous, fail-fast execution on the calling thread. The first step that
 *       exhausts its retry budget stops the run; remaining steps are recorded as
 *       {@code SKIPPED} so the trace is complete.</li>
 *   <li>The engine is stateless and therefore reusable / thread-safe; all per-run
 *       state lives in the returned {@link WorkflowResult}.</li>
 *   <li>{@link InterruptedException} is not retried — interruption is a control
 *       signal, not a transient fault. The interrupt flag is restored and the run
 *       fails immediately.</li>
 *   <li>{@link Clock} and {@link Sleeper} are injected for deterministic tests.</li>
 * </ul>
 */
public final class WorkflowEngine {

    private final Clock clock;
    private final Sleeper sleeper;

    public WorkflowEngine() {
        this(Clock.systemUTC(), Sleeper.SYSTEM);
    }

    public WorkflowEngine(Clock clock, Sleeper sleeper) {
        this.clock = Objects.requireNonNull(clock, "clock");
        this.sleeper = Objects.requireNonNull(sleeper, "sleeper");
    }

    public WorkflowResult run(Workflow workflow, WorkflowContext context) {
        Objects.requireNonNull(workflow, "workflow");
        Objects.requireNonNull(context, "context");

        List<StepExecution> executions = new ArrayList<>(workflow.steps().size());
        boolean failed = false;

        for (StepDefinition def : workflow.steps()) {
            if (failed) {
                executions.add(StepExecution.skipped(def.name()));
                continue;
            }
            StepExecution exec = runStep(workflow.name(), def, context);
            executions.add(exec);
            if (exec.status() == StepExecution.Status.FAILED) {
                failed = true;
            }
        }

        WorkflowResult.Status status = failed
            ? WorkflowResult.Status.FAILED
            : WorkflowResult.Status.SUCCEEDED;
        return new WorkflowResult(workflow.name(), status, context, List.copyOf(executions));
    }

    private StepExecution runStep(String workflowName, StepDefinition def, WorkflowContext ctx) {
        RetryPolicy policy = def.retryPolicy();
        Instant start = clock.instant();
        Exception lastFailure = null;

        for (int attempt = 1; attempt <= policy.maxAttempts(); attempt++) {
            try {
                def.step().execute(ctx);
                return StepExecution.succeeded(def.name(), attempt, elapsed(start));
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                WorkflowException wex = new WorkflowException(workflowName, def.name(), attempt, ie);
                return StepExecution.failed(def.name(), attempt, elapsed(start), wex);
            } catch (Exception e) {
                lastFailure = e;
                boolean willRetry = attempt < policy.maxAttempts();
                if (willRetry) {
                    Duration wait = policy.delayFor(attempt);
                    if (wait.isZero()) {
                        continue;
                    }
                    try {
                        sleeper.sleep(wait);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        WorkflowException wex =
                            new WorkflowException(workflowName, def.name(), attempt, ie);
                        return StepExecution.failed(def.name(), attempt, elapsed(start), wex);
                    }
                }
            }
        }

        WorkflowException wex =
            new WorkflowException(workflowName, def.name(), policy.maxAttempts(), lastFailure);
        return StepExecution.failed(def.name(), policy.maxAttempts(), elapsed(start), wex);
    }

    private Duration elapsed(Instant start) {
        return Duration.between(start, clock.instant());
    }
}
