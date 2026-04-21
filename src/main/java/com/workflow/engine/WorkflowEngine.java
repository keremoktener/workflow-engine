package com.workflow.engine;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Executes a {@link Workflow} stage by stage against a {@link WorkflowContext}.
 *
 * <p>Design choices:</p>
 * <ul>
 *   <li>Sequential stages run on the calling thread. Parallel stages submit each
 *       child to an {@link ExecutorService} and block until every child finishes,
 *       including its retries — failures do not cancel siblings, because the
 *       workflow's forward progress is already gated on the whole group, and
 *       interrupting in-flight steps would corrupt per-step retry accounting.</li>
 *   <li>Parallel children run in write-isolated branches of the context. Their
 *       writes are merged back into the parent atomically once the whole group
 *       succeeds; on any failure (or sibling write conflict) nothing is merged.
 *       See {@link #runParallel} for the exact rules.</li>
 *   <li>The engine is fail-fast across stages: once any step in a stage fails after
 *       exhausting its retries, later stages are not executed and their steps are
 *       recorded as {@code SKIPPED}. Siblings inside the already-running parallel
 *       stage still get to complete and produce real outcomes.</li>
 *   <li>The engine is stateless and therefore reusable / thread-safe; all per-run
 *       state lives in the returned {@link WorkflowResult}.</li>
 *   <li>{@link InterruptedException} inside a step (or while waiting on the
 *       executor) is not retried — interruption is a control signal. The flag is
 *       restored and the run fails immediately.</li>
 *   <li>{@link Clock}, {@link Sleeper}, and the executor are injected for
 *       deterministic tests. If no executor is injected, a fresh virtual-thread
 *       executor is created per run and closed on exit; user-supplied executors
 *       are never closed by the engine.</li>
 * </ul>
 */
public final class WorkflowEngine {

    private final Clock clock;
    private final Sleeper sleeper;
    private final ExecutorService injectedExecutor;

    public WorkflowEngine() {
        this(Clock.systemUTC(), Sleeper.SYSTEM, null);
    }

    public WorkflowEngine(Clock clock, Sleeper sleeper) {
        this(clock, sleeper, null);
    }

    public WorkflowEngine(Clock clock, Sleeper sleeper, ExecutorService executor) {
        this.clock = Objects.requireNonNull(clock, "clock");
        this.sleeper = Objects.requireNonNull(sleeper, "sleeper");
        this.injectedExecutor = executor;
    }

    public WorkflowResult run(Workflow workflow, WorkflowContext context) {
        Objects.requireNonNull(workflow, "workflow");
        Objects.requireNonNull(context, "context");

        ExecutorService executor = injectedExecutor != null
            ? injectedExecutor
            : Executors.newVirtualThreadPerTaskExecutor();
        try {
            return runWith(workflow, context, executor);
        } finally {
            if (injectedExecutor == null) {
                executor.shutdown();
            }
        }
    }

    private WorkflowResult runWith(Workflow workflow, WorkflowContext context,
                                   ExecutorService executor) {
        List<StepExecution> executions = new ArrayList<>(workflow.steps().size());
        boolean failed = false;

        for (Stage stage : workflow.stages()) {
            if (failed) {
                for (StepDefinition def : stage.steps()) {
                    executions.add(StepExecution.skipped(def.name()));
                }
                continue;
            }
            List<StepExecution> stageResults = switch (stage) {
                case Stage.Sequential s ->
                    List.of(runStep(workflow.name(), s.step(), context));
                case Stage.Parallel p ->
                    runParallel(workflow.name(), p, context, executor);
            };
            executions.addAll(stageResults);
            for (StepExecution e : stageResults) {
                if (e.status() == StepExecution.Status.FAILED) {
                    failed = true;
                    break;
                }
            }
        }

        WorkflowResult.Status status = failed
            ? WorkflowResult.Status.FAILED
            : WorkflowResult.Status.SUCCEEDED;
        return new WorkflowResult(workflow.name(), status, context, List.copyOf(executions));
    }

    /**
     * Runs a parallel stage with write isolation.
     *
     * <p>Each child executes against a {@link WorkflowContext#branchOf branch} of the
     * parent context: reads see the parent's state as of group start, writes go to a
     * branch-local overlay. Branches never observe each other's writes.</p>
     *
     * <p><b>Merge is all-or-nothing.</b> Only if every child {@code SUCCEEDED} are the
     * overlays folded back into the parent. If any child failed (retries exhausted or
     * interrupted), every overlay is discarded and the parent context is left exactly
     * as it was before the stage ran.</p>
     *
     * <p><b>Merge conflicts</b> — two children writing the same key — are treated as a
     * workflow-definition bug, not a race to resolve. Nothing is merged; the
     * later-declared conflicting step is marked {@code FAILED} with a
     * {@link WorkflowException} whose cause names both steps and the key. Declaration
     * order gives a deterministic culprit; last-write-wins would be nondeterministic
     * and silently lose data. Writing a key that already existed in the parent is
     * allowed — only sibling collisions are conflicts.</p>
     */
    private List<StepExecution> runParallel(String workflowName, Stage.Parallel stage,
                                            WorkflowContext ctx, ExecutorService executor) {
        List<StepDefinition> defs = stage.steps();
        List<WorkflowContext> branches = new ArrayList<>(defs.size());
        List<Future<StepExecution>> futures = new ArrayList<>(defs.size());
        for (StepDefinition def : defs) {
            WorkflowContext branch = WorkflowContext.branchOf(ctx);
            branches.add(branch);
            futures.add(executor.submit(() -> runStep(workflowName, def, branch)));
        }

        List<StepExecution> results = new ArrayList<>(defs.size());
        boolean interrupted = false;
        boolean anyFailed = false;
        for (int i = 0; i < futures.size(); i++) {
            Future<StepExecution> f = futures.get(i);
            StepDefinition def = defs.get(i);
            if (interrupted) {
                f.cancel(true);
                results.add(syntheticInterrupted(workflowName, def));
                anyFailed = true;
                continue;
            }
            try {
                StepExecution exec = f.get();
                results.add(exec);
                anyFailed |= exec.status() == StepExecution.Status.FAILED;
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                interrupted = true;
                anyFailed = true;
                f.cancel(true);
                results.add(syntheticInterrupted(workflowName, def));
            } catch (ExecutionException ee) {
                // runStep never throws — it always returns a StepExecution — so
                // reaching here means a programming error. Surface it as a failed
                // step rather than losing the trace.
                Throwable cause = ee.getCause() != null ? ee.getCause() : ee;
                WorkflowException wex = new WorkflowException(workflowName, def.name(), 1, cause);
                results.add(StepExecution.failed(def.name(), 1, Duration.ZERO, wex));
                anyFailed = true;
            }
        }

        if (anyFailed) {
            return results;
        }

        // All children succeeded — attempt to merge overlays. Detect sibling collisions
        // first; only apply if there are none.
        Map<String, Integer> keyOwner = new LinkedHashMap<>();
        for (int i = 0; i < defs.size(); i++) {
            for (String key : branches.get(i).localWrites().keySet()) {
                Integer prior = keyOwner.putIfAbsent(key, i);
                if (prior != null) {
                    StepExecution orig = results.get(i);
                    IllegalStateException cause = new IllegalStateException(
                        "parallel merge conflict on context key '" + key + "': written by both '"
                            + defs.get(prior).name() + "' and '" + defs.get(i).name()
                            + "'; no branch writes were applied");
                    WorkflowException wex = new WorkflowException(
                        workflowName, defs.get(i).name(), orig.attempts(), cause);
                    results.set(i, StepExecution.failed(
                        orig.stepName(), orig.attempts(), orig.duration(), wex));
                    return results;
                }
            }
        }
        for (WorkflowContext branch : branches) {
            for (Map.Entry<String, Object> e : branch.localWrites().entrySet()) {
                ctx.put(e.getKey(), e.getValue());
            }
        }
        return results;
    }

    private StepExecution syntheticInterrupted(String workflowName, StepDefinition def) {
        InterruptedException ie = new InterruptedException(
            "workflow '" + workflowName + "' interrupted before step '" + def.name() + "' completed");
        WorkflowException wex = new WorkflowException(workflowName, def.name(), 0, ie);
        return StepExecution.failed(def.name(), 0, Duration.ZERO, wex);
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
