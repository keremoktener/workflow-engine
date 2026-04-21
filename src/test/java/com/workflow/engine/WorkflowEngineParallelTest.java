package com.workflow.engine;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class WorkflowEngineParallelTest {

    /** Thread-safe sleeper that records delays — needed because parallel steps may retry concurrently. */
    static final class ConcurrentRecordingSleeper implements Sleeper {
        final List<Duration> delays = Collections.synchronizedList(new ArrayList<>());
        @Override public void sleep(Duration d) { delays.add(d); }
    }

    private ExecutorService pool;

    @AfterEach
    void tearDown() {
        if (pool != null) {
            pool.shutdownNow();
        }
    }

    private WorkflowEngine engineWithPool(Sleeper sleeper) {
        pool = Executors.newFixedThreadPool(4);
        return new WorkflowEngine(java.time.Clock.systemUTC(), sleeper, pool);
    }

    @Test
    void parallelStage_runsAllChildren_andBlocksUntilAllComplete() throws InterruptedException {
        // Three tasks must all hit the barrier before any can finish — proves they run concurrently.
        CountDownLatch barrier = new CountDownLatch(3);
        Map<String, Long> finishOrder = new ConcurrentHashMap<>();
        AtomicInteger seq = new AtomicInteger();

        Workflow wf = Workflow.builder("fan-out")
            .step("pre", ctx -> ctx.put("start", true))
            .parallel(p -> p
                .step("a", ctx -> {
                    barrier.countDown();
                    assertTrue(barrier.await(2, TimeUnit.SECONDS));
                    finishOrder.put("a", (long) seq.incrementAndGet());
                    ctx.put("a", 1);
                })
                .step("b", ctx -> {
                    barrier.countDown();
                    assertTrue(barrier.await(2, TimeUnit.SECONDS));
                    finishOrder.put("b", (long) seq.incrementAndGet());
                    ctx.put("b", 2);
                })
                .step("c", ctx -> {
                    barrier.countDown();
                    assertTrue(barrier.await(2, TimeUnit.SECONDS));
                    finishOrder.put("c", (long) seq.incrementAndGet());
                    ctx.put("c", 3);
                }))
            .step("post", ctx -> ctx.put("end", true))
            .build();

        WorkflowResult result = engineWithPool(Sleeper.SYSTEM).run(wf, WorkflowContext.empty());

        assertTrue(result.isSuccess());
        assertEquals(1, result.context().require("a", Integer.class));
        assertEquals(2, result.context().require("b", Integer.class));
        assertEquals(3, result.context().require("c", Integer.class));
        assertTrue(result.context().require("end", Boolean.class));

        // Trace is flat, in declaration order: pre, a, b, c, post.
        List<String> traceNames = result.stepExecutions().stream().map(StepExecution::stepName).toList();
        assertEquals(List.of("pre", "a", "b", "c", "post"), traceNames);
        assertTrue(result.stepExecutions().stream()
            .allMatch(e -> e.status() == StepExecution.Status.SUCCEEDED));

        assertEquals(3, finishOrder.size(), "every child ran exactly once");
    }

    @Test
    void parallelChild_appliesItsOwnRetryPolicy_independentlyOfSiblings() {
        ConcurrentRecordingSleeper sleeper = new ConcurrentRecordingSleeper();
        AtomicInteger flakyCalls = new AtomicInteger();
        AtomicInteger steadyCalls = new AtomicInteger();

        Workflow wf = Workflow.builder("wf")
            .parallel(p -> p
                .step("flaky", ctx -> {
                    if (flakyCalls.incrementAndGet() < 3) throw new RuntimeException("transient");
                    ctx.put("flaky", "ok");
                }, RetryPolicy.of(5, Duration.ofMillis(10)))
                .step("steady", ctx -> {
                    steadyCalls.incrementAndGet();
                    ctx.put("steady", "ok");
                }))
            .build();

        WorkflowResult result = engineWithPool(sleeper).run(wf, WorkflowContext.empty());

        assertTrue(result.isSuccess());
        assertEquals(3, flakyCalls.get());
        assertEquals(1, steadyCalls.get());

        StepExecution flakyExec = result.stepExecutions().stream()
            .filter(e -> e.stepName().equals("flaky")).findFirst().orElseThrow();
        StepExecution steadyExec = result.stepExecutions().stream()
            .filter(e -> e.stepName().equals("steady")).findFirst().orElseThrow();
        assertEquals(3, flakyExec.attempts());
        assertEquals(1, steadyExec.attempts());

        // Only the flaky step slept: 2 retry delays, 10ms each.
        assertEquals(
            List.of(Duration.ofMillis(10), Duration.ofMillis(10)),
            new ArrayList<>(sleeper.delays));
    }

    @Test
    void parallelStageFailure_letsSiblingsFinish_thenSkipsLaterStages() {
        ConcurrentRecordingSleeper sleeper = new ConcurrentRecordingSleeper();
        AtomicInteger siblingCalls = new AtomicInteger();
        IllegalStateException root = new IllegalStateException("doomed");

        Workflow wf = Workflow.builder("wf")
            .parallel(p -> p
                .step("doomed", ctx -> { throw root; }, RetryPolicy.of(2, Duration.ofMillis(5)))
                .step("sibling", ctx -> {
                    siblingCalls.incrementAndGet();
                    ctx.put("sibling", "done");
                }))
            .step("after", ctx -> ctx.put("after", true))
            .build();

        WorkflowResult result = engineWithPool(sleeper).run(wf, WorkflowContext.empty());

        assertFalse(result.isSuccess());
        assertEquals(1, siblingCalls.get(), "sibling was not cancelled — completed normally");

        // Sibling's step still SUCCEEDED (it ran to completion), but its writes
        // were NOT merged because the group failed — atomic all-or-nothing.
        StepExecution sibling = result.stepExecutions().stream()
            .filter(e -> e.stepName().equals("sibling")).findFirst().orElseThrow();
        assertEquals(StepExecution.Status.SUCCEEDED, sibling.status());
        assertFalse(result.context().contains("sibling"),
            "no branch writes are merged when any sibling fails");

        StepExecution doomed = result.failedStep().orElseThrow();
        assertEquals("doomed", doomed.stepName());
        assertEquals(2, doomed.attempts());
        assertSame(root, ((WorkflowException) doomed.failure()).getCause());

        StepExecution after = result.stepExecutions().stream()
            .filter(e -> e.stepName().equals("after")).findFirst().orElseThrow();
        assertEquals(StepExecution.Status.SKIPPED, after.status());
        assertFalse(result.context().contains("after"));
    }

    @Test
    void parallelBuilder_rejectsEmptyGroup() {
        Workflow.Builder b = Workflow.builder("wf").step("x", c -> {});
        assertThrows(IllegalStateException.class, () -> b.parallel(p -> { /* no steps */ }));
    }

    @Test
    void parallelBuilder_rejectsDuplicateNames_acrossStagesAndWithinGroup() {
        Workflow.Builder b1 = Workflow.builder("wf").step("x", c -> {});
        assertThrows(IllegalArgumentException.class,
            () -> b1.parallel(p -> p.step("x", c -> {})));

        Workflow.Builder b2 = Workflow.builder("wf");
        assertThrows(IllegalArgumentException.class,
            () -> b2.parallel(p -> p.step("dup", c -> {}).step("dup", c -> {})));
    }

    @Test
    void emptyParallelStage_recordsSkippedForAllChildren_whenPriorStepFails() {
        ConcurrentRecordingSleeper sleeper = new ConcurrentRecordingSleeper();
        Workflow wf = Workflow.builder("wf")
            .step("fail", ctx -> { throw new RuntimeException("boom"); })
            .parallel(p -> p
                .step("p1", ctx -> ctx.put("p1", 1))
                .step("p2", ctx -> ctx.put("p2", 2)))
            .build();

        WorkflowResult result = engineWithPool(sleeper).run(wf, WorkflowContext.empty());

        assertFalse(result.isSuccess());
        StepExecution p1 = result.stepExecutions().stream()
            .filter(e -> e.stepName().equals("p1")).findFirst().orElseThrow();
        StepExecution p2 = result.stepExecutions().stream()
            .filter(e -> e.stepName().equals("p2")).findFirst().orElseThrow();
        assertEquals(StepExecution.Status.SKIPPED, p1.status());
        assertEquals(StepExecution.Status.SKIPPED, p2.status());
        assertFalse(result.context().contains("p1"));
        assertFalse(result.context().contains("p2"));
    }

    @Test
    void workflow_stepsAndStages_flattenAndExposeStructure() {
        Workflow wf = Workflow.builder("wf")
            .step("a", c -> {})
            .parallel(p -> p.step("b", c -> {}).step("c", c -> {}))
            .step("d", c -> {})
            .build();

        assertEquals(List.of("a", "b", "c", "d"),
            wf.steps().stream().map(StepDefinition::name).toList());
        assertEquals(3, wf.stages().size());
        assertInstanceOf(Stage.Sequential.class, wf.stages().get(0));
        assertInstanceOf(Stage.Parallel.class, wf.stages().get(1));
        assertInstanceOf(Stage.Sequential.class, wf.stages().get(2));
        assertEquals(2, wf.stages().get(1).steps().size());
    }

    @Test
    void parallelBranches_readParentSnapshot_butNotEachOthersWrites() throws InterruptedException {
        // Both branches read "in" from the parent; branch A writes "x" early, then
        // branch B (which runs after A has written) must NOT see "x".
        CountDownLatch aWrote = new CountDownLatch(1);

        Workflow wf = Workflow.builder("wf")
            .step("seed", ctx -> ctx.put("in", 10))
            .parallel(p -> p
                .step("a", ctx -> {
                    ctx.put("x", ctx.require("in", Integer.class) + 1);
                    aWrote.countDown();
                })
                .step("b", ctx -> {
                    assertTrue(aWrote.await(2, TimeUnit.SECONDS));
                    // B sees parent "in" but not sibling's "x".
                    assertEquals(10, ctx.require("in", Integer.class));
                    assertFalse(ctx.contains("x"), "sibling writes must be isolated");
                    ctx.put("y", ctx.require("in", Integer.class) + 2);
                }))
            .step("combine", ctx -> ctx.put("sum",
                ctx.require("x", Integer.class) + ctx.require("y", Integer.class)))
            .build();

        WorkflowResult result = engineWithPool(Sleeper.SYSTEM).run(wf, WorkflowContext.empty());

        assertTrue(result.isSuccess());
        assertEquals(11, result.context().require("x", Integer.class));
        assertEquals(12, result.context().require("y", Integer.class));
        assertEquals(23, result.context().require("sum", Integer.class));
    }

    @Test
    void parallelMergeConflict_failsLaterDeclaredStep_andMergesNothing() {
        Workflow wf = Workflow.builder("wf")
            .step("seed", ctx -> ctx.put("keep", "original"))
            .parallel(p -> p
                .step("first", ctx -> {
                    ctx.put("shared", "from-first");
                    ctx.put("only-first", 1);
                })
                .step("second", ctx -> {
                    ctx.put("shared", "from-second");
                    ctx.put("only-second", 2);
                }))
            .step("after", ctx -> ctx.put("after", true))
            .build();

        WorkflowResult result = engineWithPool(Sleeper.SYSTEM).run(wf, WorkflowContext.empty());

        assertFalse(result.isSuccess());

        // "first" still SUCCEEDED; "second" (later-declared) carries the conflict failure.
        StepExecution first = result.stepExecutions().stream()
            .filter(e -> e.stepName().equals("first")).findFirst().orElseThrow();
        assertEquals(StepExecution.Status.SUCCEEDED, first.status());

        StepExecution failed = result.failedStep().orElseThrow();
        assertEquals("second", failed.stepName());
        WorkflowException wex = assertInstanceOf(WorkflowException.class, failed.failure());
        assertInstanceOf(IllegalStateException.class, wex.getCause());
        String msg = wex.getCause().getMessage();
        assertTrue(msg.contains("shared"), msg);
        assertTrue(msg.contains("first"), msg);
        assertTrue(msg.contains("second"), msg);

        // Nothing from either branch was merged; parent writes survive untouched.
        assertFalse(result.context().contains("shared"));
        assertFalse(result.context().contains("only-first"));
        assertFalse(result.context().contains("only-second"));
        assertEquals("original", result.context().require("keep", String.class));

        // Downstream stage skipped.
        StepExecution after = result.stepExecutions().stream()
            .filter(e -> e.stepName().equals("after")).findFirst().orElseThrow();
        assertEquals(StepExecution.Status.SKIPPED, after.status());
    }

    @Test
    void parallelBranch_mayOverwriteParentKey_withoutConflict() {
        Workflow wf = Workflow.builder("wf")
            .step("seed", ctx -> ctx.put("k", "old"))
            .parallel(p -> p
                .step("writer", ctx -> ctx.put("k", "new"))
                .step("other", ctx -> ctx.put("other", 1)))
            .build();

        WorkflowResult result = engineWithPool(Sleeper.SYSTEM).run(wf, WorkflowContext.empty());

        assertTrue(result.isSuccess());
        assertEquals("new", result.context().require("k", String.class));
        assertEquals(1, result.context().require("other", Integer.class));
    }

    @Test
    void branchContext_readsThroughSnapshot_andTracksLocalWrites() {
        WorkflowContext parent = WorkflowContext.empty().put("p", 1);
        WorkflowContext branch = WorkflowContext.branchOf(parent);

        assertEquals(1, branch.require("p", Integer.class));
        assertTrue(branch.localWrites().isEmpty());

        branch.put("b", 2);
        assertEquals(Map.of("b", 2), branch.localWrites());
        assertFalse(parent.contains("b"), "branch write must not leak into parent");

        // Parent mutation after branch creation is NOT visible to the branch (snapshot).
        parent.put("late", 3);
        assertFalse(branch.contains("late"));

        // asMap on branch is the merged view.
        assertEquals(Map.of("p", 1, "b", 2), branch.asMap());
    }

    @Test
    void defaultEngine_usesVirtualThreadsPerRun_forParallelStages() {
        // No injected executor — engine must still run the parallel group.
        Workflow wf = Workflow.builder("wf")
            .parallel(p -> p
                .step("a", ctx -> ctx.put("a", Thread.currentThread().isVirtual()))
                .step("b", ctx -> ctx.put("b", Thread.currentThread().isVirtual())))
            .build();

        WorkflowResult result = new WorkflowEngine().run(wf, WorkflowContext.empty());

        assertTrue(result.isSuccess());
        assertTrue(result.context().require("a", Boolean.class));
        assertTrue(result.context().require("b", Boolean.class));
    }
}
