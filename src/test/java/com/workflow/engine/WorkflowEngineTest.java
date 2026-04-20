package com.workflow.engine;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class WorkflowEngineTest {

    /** Sleeper that records requested delays instead of blocking. */
    static final class RecordingSleeper implements Sleeper {
        final List<Duration> delays = new ArrayList<>();
        @Override public void sleep(Duration d) { delays.add(d); }
    }

    private WorkflowEngine engine(RecordingSleeper s) {
        return new WorkflowEngine(java.time.Clock.systemUTC(), s);
    }

    @Test
    void happyPath_runsStepsInOrder_andSharesContext() {
        List<String> order = new ArrayList<>();

        Workflow wf = Workflow.builder("order-pipeline")
            .step("load", ctx -> { order.add("load"); ctx.put("amount", 100); })
            .step("tax", ctx -> {
                order.add("tax");
                int amount = ctx.require("amount", Integer.class);
                ctx.put("total", amount + 20);
            })
            .step("persist", ctx -> order.add("persist"))
            .build();

        WorkflowResult result = new WorkflowEngine().run(wf, WorkflowContext.empty());

        assertTrue(result.isSuccess());
        assertEquals(List.of("load", "tax", "persist"), order);
        assertEquals(120, result.context().require("total", Integer.class));
        assertEquals(3, result.stepExecutions().size());
        assertTrue(result.stepExecutions().stream()
            .allMatch(e -> e.status() == StepExecution.Status.SUCCEEDED && e.attempts() == 1));
        assertTrue(result.failedStep().isEmpty());
    }

    @Test
    void retry_succeedsWithinBudget_recordsAttemptsAndDelays() {
        AtomicInteger calls = new AtomicInteger();
        RecordingSleeper sleeper = new RecordingSleeper();

        Workflow wf = Workflow.builder("flaky")
            .step("flaky", ctx -> {
                if (calls.incrementAndGet() < 3) {
                    throw new RuntimeException("transient");
                }
                ctx.put("ok", true);
            }, RetryPolicy.of(5, Duration.ofMillis(50)))
            .build();

        WorkflowResult result = engine(sleeper).run(wf, WorkflowContext.empty());

        assertTrue(result.isSuccess());
        StepExecution exec = result.stepExecutions().get(0);
        assertEquals(3, exec.attempts());
        assertEquals(StepExecution.Status.SUCCEEDED, exec.status());
        assertEquals(List.of(Duration.ofMillis(50), Duration.ofMillis(50)), sleeper.delays);
        assertTrue(result.context().require("ok", Boolean.class));
    }

    @Test
    void failure_exhaustsRetries_stopsWorkflow_andReportsCause() {
        RecordingSleeper sleeper = new RecordingSleeper();
        IllegalStateException root = new IllegalStateException("boom");

        Workflow wf = Workflow.builder("wf")
            .step("a", ctx -> ctx.put("a", 1))
            .step("b", ctx -> { throw root; }, RetryPolicy.of(3, Duration.ofMillis(10)))
            .step("c", ctx -> ctx.put("c", 1))
            .build();

        WorkflowResult result = engine(sleeper).run(wf, WorkflowContext.empty());

        assertFalse(result.isSuccess());
        assertEquals(WorkflowResult.Status.FAILED, result.status());

        // step trace: a succeeded, b failed after 3 attempts, c skipped
        assertEquals(StepExecution.Status.SUCCEEDED, result.stepExecutions().get(0).status());

        StepExecution failed = result.failedStep().orElseThrow();
        assertEquals("b", failed.stepName());
        assertEquals(3, failed.attempts());
        assertEquals(StepExecution.Status.FAILED, failed.status());

        WorkflowException wex = assertInstanceOf(WorkflowException.class, failed.failure());
        assertEquals("b", wex.stepName());
        assertEquals("wf", wex.workflowName());
        assertEquals(3, wex.attempts());
        assertSame(root, wex.getCause());
        assertSame(wex, result.failureCause().orElseThrow());

        assertEquals(StepExecution.Status.SKIPPED, result.stepExecutions().get(2).status());
        assertFalse(result.context().contains("c"), "downstream step must not run");

        // 3 attempts -> 2 delays
        assertEquals(2, sleeper.delays.size());
    }

    @Test
    void retryPolicyNone_failsOnFirstException_withoutSleeping() {
        RecordingSleeper sleeper = new RecordingSleeper();
        Workflow wf = Workflow.builder("wf")
            .step("only", ctx -> { throw new RuntimeException("nope"); })
            .build();

        WorkflowResult result = engine(sleeper).run(wf, WorkflowContext.empty());

        assertFalse(result.isSuccess());
        assertEquals(1, result.failedStep().orElseThrow().attempts());
        assertTrue(sleeper.delays.isEmpty());
    }

    @Test
    void interruptedStep_isNotRetried_andFailsImmediately() {
        AtomicInteger calls = new AtomicInteger();
        Workflow wf = Workflow.builder("wf")
            .step("io", ctx -> {
                calls.incrementAndGet();
                throw new InterruptedException("stop");
            }, RetryPolicy.of(5, Duration.ZERO))
            .build();

        WorkflowResult result = new WorkflowEngine().run(wf, WorkflowContext.empty());
        // clear flag so it doesn't leak into other tests
        assertTrue(Thread.interrupted(), "interrupt flag should be restored");

        assertFalse(result.isSuccess());
        assertEquals(1, calls.get());
        assertEquals(1, result.failedStep().orElseThrow().attempts());
        assertInstanceOf(InterruptedException.class,
            result.failureCause().orElseThrow().getCause());
    }

    @Test
    void initialContext_isReadableByFirstStep() {
        Workflow wf = Workflow.builder("wf")
            .step("double", ctx -> ctx.put("out", ctx.require("in", Integer.class) * 2))
            .build();

        WorkflowContext ctx = WorkflowContext.empty().put("in", 21);
        WorkflowResult result = new WorkflowEngine().run(wf, ctx);

        assertTrue(result.isSuccess());
        assertEquals(42, ctx.require("out", Integer.class));
    }

    @Test
    void builder_rejectsDuplicateStepNames() {
        Workflow.Builder b = Workflow.builder("wf").step("x", c -> {});
        assertThrows(IllegalArgumentException.class, () -> b.step("x", c -> {}));
    }

    @Test
    void exponentialBackoff_delaysGrowGeometrically_untilSuccess() {
        AtomicInteger calls = new AtomicInteger();
        RecordingSleeper sleeper = new RecordingSleeper();

        RetryPolicy policy = RetryPolicy.exponential(
            5, Duration.ofMillis(100), 2.0, Duration.ofSeconds(10));

        Workflow wf = Workflow.builder("wf")
            .step("flaky", ctx -> {
                if (calls.incrementAndGet() < 4) throw new RuntimeException("transient");
            }, policy)
            .build();

        WorkflowResult result = engine(sleeper).run(wf, WorkflowContext.empty());

        assertTrue(result.isSuccess());
        assertEquals(4, result.stepExecutions().get(0).attempts());
        // 3 retries -> delays after attempts 1, 2, 3: 100, 200, 400 ms
        assertEquals(
            List.of(Duration.ofMillis(100), Duration.ofMillis(200), Duration.ofMillis(400)),
            sleeper.delays);
    }

    @Test
    void exponentialBackoff_isCappedAtMaxDelay() {
        AtomicInteger calls = new AtomicInteger();
        RecordingSleeper sleeper = new RecordingSleeper();

        // 100ms, *3 → 100, 300, 900, 2700, 8100 …; cap at 500ms → 100, 300, 500, 500, 500
        RetryPolicy policy = RetryPolicy.exponential(
            6, Duration.ofMillis(100), 3.0, Duration.ofMillis(500));

        Workflow wf = Workflow.builder("wf")
            .step("always-fails", ctx -> {
                calls.incrementAndGet();
                throw new RuntimeException("nope");
            }, policy)
            .build();

        WorkflowResult result = engine(sleeper).run(wf, WorkflowContext.empty());

        assertFalse(result.isSuccess());
        assertEquals(6, result.failedStep().orElseThrow().attempts());
        assertEquals(6, calls.get());
        // 6 attempts -> 5 delays
        assertEquals(
            List.of(
                Duration.ofMillis(100),
                Duration.ofMillis(300),
                Duration.ofMillis(500), // would be 900, capped
                Duration.ofMillis(500),
                Duration.ofMillis(500)),
            sleeper.delays);
    }

    @Test
    void exponentialBackoff_withMultiplierOne_behavesLikeFixedDelay() {
        RecordingSleeper sleeper = new RecordingSleeper();
        RetryPolicy policy = RetryPolicy.exponential(
            4, Duration.ofMillis(50), 1.0, Duration.ofMillis(50));

        Workflow wf = Workflow.builder("wf")
            .step("x", ctx -> { throw new RuntimeException("boom"); }, policy)
            .build();

        WorkflowResult result = engine(sleeper).run(wf, WorkflowContext.empty());

        assertFalse(result.isSuccess());
        assertEquals(
            List.of(Duration.ofMillis(50), Duration.ofMillis(50), Duration.ofMillis(50)),
            sleeper.delays);
    }

    @Test
    void retryPolicy_delayFor_clampsOverflowToMaxDelay() {
        // Huge attempt number × multiplier=2 would overflow a long in nanos;
        // delayFor must clamp rather than return a bogus value.
        RetryPolicy p = RetryPolicy.exponential(
            1000, Duration.ofMillis(1), 2.0, Duration.ofSeconds(30));
        assertEquals(Duration.ofSeconds(30), p.delayFor(999));
    }

    @Test
    void retryPolicy_rejectsInvalidConfig() {
        assertThrows(IllegalArgumentException.class,
            () -> RetryPolicy.exponential(3, Duration.ofMillis(100), 0.5, Duration.ofSeconds(1)));
        assertThrows(IllegalArgumentException.class,
            () -> RetryPolicy.exponential(3, Duration.ofMillis(100), 2.0, Duration.ofMillis(50)));
        assertThrows(IllegalArgumentException.class,
            () -> RetryPolicy.exponential(3, Duration.ofMillis(-1), 2.0, Duration.ofSeconds(1)));
        assertThrows(IllegalArgumentException.class,
            () -> RetryPolicy.exponential(3, Duration.ofMillis(100), Double.NaN, Duration.ofSeconds(1)));
    }
}
