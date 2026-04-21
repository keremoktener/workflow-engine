package com.workflow.engine;

import java.util.List;
import java.util.Objects;

/**
 * A unit of workflow execution: either one step run by itself, or a group of steps
 * run concurrently.
 *
 * <p>Design choice: stages are the engine's iteration unit, but the per-step
 * {@link StepDefinition} (with its name and {@link RetryPolicy}) remains the leaf.
 * That keeps retry and execution tracking unchanged — each child of a parallel
 * stage retries independently and contributes its own {@link StepExecution} to the
 * flat trace — while only the execution order becomes richer.</p>
 *
 * <p>A {@link Parallel} stage waits for all children to finish before the workflow
 * advances, regardless of outcome. If any child fails after exhausting its retries,
 * the workflow fails once the stage completes; siblings are not cancelled.</p>
 */
public sealed interface Stage permits Stage.Sequential, Stage.Parallel {

    /** Steps belonging to this stage, in declaration order. */
    List<StepDefinition> steps();

    record Sequential(StepDefinition step) implements Stage {
        public Sequential {
            Objects.requireNonNull(step, "step");
        }
        @Override public List<StepDefinition> steps() {
            return List.of(step);
        }
    }

    record Parallel(List<StepDefinition> steps) implements Stage {
        public Parallel {
            Objects.requireNonNull(steps, "steps");
            steps = List.copyOf(steps);
            if (steps.isEmpty()) {
                throw new IllegalArgumentException("parallel stage must contain at least one step");
            }
        }
    }
}
