package com.workflow.engine;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Immutable, ordered sequence of {@link Stage}s. Build via {@link #builder(String)}.
 * A workflow is a definition only — it holds no execution state and can be run many
 * times concurrently by a {@link WorkflowEngine}.
 *
 * <p>Stages either wrap a single {@link StepDefinition} (the default when calling
 * {@code .step(...)}) or a group to be run concurrently (via
 * {@link Builder#parallel(Consumer)}). {@link #steps()} returns the flat list of
 * all step definitions across all stages in declaration order, which is the same
 * order as {@link WorkflowResult#stepExecutions()}.</p>
 */
public final class Workflow {

    private final String name;
    private final List<Stage> stages;
    private final List<StepDefinition> flatSteps;

    private Workflow(String name, List<Stage> stages) {
        this.name = name;
        this.stages = List.copyOf(stages);
        List<StepDefinition> flat = new ArrayList<>();
        for (Stage s : this.stages) {
            flat.addAll(s.steps());
        }
        this.flatSteps = List.copyOf(flat);
    }

    public String name() {
        return name;
    }

    /** Stages in declaration order. */
    public List<Stage> stages() {
        return stages;
    }

    /** All step definitions, flattened across stages, in declaration order. */
    public List<StepDefinition> steps() {
        return flatSteps;
    }

    public static Builder builder(String name) {
        return new Builder(name);
    }

    public static final class Builder {
        private final String name;
        private final List<Stage> stages = new ArrayList<>();
        private final Set<String> names = new LinkedHashSet<>();

        private Builder(String name) {
            if (name == null || name.isBlank()) {
                throw new IllegalArgumentException("workflow name must not be blank");
            }
            this.name = name;
        }

        public Builder step(String stepName, Step step) {
            return step(stepName, step, RetryPolicy.none());
        }

        public Builder step(String stepName, Step step, RetryPolicy retryPolicy) {
            StepDefinition def = registerStep(stepName, step, retryPolicy);
            stages.add(new Stage.Sequential(def));
            return this;
        }

        /**
         * Add a group of steps that the engine will run concurrently. The workflow
         * blocks at this stage until every child has finished (including its retries).
         *
         * <p>Children are built with the same {@code .step(...)} API. Each child's
         * {@link RetryPolicy} applies independently, and each produces its own
         * {@link StepExecution} in the final trace. Step names share the
         * workflow-wide uniqueness check.</p>
         */
        public Builder parallel(Consumer<ParallelBuilder> body) {
            Objects.requireNonNull(body, "body");
            ParallelBuilder pb = new ParallelBuilder(this);
            body.accept(pb);
            List<StepDefinition> group = pb.collected;
            if (group.isEmpty()) {
                throw new IllegalStateException(
                    "parallel(...) block added no steps to workflow '" + name + "'");
            }
            stages.add(new Stage.Parallel(group));
            return this;
        }

        public Workflow build() {
            if (stages.isEmpty()) {
                throw new IllegalStateException("workflow '" + name + "' has no steps");
            }
            return new Workflow(name, stages);
        }

        private StepDefinition registerStep(String stepName, Step step, RetryPolicy retryPolicy) {
            if (!names.add(stepName)) {
                throw new IllegalArgumentException("duplicate step name: " + stepName);
            }
            return new StepDefinition(stepName, step, retryPolicy);
        }
    }

    /** Builder for the children of a {@link Stage.Parallel}. */
    public static final class ParallelBuilder {
        private final Builder parent;
        private final List<StepDefinition> collected = new ArrayList<>();

        private ParallelBuilder(Builder parent) {
            this.parent = parent;
        }

        public ParallelBuilder step(String stepName, Step step) {
            return step(stepName, step, RetryPolicy.none());
        }

        public ParallelBuilder step(String stepName, Step step, RetryPolicy retryPolicy) {
            collected.add(parent.registerStep(stepName, step, retryPolicy));
            return this;
        }
    }
}
