package com.workflow.engine;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Immutable, ordered sequence of {@link StepDefinition}s. Build via {@link #builder(String)}.
 * A workflow is a definition only — it holds no execution state and can be run many
 * times concurrently by a {@link WorkflowEngine}.
 */
public final class Workflow {

    private final String name;
    private final List<StepDefinition> steps;

    private Workflow(String name, List<StepDefinition> steps) {
        this.name = name;
        this.steps = List.copyOf(steps);
    }

    public String name() {
        return name;
    }

    public List<StepDefinition> steps() {
        return steps;
    }

    public static Builder builder(String name) {
        return new Builder(name);
    }

    public static final class Builder {
        private final String name;
        private final List<StepDefinition> steps = new ArrayList<>();
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
            if (!names.add(stepName)) {
                throw new IllegalArgumentException("duplicate step name: " + stepName);
            }
            steps.add(new StepDefinition(stepName, step, retryPolicy));
            return this;
        }

        public Workflow build() {
            if (steps.isEmpty()) {
                throw new IllegalStateException("workflow '" + name + "' has no steps");
            }
            return new Workflow(name, steps);
        }
    }
}
