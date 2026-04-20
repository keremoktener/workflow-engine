package com.workflow.engine;

import java.time.Duration;
import java.util.Objects;

/**
 * Per-step retry configuration.
 *
 * <p>{@code maxAttempts} is the total number of tries (first attempt + retries),
 * so {@code maxAttempts = 1} means "no retry".</p>
 *
 * <p>Backoff is a single, general model: the delay before attempt {@code n+1}
 * (after attempt {@code n} just failed) is
 * <pre>
 *   min(initialDelay * multiplier^(n-1), maxDelay)
 * </pre>
 * <ul>
 *   <li>Fixed delay is the special case {@code multiplier = 1.0}, in which the
 *       formula collapses to {@code initialDelay} every time. This is what
 *       {@link #of(int, Duration)} returns — the existing fixed-delay callers
 *       keep their exact previous behavior.</li>
 *   <li>Exponential backoff uses {@code multiplier > 1.0} and is capped by
 *       {@code maxDelay} so the wait cannot grow without bound — see
 *       {@link #exponential(int, Duration, double, Duration)}.</li>
 * </ul>
 * The value returned by {@link #delayFor(int)} is always clamped to
 * {@code [0, maxDelay]}, which also makes the math safe against overflow for
 * large attempt counts.
 */
public record RetryPolicy(
    int maxAttempts,
    Duration initialDelay,
    double multiplier,
    Duration maxDelay
) {

    public RetryPolicy {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1, got " + maxAttempts);
        }
        Objects.requireNonNull(initialDelay, "initialDelay");
        if (initialDelay.isNegative()) {
            throw new IllegalArgumentException("initialDelay must not be negative");
        }
        if (!(multiplier >= 1.0) || Double.isNaN(multiplier) || Double.isInfinite(multiplier)) {
            throw new IllegalArgumentException(
                "multiplier must be a finite value >= 1.0, got " + multiplier);
        }
        Objects.requireNonNull(maxDelay, "maxDelay");
        if (maxDelay.isNegative()) {
            throw new IllegalArgumentException("maxDelay must not be negative");
        }
        if (maxDelay.compareTo(initialDelay) < 0) {
            throw new IllegalArgumentException(
                "maxDelay (" + maxDelay + ") must be >= initialDelay (" + initialDelay + ")");
        }
    }

    /** No retry: a single attempt, no delay. */
    public static RetryPolicy none() {
        return new RetryPolicy(1, Duration.ZERO, 1.0, Duration.ZERO);
    }

    /**
     * Fixed delay between attempts — the same configuration available in v1.
     * Modeled as exponential backoff with {@code multiplier = 1.0} and
     * {@code maxDelay = delay}, so {@link #delayFor(int)} returns {@code delay}
     * for every retry.
     */
    public static RetryPolicy of(int maxAttempts, Duration delay) {
        return new RetryPolicy(maxAttempts, delay, 1.0, delay);
    }

    /**
     * Exponential backoff: delay doubles (or scales by {@code multiplier}) after
     * each failure, capped at {@code maxDelay}.
     *
     * @param maxAttempts  total attempts including the first (must be >= 1)
     * @param initialDelay delay before the 2nd attempt (must be >= 0)
     * @param multiplier   growth factor per failure (must be >= 1.0)
     * @param maxDelay     hard ceiling on any single delay (must be >= initialDelay)
     */
    public static RetryPolicy exponential(
        int maxAttempts, Duration initialDelay, double multiplier, Duration maxDelay) {
        return new RetryPolicy(maxAttempts, initialDelay, multiplier, maxDelay);
    }

    /**
     * Delay the engine should wait <em>after</em> {@code failedAttempt} before the
     * next attempt. {@code failedAttempt} is 1-based: passing {@code 1} gives the
     * delay between the 1st and 2nd attempt.
     *
     * <p>Always returns a value in {@code [Duration.ZERO, maxDelay]}.</p>
     */
    public Duration delayFor(int failedAttempt) {
        if (failedAttempt < 1) {
            throw new IllegalArgumentException("failedAttempt must be >= 1, got " + failedAttempt);
        }
        if (initialDelay.isZero()) {
            return Duration.ZERO;
        }
        // Compute in double-nanos to avoid long overflow across many attempts,
        // then clamp to maxDelay. Math.pow on a non-negative finite base is safe;
        // any overflow just becomes +Infinity, which the clamp handles.
        double scaled = initialDelay.toNanos() * Math.pow(multiplier, failedAttempt - 1);
        long capNanos = maxDelay.toNanos();
        if (!Double.isFinite(scaled) || scaled >= capNanos) {
            return maxDelay;
        }
        return Duration.ofNanos((long) scaled);
    }
}
