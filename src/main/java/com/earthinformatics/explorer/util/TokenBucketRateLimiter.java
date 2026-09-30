package com.earthinformatics.explorer.util;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Non-blocking token bucket used to police outbound request rates per upstream host.
 *
 * <p>Free public APIs publish hard limits (NOAA CO-OPS: 3 requests/second; USGS: 120/minute).
 * A reactive application cannot use a blocking semaphore on an event loop, so the bucket
 * hands back the *delay* the caller should observe and lets Reactor do the waiting with
 * {@code Mono.delay} - which parks the timer, not the thread.
 *
 * <p>The implementation is a CAS loop over a single immutable state record: contention is
 * measured in nanoseconds and, with the handful of request rates we allow, a retry is
 * essentially never needed. All arithmetic uses {@code System.nanoTime()} so it is immune to
 * wall-clock jumps (NTP steps, suspend/resume).
 */
public final class TokenBucketRateLimiter {

    private record State(double tokens, long lastRefillNanos) {
    }

    private final double permitsPerNanosecond;
    private final double burst;
    private final AtomicReference<State> state;

    public TokenBucketRateLimiter(double tokensPerSecond, double burst) {
        if (tokensPerSecond <= 0) {
            throw new IllegalArgumentException("tokensPerSecond must be positive");
        }
        this.permitsPerNanosecond = tokensPerSecond / 1_000_000_000d;
        this.burst = Math.max(1d, burst);
        this.state = new AtomicReference<>(new State(burst, System.nanoTime()));
    }

    /**
     * Consumes one permit if available.
     *
     * @return {@link Duration#ZERO} when the caller may proceed immediately, otherwise the
     *         minimum duration to wait before retrying.
     */
    public Duration tryAcquire() {
        for (int attempt = 0; attempt < 8; attempt++) {
            State current = state.get();
            long now = System.nanoTime();
            long elapsedNanos = Math.max(0L, now - current.lastRefillNanos());
            double tokens = Math.min(burst, current.tokens() + elapsedNanos * permitsPerNanosecond);

            if (tokens >= 1d) {
                State updated = new State(tokens - 1d, now);
                if (state.compareAndSet(current, updated)) {
                    return Duration.ZERO;
                }
            } else {
                double deficit = 1d - tokens;
                long waitNanos = (long) Math.ceil(deficit / permitsPerNanosecond);
                if (state.compareAndSet(current, new State(tokens, now))) {
                    return Duration.ofNanos(Math.min(waitNanos, 1_000_000_000L));
                }
            }
        }
        // Contention fallback: a millisecond of slack is imperceptible against network latency.
        return Duration.ofMillis(1);
    }

    /** Exposed for diagnostics: how many permits are available right now. */
    public double availablePermits() {
        State current = state.get();
        long elapsedNanos = Math.max(0L, System.nanoTime() - current.lastRefillNanos());
        return Math.min(burst, current.tokens() + elapsedNanos * permitsPerNanosecond);
    }
}
