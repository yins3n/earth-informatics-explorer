package com.earthinformatics.explorer.util;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.Exceptions;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;

/**
 * Shared retry policy for upstream calls.
 *
 * <p>Only transient faults are retried: connection resets, DNS hiccups, timeouts, and 5xx /
 * 429 responses. A 404 from NOAA for a decommissioned tide gauge, or a 401 from FIRMS with a
 * bad MAP_KEY, will fail identically on every attempt - retrying those just burns the token
 * bucket and delays the degraded response the user should see.
 *
 * <p>Jitter is mandatory here: several providers front their APIs with a shared cache, and a
 * synchronised retry storm from a thousand globe clients would re-create the outage.
 */
public final class UpstreamRetry {

    private UpstreamRetry() {
    }

    /** Retries on transport errors and server-side failures only. */
    public static RetryBackoffSpec backoff(Duration minBackoff, int maxRetries) {
        return Retry.backoff(maxRetries, minBackoff)
                .maxBackoff(minBackoff.multipliedBy(8))
                .jitter(0.3d)
                .filter(UpstreamRetry::isTransient)
                .onRetryExhaustedThrow((spec, signal) ->
                        Exceptions.retryExhausted("upstream retries exhausted", signal.failure()));
    }

    /** Classifies a failure as transient (connection-level or server-side). */
    public static boolean isTransient(Throwable error) {
        Throwable unwrapped = Exceptions.unwrap(error);
        // A 200 with an unusable body. Retryable because the representation depends on which
        // load-balancer node answers, not on anything we sent.
        if (unwrapped instanceof UnusableRepresentationException) {
            return true;
        }
        if (unwrapped instanceof WebClientResponseException response) {
            int status = response.getStatusCode().value();
            return status == 429 || status == 408 || (status >= 500 && status <= 599);
        }
        if (unwrapped instanceof WebClientRequestException
                || unwrapped instanceof TimeoutException
                || unwrapped instanceof IOException) {
            return true;
        }
        // Reactor Netty wraps connection failures in its own exception hierarchy.
        String name = unwrapped.getClass().getName();
        return name.startsWith("io.netty") || name.startsWith("reactor.netty");
    }
}
