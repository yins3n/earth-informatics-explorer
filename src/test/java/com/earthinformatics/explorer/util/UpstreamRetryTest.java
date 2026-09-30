package com.earthinformatics.explorer.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.test.StepVerifier;

/**
 * The retry classifier.
 *
 * <p>This is the policy every one of the seven upstream clients depends on, and both directions of
 * it are load-bearing. Retrying a permanent failure burns rate-limit tokens and delays the
 * degraded response the user actually needs; failing to retry a transient one turns a
 * two-second blip into a permanently empty layer. The distinction is easy to get subtly wrong and
 * impossible to notice by reading the call sites, so it is pinned here.
 */
class UpstreamRetryTest {

    @Test
    @DisplayName("retries 5xx, 429 and 408")
    void retriesServerSide() {
        assertThat(UpstreamRetry.isTransient(status(HttpStatus.SERVICE_UNAVAILABLE))).isTrue();
        assertThat(UpstreamRetry.isTransient(status(HttpStatus.INTERNAL_SERVER_ERROR))).isTrue();
        assertThat(UpstreamRetry.isTransient(status(HttpStatus.BAD_GATEWAY))).isTrue();
        assertThat(UpstreamRetry.isTransient(status(HttpStatus.TOO_MANY_REQUESTS))).isTrue();
        assertThat(UpstreamRetry.isTransient(status(HttpStatus.REQUEST_TIMEOUT))).isTrue();
    }

    @Test
    @DisplayName("does not retry client errors that will fail identically every time")
    void doesNotRetryClientErrors() {
        // These are the failures the degraded response exists for: a dead NOAA gauge, a bad
        // FIRMS MAP_KEY, a query a provider rejects outright.
        assertThat(UpstreamRetry.isTransient(status(HttpStatus.NOT_FOUND))).isFalse();
        assertThat(UpstreamRetry.isTransient(status(HttpStatus.UNAUTHORIZED))).isFalse();
        assertThat(UpstreamRetry.isTransient(status(HttpStatus.BAD_REQUEST))).isFalse();
        assertThat(UpstreamRetry.isTransient(status(HttpStatus.FORBIDDEN))).isFalse();
    }

    @Test
    @DisplayName("retries transport-level failures")
    void retriesTransport() {
        assertThat(UpstreamRetry.isTransient(new TimeoutException("read timed out"))).isTrue();
        assertThat(UpstreamRetry.isTransient(new UnknownHostException("api.example.invalid")))
                .isTrue();
        assertThat(UpstreamRetry.isTransient(new SocketTimeoutException("connect timed out")))
                .isTrue();
    }

    @Test
    @DisplayName("retries a 200 whose body we cannot parse, because the next node may differ")
    void retriesUnusableRepresentation() {
        // EONET's load balancer intermittently answers RSS to a JSON request with HTTP 200.
        // Classifying this as permanent was a live bug: the volcano layer degraded for a full
        // cache window over a response the very next request would have parsed.
        assertThat(UpstreamRetry.isTransient(
                new UnusableRepresentationException("answered with RSS instead of JSON"))).isTrue();

        assertThat(UpstreamRetry.isTransient(
                new UnusableRepresentationException("not JSON", new IOException("truncated"))))
                .isTrue();
    }

    @Test
    @DisplayName("does not retry our own bugs")
    void doesNotRetryProgrammingErrors() {
        // An NPE or an IllegalArgumentException is ours, not the provider's; retrying hides it.
        assertThat(UpstreamRetry.isTransient(new IllegalStateException("bug"))).isFalse();
        assertThat(UpstreamRetry.isTransient(new NullPointerException("bug"))).isFalse();
        assertThat(UpstreamRetry.isTransient(new IllegalArgumentException("bug"))).isFalse();
    }

    @Test
    @DisplayName("a parse failure inside the reactive chain is actually retried")
    void retriesThroughTheRealChain() {
        // The classifier on its own proves little: what matters is that a Mono which throws
        // UnusableRepresentationException from inside .map() retries through UpstreamRetry.
        // That is the exact shape of the EONET call, and it was silently non-retrying before.
        var attempts = new java.util.concurrent.atomic.AtomicInteger();

        reactor.core.publisher.Mono<String> flaky = reactor.core.publisher.Mono
                .fromCallable(() -> {
                    if (attempts.incrementAndGet() == 1) {
                        throw new UnusableRepresentationException("answered with RSS");
                    }
                    return "{\"events\":[]}";
                })
                .retryWhen(UpstreamRetry.backoff(java.time.Duration.ofMillis(5), 2))
                .map(body -> body.contains("events") ? "recovered" : "wrong");

        StepVerifier.create(flaky)
                .expectNext("recovered")
                .verifyComplete();
        assertThat(attempts).hasValue(2);
    }

    private static WebClientResponseException status(HttpStatus status) {
        return WebClientResponseException.create(status.value(), status.getReasonPhrase(),
                new org.springframework.http.HttpHeaders(), new byte[0], null, null);
    }
}
