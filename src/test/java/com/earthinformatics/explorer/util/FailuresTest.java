package com.earthinformatics.explorer.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.WebClientResponseException;

class FailuresTest {

    private static WebClientResponseException http(HttpStatus status) {
        return WebClientResponseException.create(status.value(), status.getReasonPhrase(),
                new org.springframework.http.HttpHeaders(), new byte[0], null);
    }

    @Test
    @DisplayName("a 401 says the credential is the problem, not what the URL was")
    void unauthorizedNamesTheKey() {
        String reason = Failures.reason(http(HttpStatus.UNAUTHORIZED));

        assertThat(reason).contains("credential");
        assertThat(reason)
                .as("a 401 is only actionable if it names the variable to set")
                .contains("NASA_FIRMS_KEY")
                .contains("EXPLORER_AIS_API_KEY");
        assertThat(reason)
                .as("the user-facing reason must not leak the class name or the URL")
                .doesNotContain("WebClientResponseException")
                .doesNotContain("example.invalid");
    }

    @Test
    @DisplayName("a 429 is reported as a quota or rate limit")
    void tooManyRequests() {
        assertThat(Failures.reason(http(HttpStatus.TOO_MANY_REQUESTS)))
                .contains("quota").contains("429");
    }

    @Test
    @DisplayName("a 404 points at the configuration rather than the payload")
    void notFound() {
        assertThat(Failures.reason(http(HttpStatus.NOT_FOUND)))
                .contains("404").contains("configured URL");
    }

    @Test
    @DisplayName("a 5xx is reported as an upstream server error")
    void serverError() {
        assertThat(Failures.reason(http(HttpStatus.BAD_GATEWAY)))
                .contains("upstream server error").contains("502");
    }

    @Test
    @DisplayName("a key rejection is preferred over retry exhaustion when both are in the chain")
    void statusBeatsRetryExhaustion() {
        Throwable error = reactor.core.Exceptions.retryExhausted("gave up",
                http(HttpStatus.UNAUTHORIZED));

        assertThat(Failures.reason(error))
                .as("'your key is wrong' is more actionable than 'we tried a few times'")
                .contains("credential");
    }

    @Test
    @DisplayName("retry exhaustion is named as such")
    void retryExhausted() {
        // The exception class itself is package-private; this is the public factory.
        Throwable error = new IllegalStateException("wrapped",
                reactor.core.Exceptions.retryExhausted("no more attempts",
                        new ConnectException("refused")));

        assertThat(Failures.reason(error))
                .as("the real cause is several levels down the chain")
                .contains("retries exhausted");
    }

    @Test
    @DisplayName("network faults are distinguished from protocol faults")
    void networkFaults() {
        assertThat(Failures.reason(new TimeoutException())).contains("timed out");
        assertThat(Failures.reason(new UnknownHostException("firms.invalid")))
                .contains("could not be resolved");
        assertThat(Failures.reason(new ConnectException("refused"))).contains("connect");
        assertThat(Failures.reason(new IOException("stream closed"))).contains("network error");
    }

    @Test
    @DisplayName("a null error and a message-less error both yield something showable")
    void degenerateInputs() {
        assertThat(Failures.reason(null)).isNotBlank();
        assertThat(Failures.reason(new IllegalStateException())).isNotBlank();
    }

    @Test
    @DisplayName("a long message is truncated rather than blowing out the banner")
    void truncatesLongMessages() {
        String reason = Failures.reason(new IllegalStateException("x".repeat(400)));

        assertThat(reason).hasSizeLessThanOrEqualTo(120).endsWith("…");
    }
}
