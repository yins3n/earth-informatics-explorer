package com.earthinformatics.explorer.util;

import java.util.concurrent.TimeoutException;
import org.springframework.web.reactive.function.client.WebClientResponseException;

/**
 * Turns an upstream failure into something worth showing a person.
 *
 * <p>Degradation reasons are surfaced directly in the API ({@code meta.error}) and rendered in
 * the dashboard banner, so a raw {@code Throwable.toString()} there is both ugly and mostly
 * noise: the class name and the full request URL say nothing the operator can act on. What they
 * need to know is which knob to turn - a missing key, a quota, a bad layer name - so that is
 * what the message says.
 *
 * <p>Nothing is lost: each service logs the full throwable at the point it converts it, so the
 * class name and request URL remain in the log for correlation - they are just not pushed at
 * the user.
 */
public final class Failures {

    private Failures() {
    }

    /** A short, human-readable reason. Never null; falls back to the exception's own message. */
    public static String reason(Throwable error) {
        if (error == null) {
            return "upstream unavailable";
        }
        Throwable root = rootCause(error);

        if (root instanceof WebClientResponseException response) {
            int status = response.getStatusCode().value();
            return switch (status) {
                case 401, 403 -> "upstream rejected the credentials (HTTP " + status
                        + ") - set NASA_FIRMS_KEY for wildfires or EXPLORER_AIS_API_KEY for vessels"
                        + " (the bundled FIRMS DEMO_KEY is not authorised for real queries)";
                case 404 -> "upstream endpoint not found (HTTP 404) - check the configured URL";
                case 429 -> "upstream rate limit or daily quota exhausted (HTTP 429)";
                case 400 -> "upstream rejected the request (HTTP 400) - check the query parameters";
                default -> status >= 500
                        ? "upstream server error (HTTP " + status + ")"
                        : "upstream returned HTTP " + status;
            };
        }
        // Retry exhaustion is checked across the whole chain, not just the root cause: the root
        // cause of a RetryExhaustedException is the underlying transport fault, so resolving it
        // first would report a bare "connection refused" and lose the fact that retries ran out.
        // The status-code branch above still wins, because "your key is wrong" is more
        // actionable than "we tried a few times".
        if (isRetryExhausted(error)) {
            return "upstream retries exhausted - the service is unreachable or failing";
        }
        if (root instanceof TimeoutException || root instanceof java.net.SocketTimeoutException
                || root instanceof java.net.http.HttpTimeoutException) {
            return "upstream request timed out";
        }
        if (root instanceof java.net.UnknownHostException) {
            return "upstream host could not be resolved - check network or DNS";
        }
        if (root instanceof java.net.ConnectException) {
            return "could not connect to the upstream";
        }
        if (root instanceof com.fasterxml.jackson.core.JsonProcessingException) {
            return "upstream returned a malformed document";
        }
        if (root instanceof java.io.IOException) {
            return "network error talking to the upstream";
        }

        String message = root.getMessage();
        if (message == null || message.isBlank()) {
            return root.getClass().getSimpleName();
        }
        // Keep it short: this renders in a 340px banner.
        return message.length() > 120 ? message.substring(0, 119) + "…" : message;
    }

    /** The full technical text, for {@code meta.details.errorDetail} and log correlation. */
    public static String detail(Throwable error) {
        return error == null ? null : error.toString();
    }

    private static boolean isRetryExhausted(Throwable error) {
        for (Throwable current = error; current != null; current = current.getCause()) {
            String name = current.getClass().getName();
            if (name.endsWith("RetryExhaustedException")
                    || name.endsWith("Exceptions$RetryExhaustedException")) {
                return true;
            }
            if (current.getCause() == current) {
                break;
            }
        }
        return false;
    }

    private static Throwable rootCause(Throwable error) {
        Throwable current = error;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current;
    }
}
