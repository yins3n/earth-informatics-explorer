package com.earthinformatics.explorer.util;

/**
 * The upstream answered successfully but with a body this client cannot use.
 *
 * <p>This is a distinct fault class from a 4xx or a connection reset, and it is worth naming.
 * Several of the public APIs this project depends on are fronted by load balancers whose nodes
 * disagree about content negotiation: NASA EONET in particular intermittently answers
 * {@code application/rss+xml} with HTTP 200 to a client that asked for JSON, and the same
 * request against the next node returns the expected document.
 *
 * <p>So an unusable representation is <em>transient</em> even though the response was a 200, and
 * {@link UpstreamRetry#isTransient(Throwable)} treats it as such. Modelling it as a plain
 * {@link IllegalStateException} instead made the failure look like a bug in our parser and, worse,
 * made it unretryable - the volcano layer degraded for the whole cache window over a response that
 * the next request would have parsed fine.
 */
public class UnusableRepresentationException extends RuntimeException {

    public UnusableRepresentationException(String message) {
        super(message);
    }

    public UnusableRepresentationException(String message, Throwable cause) {
        super(message, cause);
    }
}
