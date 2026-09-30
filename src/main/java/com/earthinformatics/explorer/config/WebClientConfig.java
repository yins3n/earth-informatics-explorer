package com.earthinformatics.explorer.config;

import com.earthinformatics.explorer.properties.ExplorerProperties;
import com.earthinformatics.explorer.util.HostRateLimiterRegistry;
import com.earthinformatics.explorer.util.TokenBucketRateLimiter;
import io.netty.channel.ChannelOption;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.ClientRequest;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;

/**
 * Outbound HTTP plumbing.
 *
 * <p>A single pooled {@link WebClient} is shared by every provider client. One pool means one
 * TLS handshake per host instead of per-call, and one place where timeouts, retries and
 * rate limiting are enforced - a per-client {@code WebClient} is the classic way to
 * accidentally lose all of them.
 *
 * <p>The {@code rateLimit} filter is the outermost safety net: it consults a per-host token
 * bucket and, when the bucket is empty, defers the exchange with {@link Mono#delay} rather than
 * blocking a Reactor Netty event-loop thread.
 */
@Configuration
@Slf4j
public class WebClientConfig {

    /** Identifies us to providers; several publish rate limits keyed on user agent + IP. */
    public static final String USER_AGENT =
            "EarthInformaticsExplorer/1.0 (+https://github.com/earth-informatics/explorer)";

    private static final String REQUEST_ID_HEADER = "X-Request-Id";

    @Bean
    public WebClient upstreamWebClient(ExplorerProperties properties,
            HostRateLimiterRegistry rateLimiters) {
        ExplorerProperties.Upstreams.Resilience resilience = properties.upstreams().resilience();

        // Bounded pool: prevents a burst of grid chunks from opening unbounded sockets, and
        // makes pool saturation back-pressure into the reactive chain instead of the file
        // descriptor table.
        ConnectionProvider connectionProvider = ConnectionProvider.builder("explorer-upstream")
                .maxConnections(64)
                .pendingAcquireMaxCount(-1)
                .pendingAcquireTimeout(resilience.connectTimeout())
                .metrics(true)
                .build();

        HttpClient httpClient = HttpClient.create(connectionProvider)
                .followRedirect(true)
                .compress(true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS,
                        (int) resilience.connectTimeout().toMillis())
                .responseTimeout(resilience.responseTimeout())
                .keepAlive(true);

        return WebClient.builder()
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .defaultHeader(HttpHeaders.USER_AGENT, USER_AGENT)
                .defaultHeader(HttpHeaders.ACCEPT, "application/json, text/plain, */*")
                .exchangeStrategies(ExchangeStrategies.builder()
                        // Wildfire and state-vector documents are multi-megabyte; the default
                        // 256 KB in-memory limit would blow up with DataBufferLimitException.
                        .codecs(configurer -> configurer.defaultCodecs()
                                .maxInMemorySize(resilience.maxResponseBytes()))
                        .build())
                .filter(rateLimitFilter(rateLimiters))
                .filter(requestIdFilter())
                .filter(loggingFilter())
                .build();
    }

    /**
     * Per-host token bucket. Runs before the connection is even opened, which means throttled
     * requests cost us nothing but a timer.
     */
    private ExchangeFilterFunction rateLimitFilter(HostRateLimiterRegistry registry) {
        return (request, next) -> {
            String host = request.url().getHost();
            TokenBucketRateLimiter limiter = registry.forHost(host);
            Duration delay = limiter.tryAcquire();
            if (delay.isZero()) {
                return next.exchange(request);
            }
            return Mono.delay(delay).then(Mono.defer(() -> next.exchange(request)));
        };
    }

    /** Correlation id used to tie a downstream trace line to an upstream request. */
    private ExchangeFilterFunction requestIdFilter() {
        AtomicLong counter = new AtomicLong();
        return (request, next) -> {
            String id = "up-" + counter.incrementAndGet();
            // ClientRequest is immutable; rebuild it through the from(...) builder.
            request = ClientRequest.from(request)
                    .header(REQUEST_ID_HEADER, id)
                    .build();
            return next.exchange(request)
                    .doOnError(error -> log.debug("[{}] upstream call failed: {}", id,
                            error.toString()));
        };
    }

    /** DEBUG-level timing log - never INFO, otherwise the poll loop would drown the console. */
    private ExchangeFilterFunction loggingFilter() {
        return (request, next) -> {
            if (!log.isDebugEnabled()) {
                return next.exchange(request);
            }
            long start = System.nanoTime();
            URI uri = request.url();
            return next.exchange(request).doFinally(signal -> log.debug("{} {} took {} ms",
                    request.method(), uri, (System.nanoTime() - start) / 1_000_000));
        };
    }

    /** Named qualifier for the shared client so tests can supply a stub. */
    public static final String QUALIFIER = "upstreamWebClient";

    /** Convenience used by the health indicator. */
    public static Duration defaultTimeout() {
        return Duration.ofSeconds(20);
    }
}
