package com.earthinformatics.explorer.error;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.support.WebExchangeBindException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;
import com.earthinformatics.explorer.util.JsonSupport;

/**
 * Uniform error contract.
 *
 * <p>Clients (and the globe's own error banner) get a predictable document shape for every
 * failure class:
 * <pre>{@code
 * {
 *   "error": {
 *     "status": 400,
 *     "code": "INVALID_REQUEST",
 *     "message": "gridStep must be between 5 and 90",
 *     "path": "/api/v1/atmospherics/air-quality",
 *     "timestamp": "2026-01-01T00:00:00Z"
 *   }
 * }
 * }</pre>
 *
 * <p>{@code @Order(-1)} keeps this advice ahead of Boot's default handler while still letting
 * Spring's own binding/validation advice take precedence for constraint violations, which
 * produce far better field-level messages than we could.
 */
@RestControllerAdvice
@Order(-1)
@Slf4j
public class GlobalErrorHandler {

    private final ObjectMapper objectMapper;

    public GlobalErrorHandler(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @ExceptionHandler(ResponseStatusException.class)
    public Mono<Void> handleStatus(ResponseStatusException ex, ServerWebExchange exchange) {
        HttpStatus status = HttpStatus.resolve(ex.getStatusCode().value());
        HttpStatus resolved = status != null ? status : HttpStatus.INTERNAL_SERVER_ERROR;
        return write(exchange, resolved, ex.getReason() != null ? ex.getReason()
                : resolved.getReasonPhrase());
    }

    @ExceptionHandler(WebExchangeBindException.class)
    public Mono<Void> handleBinding(WebExchangeBindException ex, ServerWebExchange exchange) {
        String detail = ex.getFieldErrors().stream()
                .map(error -> error.getField() + ": " + error.getDefaultMessage())
                .reduce((left, right) -> left + "; " + right)
                .orElse("request validation failed");
        return write(exchange, HttpStatus.BAD_REQUEST, detail);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public Mono<Void> handleIllegalArgument(IllegalArgumentException ex, ServerWebExchange exchange) {
        return write(exchange, HttpStatus.BAD_REQUEST, ex.getMessage());
    }

    @ExceptionHandler(Throwable.class)
    public Mono<Void> handleUnexpected(Throwable ex, ServerWebExchange exchange) {
        log.error("Unhandled error on {} {}", exchange.getRequest().getMethod(),
                exchange.getRequest().getPath(), ex);
        return write(exchange, HttpStatus.INTERNAL_SERVER_ERROR,
                "unexpected server error: " + ex.getClass().getSimpleName());
    }

    private Mono<Void> write(ServerWebExchange exchange, HttpStatus status, String message) {
        if (exchange.getResponse().isCommitted()) {
            return Mono.empty();
        }
        exchange.getResponse().setStatusCode(status);
        exchange.getResponse().getHeaders().setContentType(MediaType.APPLICATION_JSON);

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", status.value());
        body.put("code", code(status));
        body.put("message", message);
        body.put("path", exchange.getRequest().getPath().value());
        body.put("timestamp", Instant.now().toString());
        if (status.is5xxServerError()) {
            body.put("hint", "check the explorer.upstreams.* configuration and network egress");
        }

        Map<String, Object> envelope = Map.of("error", body);
        byte[] bytes;
        try {
            bytes = objectMapper.writeValueAsBytes(envelope);
        } catch (Exception serialisationFailure) {
            // Last-resort: hand back a literal so the client at least sees an error.
            bytes = JsonSupport.mapper().createObjectNode()
                    .put("error", "serialisation failure").toString().getBytes();
        }
        return exchange.getResponse().writeWith(
                Mono.just(exchange.getResponse().bufferFactory().wrap(bytes)));
    }

    private static String code(HttpStatus status) {
        if (status == HttpStatus.BAD_REQUEST) {
            return "INVALID_REQUEST";
        }
        if (status == HttpStatus.NOT_FOUND) {
            return "NOT_FOUND";
        }
        if (status == HttpStatus.TOO_MANY_REQUESTS) {
            return "RATE_LIMITED";
        }
        if (status.is5xxServerError()) {
            return "INTERNAL_ERROR";
        }
        return status.name();
    }

    /** Documented for OpenAPI consumers; not used at runtime. */
    public static URI schemaLocation() {
        return URI.create("https://explorer.earthinformatics.dev/errors");
    }
}
