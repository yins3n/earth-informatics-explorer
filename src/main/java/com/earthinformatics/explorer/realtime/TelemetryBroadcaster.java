package com.earthinformatics.explorer.realtime;

import com.earthinformatics.explorer.dto.TelemetryTick;
import com.earthinformatics.explorer.properties.ExplorerProperties;
import com.earthinformatics.explorer.service.TelemetryService;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * Owns the telemetry tick loop and fans frames out to every attached WebSocket session.
 *
 * <p>Design decisions worth stating explicitly:
 * <ul>
 *   <li><b>Multicast sink, not a per-subscriber loop.</b> One tick is computed and serialised
 *       once, then replayed to every client. A thousand dashboards cost the same upstream budget
 *       as one.</li>
 *   <li><b>Back-pressure over staleness.</b> The sink buffers only the newest ticks; if a client
 *       cannot keep up it is skipped rather than allowed to grow an unbounded queue that would
 *       eventually exhaust the heap.</li>
 *   <li><b>Sequential ticks.</b> {@code concatMap} guarantees a slow frame delays the next tick
 *       instead of overlapping it, so a provider that hangs cannot cause a pile-up of
 *       concurrent upstream calls.</li>
 *   <li><b>Errors are contained.</b> A failing summary degrades that domain to an empty roll-up;
 *       it never kills the loop.</li>
 * </ul>
 */
@Component
@Slf4j
public class TelemetryBroadcaster {

    private final TelemetryService telemetryService;
    private final ExplorerProperties properties;

    /**
     * Bounded multicast buffer, sized from configuration.
     *
     * <p>Depth is a memory/latency trade-off, not a correctness one: a client that falls behind by
     * more than {@code bufferTicks} frames loses the oldest pending tick rather than growing a
     * queue, which is the desired behaviour for a dashboard (stale frames have no value), but the
     * depth belongs in {@code application.yml} next to the tick interval so an operator can
     * reason about worst-case buffered bytes.
     */
    private final Sinks.Many<TelemetryTick> tickSink;
    private final AtomicInteger clients = new AtomicInteger();
    private final AtomicLong ticksEmitted = new AtomicLong();
    private final AtomicLong ticksFailed = new AtomicLong();
    private final AtomicLong ticksDropped = new AtomicLong();

    private volatile Disposable loop;
    private volatile Scheduler scheduler;

    public TelemetryBroadcaster(TelemetryService telemetryService, ExplorerProperties properties) {
        this.telemetryService = telemetryService;
        this.properties = properties;
        this.tickSink = Sinks.many().multicast().onBackpressureBuffer(
                Math.max(1, properties.websocket().bufferTicks()), false);
    }

    @PostConstruct
    void start() {
        Duration interval = properties.websocket().tickInterval();
        // A dedicated scheduler isolates tick work from the shared Reactor pool.
        this.scheduler = Schedulers.newSingle("telemetry-tick");
        this.loop = Flux.interval(Duration.ZERO, interval, scheduler)
                // concatMap: never overlap ticks, however slow a provider becomes.
                .concatMap(ignored -> buildAndEmit(), 1)
                .onErrorResume(error -> {
                    // Full stack: every per-domain failure is already handled further down with
                    // onErrorReturn, so anything surfacing here is a bug in the tick assembly
                    // itself and is worthless without a trace.
                    log.warn("Telemetry tick loop recovered from an unexpected error", error);
                    return Mono.empty();
                })
                .subscribe(
                        ignored -> {
                        },
                        error -> log.error("Telemetry tick loop terminated", error));
        log.info("Telemetry broadcaster started: tick every {} ms, buffer {} frames",
                interval.toMillis(), properties.websocket().bufferTicks());
    }

    @PreDestroy
    void stop() {
        Disposable current = loop;
        if (current != null && !current.isDisposed()) {
            current.dispose();
        }
        Scheduler currentScheduler = scheduler;
        if (currentScheduler != null) {
            currentScheduler.dispose();
        }
        tickSink.emitComplete(Sinks.EmitFailureHandler.FAIL_FAST);
        log.info("Telemetry broadcaster stopped after {} ticks", ticksEmitted.get());
    }

    private Mono<Void> buildAndEmit() {
        return telemetryService.buildTick(clients.get())
                .doOnNext(tick -> {
                    ticksEmitted.incrementAndGet();
                    // tryEmitNext reports the outcome instead of throwing. Emission happens on the
                    // single tick thread, so FAIL_NON_SERIALIZED cannot occur; a FAIL_OVERFLOW
                    // means a subscriber could not keep up and missed this frame, while every
                    // other subscriber still received it.
                    Sinks.EmitResult result = tickSink.tryEmitNext(tick);
                    if (result.isFailure()) {
                        ticksDropped.incrementAndGet();
                        log.debug("Telemetry tick not delivered to at least one subscriber: {}",
                                result);
                    }
                })
                .onErrorResume(error -> {
                    ticksFailed.incrementAndGet();
                    log.warn("Telemetry tick failed: {}", error.toString());
                    return Mono.empty();
                })
                .then();
    }

    /** Tick stream for a single WebSocket session. */
    public Flux<TelemetryTick> ticks() {
        return tickSink.asFlux();
    }

    public void clientConnected() {
        clients.incrementAndGet();
    }

    public void clientDisconnected() {
        clients.updateAndGet(current -> current > 0 ? current - 1 : 0);
    }

    public int connectedClients() {
        return clients.get();
    }

    public long ticksEmitted() {
        return ticksEmitted.get();
    }

    public long ticksFailed() {
        return ticksFailed.get();
    }

    /** Frames skipped because at least one subscriber could not keep up. */
    public long ticksDropped() {
        return ticksDropped.get();
    }

    public long tickIntervalMillis() {
        return properties.websocket().tickInterval().toMillis();
    }
}
