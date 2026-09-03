package com.example.as400.infrastructure.runtime;

import com.example.as400.application.port.in.ProcessMessageUseCase;
import com.example.as400.application.port.out.DataQueueMessageSource;
import com.example.as400.domain.DataQueueMessage;
import com.example.as400.infrastructure.config.ConsumerProperties;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

@Component
public final class DataQueueConsumerLifecycle implements SmartLifecycle {
    private static final Logger log = LoggerFactory.getLogger(DataQueueConsumerLifecycle.class);

    private final DataQueueMessageSource source;
    private final ProcessMessageUseCase processor;
    private final ConsumerProperties properties;
    private final BlockingQueue<DataQueueMessage> buffer;
    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicBoolean connected = new AtomicBoolean();
    private final AtomicInteger inFlight = new AtomicInteger();
    private volatile Thread readerThread;
    private volatile ExecutorService dispatcherExecutor;
    private volatile Scheduler dispatcherScheduler;
    private volatile Disposable processingSubscription;

    public DataQueueConsumerLifecycle(
            DataQueueMessageSource source,
            ProcessMessageUseCase processor,
            ConsumerProperties properties,
            MeterRegistry registry) {
        this.source = source;
        this.processor = processor;
        this.properties = properties;
        this.buffer = new ArrayBlockingQueue<>(properties.bufferCapacity());
        Gauge.builder("as400.buffer.size", buffer, BlockingQueue::size).register(registry);
        Gauge.builder("as400.processing.inflight", inFlight, AtomicInteger::get).register(registry);
        Gauge.builder("as400.connection.up", connected, state -> state.get() ? 1 : 0).register(registry);
    }

    @Override
    public synchronized void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        startProcessingPipeline();
        readerThread = Thread.ofVirtual().name("as400-dataqueue-reader").start(this::readLoop);
        log.info("AS400 consumer started bufferCapacity={} concurrency={}",
                properties.bufferCapacity(), properties.concurrency());
    }

    private void startProcessingPipeline() {
        dispatcherExecutor = Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("as400-buffer-dispatcher", 0).factory());
        dispatcherScheduler = Schedulers.fromExecutorService(dispatcherExecutor);
        processingSubscription = Flux.<DataQueueMessage>generate(this::emitNextBufferedMessage)
                .subscribeOn(dispatcherScheduler)
                .flatMap(message -> Mono.defer(() -> {
                            inFlight.incrementAndGet();
                            return processor.process(message);
                        })
                        .doOnError(error -> log.error(
                                "Message processing failed id={} correlationId={}",
                                message.id(), message.correlationId(), error))
                        .onErrorComplete()
                        .doFinally(signal -> inFlight.decrementAndGet()), properties.concurrency(), 1)
                .subscribe();
    }

    private void emitNextBufferedMessage(reactor.core.publisher.SynchronousSink<DataQueueMessage> sink) {
        try {
            while (running.get() || !buffer.isEmpty()) {
                DataQueueMessage message = buffer.poll(100, TimeUnit.MILLISECONDS);
                if (message != null) {
                    sink.next(message);
                    return;
                }
            }
            sink.complete();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            sink.complete();
        }
    }

    private void readLoop() {
        var config = properties.backoff();
        var backoff = new ExponentialBackoff(config.initial(), config.maximum(), config.multiplier(), config.jitter());
        while (running.get()) {
            try {
                if (!source.isConnected()) {
                    source.connect();
                    connected.set(true);
                    backoff.reset();
                    log.info("Connected to Data Queue");
                }
                source.readBlocking(properties.readTimeout()).ifPresent(this::putWithBackpressure);
            } catch (Exception error) {
                connected.set(false);
                source.close();
                if (running.get()) {
                    Duration delay = backoff.nextDelay();
                    log.warn("Data Queue read failed; reconnecting in {} ms: {}", delay.toMillis(), error.toString());
                    sleep(delay);
                }
            }
        }
    }

    private void putWithBackpressure(DataQueueMessage message) {
        try {
            buffer.put(message);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public synchronized void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        connected.set(false);
        source.close();
        if (readerThread != null) {
            readerThread.interrupt();
        }
        waitForDrain(properties.shutdownTimeout());
        if (processingSubscription != null) {
            processingSubscription.dispose();
        }
        if (dispatcherScheduler != null) {
            dispatcherScheduler.dispose();
        }
        if (dispatcherExecutor != null) {
            dispatcherExecutor.shutdownNow();
        }
        log.info("AS400 consumer stopped remainingBuffer={} inFlight={}", buffer.size(), inFlight.get());
    }

    private void waitForDrain(Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while ((!buffer.isEmpty() || inFlight.get() > 0) && System.nanoTime() < deadline) {
            sleep(Duration.ofMillis(10));
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    public boolean isConnected() {
        return connected.get();
    }

    public int bufferSize() {
        return buffer.size();
    }

    public int inFlightCount() {
        return inFlight.get();
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 100;
    }
}
