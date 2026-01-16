package com.example.server_sent_event.service;

import com.example.server_sent_event.model.News;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Service
@Slf4j
public class NewsService {

    private final List<News> newsList = new ArrayList<>();
    private final AtomicLong idGenerator = new AtomicLong(1);
    private final AtomicInteger subscriberCount = new AtomicInteger(0);

    // Use Sinks.Many for multicasting news to all subscribers
    private final Sinks.Many<News> newsSink = Sinks.many().multicast().onBackpressureBuffer();

    // Sink for broadcasting subscriber count updates
    private final Sinks.Many<Integer> countSink = Sinks.many().multicast().onBackpressureBuffer();

    // Initialize with default news items
    {
        newsList.add(new News(idGenerator.getAndIncrement(),
                "Welcome to News Broadcasting",
                "This is a Server-Sent Events (SSE) based news broadcasting system. Subscribe to receive real-time news updates!",
                LocalDateTime.now().minusHours(2),
                "Technology",
                "System Admin"));

        newsList.add(new News(idGenerator.getAndIncrement(),
                "Spring Boot WebFlux SSE Implementation",
                "This application demonstrates real-time news broadcasting using Spring Boot WebFlux and Server-Sent Events. New subscribers will receive all existing news immediately upon connection.",
                LocalDateTime.now().minusHours(1),
                "Technology",
                "Development Team"));

        log.info("Initialized with {} default news items", newsList.size());
    }

    /**
     * Initialize heartbeat mechanism to detect dead connections
     */
    @PostConstruct
    public void init() {
        log.info("NewsService initialized with WebFlux reactive streams");
    }

    /**
     * Cleanup on shutdown
     */
    @PreDestroy
    public void cleanup() {
        newsSink.tryEmitComplete();
        countSink.tryEmitComplete();
        log.info("NewsService cleanup completed");
    }

    /**
     * Get a Flux of news that emits all existing news first, then streams new news
     * Each subscriber gets a fresh stream with all existing news, then new news
     */
    public Flux<News> getNewsStream() {

        Flux<News> existingNewsFlux = Flux.fromIterable(newsList);
        Flux<News> newNewsFlux = newsSink.asFlux();

        return existingNewsFlux.concatWith(newNewsFlux)
                .concatWith(Flux.never())
                .doOnSubscribe(subscription -> {
                    int count = subscriberCount.incrementAndGet();
                    log.info("New subscriber connected. Total subscribers: {}", count);
                    countSink.tryEmitNext(count);
                })
                .doOnCancel(() -> {
                    int count = subscriberCount.decrementAndGet();
                    log.info("Subscriber cancelled. Remaining subscribers: {}", count);
                    countSink.tryEmitNext(count);
                })
                .doOnError(error -> {
                    log.error("Error in news stream: {}", error.getMessage());
                })
                .doOnTerminate(() -> {
                    int count = subscriberCount.decrementAndGet();
                    log.info("Subscriber stream terminated. Remaining subscribers: {}", count);
                    countSink.tryEmitNext(count);
                });
    }

    /**
     * Add new news and notify all subscribers
     */
    public News addNews(String title, String content, String category, String author) {
        News news = new News(idGenerator.getAndIncrement(), title, content, LocalDateTime.now(), category, author);
        newsList.add(news);

        log.info("New news added: {}", news.getTitle());

        // Emit the new news to all subscribers via the sink
        Sinks.EmitResult result = newsSink.tryEmitNext(news);
        if (result.isFailure()) {
            log.warn("Failed to emit news to subscribers: {}", result);
        }

        return news;
    }

    /**
     * Get all news
     */
    public List<News> getAllNews() {
        return new ArrayList<>(newsList);
    }

    /**
     * Get number of active subscribers
     * Returns the current count of active subscribers to the news stream
     */
    public int getSubscriberCount() {
        return subscriberCount.get();
    }

    /**
     * Get a Flux of subscriber count updates
     * Emits the current count immediately, then streams updates whenever the count changes
     * Supports multiple subscribers - all subscribers receive the same count updates via multicast sink
     */
    public Flux<Integer> getSubscriberCountStream() {
        // Get the multicast flux from the sink (already supports multiple subscribers)
        Flux<Integer> countUpdates = countSink.asFlux()
                .distinctUntilChanged(); // Only emit when count actually changes

        // For each new subscriber, emit the current count first, then stream updates
        // Use defer to get fresh current count for each subscriber, then concat with shared updates
        return Flux.defer(() -> Flux.just(subscriberCount.get()))
                .concatWith(countUpdates)
                .concatWith(Flux.never()) // Keep stream open indefinitely - never completes
                .doOnSubscribe(subscription -> log.debug("New subscriber connected to count stream"))
                .doOnCancel(() -> log.debug("Subscriber disconnected from count stream"));
    }
}
