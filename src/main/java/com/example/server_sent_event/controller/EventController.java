package com.example.server_sent_event.controller;

import com.example.server_sent_event.model.News;
import com.example.server_sent_event.service.NewsService;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

@RestController
@Slf4j
public class EventController {

    private final NewsService newsService;

    public EventController(NewsService newsService) {
        this.newsService = newsService;
    }

    /**
     * Simple SSE stream example
     */
    @GetMapping(value = "/stream-sse", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<String>> streamSseEvents() {
        return Flux.interval(Duration.ofSeconds(1))
            .take(10)
            .map(sequence -> ServerSentEvent.<String>builder()
                .id(String.valueOf(sequence))
                .event("message")
                .data("SSE WebFlux - " + System.currentTimeMillis())
                .build());
    }

    /**
     * Subscribe to news updates via SSE
     * This endpoint will send all existing news and then stream new news as they arrive
     */
    @GetMapping(value = "/news/subscribe", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<News>> subscribeToNews() {
        return newsService.getNewsStream()
            .map(news -> {
                try {
                    return ServerSentEvent.<News>builder()
                        .id(String.valueOf(news.getId()))
                        .event("news")
                        .data(news)
                        .build();
                } catch (Exception e) {
                    log.error("Error creating SSE event for news: {}", e.getMessage(), e);
                    throw new RuntimeException("Failed to create SSE event", e);
                }
            })
            .concatWith(Flux.never())
            .doOnSubscribe(subscription -> log.info("New subscriber connected to news stream"))
            .doOnCancel(() -> log.info("Subscriber disconnected from news stream"))
            .doOnError(error -> log.error("Error in SSE stream: {}", error.getMessage(), error))
            .onErrorResume(error -> {
                log.error("Fatal error in SSE stream: {}", error.getMessage(), error);
                return Flux.empty();
            });
    }

    /**
     * Add new news (for testing/admin purposes)
     */
    @PostMapping("/news")
    public Mono<News> addNews(@RequestBody NewsRequest request) {
        News news = newsService.addNews(
            request.getTitle(), 
            request.getContent(), 
            request.getCategory(), 
            request.getAuthor()
        );
        return Mono.just(news);
    }

    /**
     * Get all news
     */
    @GetMapping("/news")
    public Mono<java.util.List<News>> getAllNews() {
        return Mono.just(newsService.getAllNews());
    }

    /**
     * Get subscriber count (REST endpoint for backward compatibility)
     */
    @GetMapping("/news/subscribers/count")
    public Mono<Integer> getSubscriberCount() {
        return Mono.just(newsService.getSubscriberCount());
    }

    /**
     * Subscribe to subscriber count updates via SSE
     * Emits the current count immediately, then streams updates whenever the count changes
     */
    @GetMapping(value = "/news/subscribers/count/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<Integer>> subscribeToSubscriberCount() {
        return newsService.getSubscriberCountStream()
            .map(count -> ServerSentEvent.<Integer>builder()
                .id(String.valueOf(count))
                .event("count")
                .data(count)
                .build())
            .doOnSubscribe(subscription -> log.info("New subscriber connected to count stream"))
            .doOnCancel(() -> log.info("Subscriber disconnected from count stream"))
            .doOnError(error -> log.error("Error in count stream: {}", error.getMessage(), error))
            .onErrorResume(error -> {
                log.error("Fatal error in count stream: {}", error.getMessage(), error);
                return Flux.empty();
            });
    }

    /**
     * Remove a specific subscriber (for explicit disconnect)
     * Note: In WebFlux, disconnection is handled automatically by the reactive stream
     */
    @PostMapping("/news/disconnect")
    public Mono<String> disconnect() {
        // In WebFlux, disconnection is handled automatically when the client closes the connection
        return Mono.just("Disconnect request received. Connection will close automatically.");
    }

    /**
     * Request DTO for adding news
     */
    @Getter
    @Setter
    public static class NewsRequest {
        private String title;
        private String content;
        private String category;
        private String author;
    }
}
