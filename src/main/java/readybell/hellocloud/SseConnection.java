package readybell.hellocloud;

import java.time.Instant;
import java.util.UUID;

import jakarta.ws.rs.sse.SseEventSink;

class SseConnection {

    private final UUID uuid;
    private final SseEventSink sink;
    private Instant lastMessageSent;

    SseConnection(UUID uuid, SseEventSink sink, Instant lastMessageSent) {
        this.uuid = uuid;
        this.sink = sink;
        this.lastMessageSent = lastMessageSent;
    }

    UUID getUuid() {
        return uuid;
    }

    SseEventSink getSink() {
        return sink;
    }

    Instant getLastMessageSent() {
        return lastMessageSent;
    }

    void setLastMessageSent(Instant lastMessageSent) {
        this.lastMessageSent = lastMessageSent;
    }
}
