package com.bin932.readybelldemo;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

public record DemoInfo(
        UUID uuid,
        String secret,
        String putUrl,
        String getUrl,
        Instant created,
        Instant lastCheck,
        DemoStatus status,
        List<String> events) {

    DemoInfo withLastCheck(Instant lastCheck) {
        return new DemoInfo(uuid, secret, putUrl, getUrl, created, lastCheck, status, events);
    }

    DemoInfo withStatus(DemoStatus status) {
        return new DemoInfo(uuid, secret, putUrl, getUrl, created, lastCheck, status, events);
    }

    DemoInfo withExtraEvent(String message) {
        List<String> updated = Stream.concat(events.stream(), Stream.of(Instant.now() + " " + message)).toList();
        return new DemoInfo(uuid, secret, putUrl, getUrl, created, lastCheck, status, updated);
    }
}
