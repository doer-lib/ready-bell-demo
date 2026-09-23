package readybell.hellocloud;

import java.util.UUID;

record Plan(UUID expiredJobToReclaim, UUID jobToCheck, SseConnection connectionToPing, long timeoutMillis) {

    boolean hasExpiredJobToReclaim() {
        return expiredJobToReclaim != null;
    }

    boolean hasJobToCheck() {
        return jobToCheck != null;
    }

    boolean hasConnectionToPing() {
        return connectionToPing != null;
    }

    static Plan reclaimExpiredJob(UUID uuid) {
        return new Plan(uuid, null, null, 0);
    }

    static Plan checkJob(UUID uuid) {
        return new Plan(null, uuid, null, 0);
    }

    static Plan pingConnection(SseConnection connection) {
        return new Plan(null, null, connection, 0);
    }

    static Plan waitFor(long millis) {
        return new Plan(null, null, null, millis);
    }
}
