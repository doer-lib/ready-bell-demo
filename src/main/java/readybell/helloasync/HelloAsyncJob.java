package readybell.helloasync;

import java.time.Instant;
import java.util.UUID;

import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;
import jakarta.ws.rs.BadRequestException;

public record HelloAsyncJob(
        UUID id,
        Instant created,
        Instant modified,
        HelloAsyncStatus status,
        int version,
        String inputName,
        Integer inputDbgDelaySec,
        String outputGreeting,
        String errorMessage) {

    private static final int MAX_DBG_DELAY_SEC = 600;

    static HelloAsyncJob fromRequest(UUID id, JsonObject body) {
        if (body == null || !body.containsKey("input")) {
            throw new BadRequestException("input is required");
        }
        JsonObject input = body.getJsonObject("input");
        String name = input.getString("name", null);
        if (name == null) {
            throw new BadRequestException("input.name is required");
        }
        Integer dbgDelaySec = input.containsKey("dbg-delay-sec") ? input.getInt("dbg-delay-sec") : null;
        if (dbgDelaySec != null && (dbgDelaySec < 0 || dbgDelaySec > MAX_DBG_DELAY_SEC)) {
            throw new BadRequestException("input.dbg-delay-sec must be between 0 and " + MAX_DBG_DELAY_SEC);
        }
        Instant now = Instant.now();
        return new HelloAsyncJob(id, now, now, HelloAsyncStatus.IN_PROGRESS, 0, name, dbgDelaySec, null, null);
    }

    HelloAsyncJob withReady(String greeting, Instant now) {
        return new HelloAsyncJob(id, created, now, HelloAsyncStatus.READY, version,
                inputName, inputDbgDelaySec, greeting, null);
    }

    HelloAsyncJob withFailed(String errorMessage, Instant now) {
        return new HelloAsyncJob(id, created, now, HelloAsyncStatus.FAILED, version,
                inputName, inputDbgDelaySec, null, errorMessage);
    }

    Instant dueAt() {
        int delay = inputDbgDelaySec != null ? inputDbgDelaySec : 0;
        return created.plusSeconds(delay);
    }

    JsonObject toJson() {
        JsonObjectBuilder output = Json.createObjectBuilder();
        if (outputGreeting != null) {
            output.add("greeting", outputGreeting);
        }
        JsonObjectBuilder root = Json.createObjectBuilder()
                .add("id", id.toString())
                .add("created", created.toString())
                .add("modified", modified.toString())
                .add("status", status.name())
                .add("input", Json.createObjectBuilder().add("name", inputName))
                .add("output", output);
        if (errorMessage != null) {
            root.add("error_message", errorMessage);
        } else {
            root.addNull("error_message");
        }
        return root.build();
    }
}
