package readybell.hellocloud;

import java.time.Instant;
import java.util.UUID;

import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;

public class HelloCloudJob {

    private final UUID uuid;
    private final String inputPutUrl;
    private final String outputGetUrl;
    private HelloCloudStatus status;
    private final Instant created;
    private Instant modified;
    private Instant plannedCheck;
    private Instant finished;

    HelloCloudJob(UUID uuid, String inputPutUrl, String outputGetUrl, Instant created) {
        this.uuid = uuid;
        this.inputPutUrl = inputPutUrl;
        this.outputGetUrl = outputGetUrl;
        this.created = created;
        this.modified = created;
        this.status = HelloCloudStatus.NEW;
        this.plannedCheck = created;
    }

    UUID getUuid() {
        return uuid;
    }

    HelloCloudStatus getStatus() {
        return status;
    }

    void setStatus(HelloCloudStatus status) {
        this.status = status;
    }

    Instant getCreated() {
        return created;
    }

    Instant getModified() {
        return modified;
    }

    void setModified(Instant modified) {
        this.modified = modified;
    }

    Instant getPlannedCheck() {
        return plannedCheck;
    }

    void setPlannedCheck(Instant plannedCheck) {
        this.plannedCheck = plannedCheck;
    }

    Instant getFinished() {
        return finished;
    }

    void setFinished(Instant finished) {
        this.finished = finished;
    }

    JsonObject toJson() {
        JsonObjectBuilder root = Json.createObjectBuilder()
                .add("uuid", uuid.toString())
                .add("status", status.name())
                .add("created", created.toString())
                .add("modified", modified.toString())
                .add("inputPutUrl", inputPutUrl)
                .add("outputGetUrl", outputGetUrl);
        return root.build();
    }
}
