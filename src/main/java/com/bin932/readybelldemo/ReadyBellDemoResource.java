package com.bin932.readybelldemo;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import io.quarkus.logging.Log;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.ObservesAsync;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.sse.Sse;
import jakarta.ws.rs.sse.SseEventSink;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;

@ApplicationScoped
@Path("/ready-bell-demo")
public class ReadyBellDemoResource {

    @ConfigProperty(name = "ready-bell-demo.bucket")
    String bucket;

    @Inject
    S3Client s3;

    @Inject
    S3Presigner presigner;

    @Inject
    ReadyBellService readyBell;

    @Context
    Sse sse;

    private final ConcurrentHashMap<UUID, DemoInfo> sessions = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<UUID, SseEventSink> sseSinks = new ConcurrentHashMap<>();

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    public Response createDemo(@QueryParam("uuid") String uuidParam, @QueryParam("secret") String secret) {
        UUID uuid;
        try {
            uuid = UUID.fromString(uuidParam);
        } catch (Exception e) {
            return Response.status(Response.Status.BAD_REQUEST).entity("invalid uuid").build();
        }
        if (secret == null || secret.isBlank()) {
            return Response.status(Response.Status.BAD_REQUEST).entity("secret is required").build();
        }

        boolean[] created = {false};
        DemoInfo info;
        try {
            info = sessions.computeIfAbsent(uuid, k -> {
                created[0] = true;
                return buildSession(k, secret);
            });
        } catch (SessionAlreadyClaimedException e) {
            return Response.status(Response.Status.CONFLICT).entity("uuid already used").build();
        }

        if (!created[0] && !info.secret().equals(secret)) {
            return Response.status(Response.Status.FORBIDDEN).build();
        }

        Response.Status status = created[0] ? Response.Status.CREATED : Response.Status.OK;
        return Response.status(status).entity(toView(info)).build();
    }

    @GET
    @Path("/{uuid}/sse")
    @Produces(MediaType.SERVER_SENT_EVENTS)
    public void sse(@PathParam("uuid") String uuidParam, @QueryParam("secret") String secret,
            @Context SseEventSink sink) {
        DemoInfo info = null;
        UUID uuid = null;
        try {
            uuid = UUID.fromString(uuidParam);
            info = sessions.get(uuid);
        } catch (Exception ignored) {
            // falls through to the error response below
        }

        if (info == null || !info.secret().equals(secret)) {
            sink.send(sse.newEventBuilder().name("error").data("invalid uuid or secret").build());
            sink.close();
            return;
        }

        SseEventSink previous = sseSinks.put(uuid, sink);
        if (previous != null && !previous.isClosed()) {
            previous.close();
        }
        for (String event : info.events()) {
            sink.send(sse.newEvent(event));
        }
    }

    @Scheduled(every = "10s")
    void checkSessions() {
        Instant now = Instant.now();
        for (UUID uuid : sessions.keySet()) {
            checkOne(uuid, now);
        }
        for (var entry : List.copyOf(sessions.entrySet())) {
            if (Duration.between(entry.getValue().created(), now).toHours() >= 1) {
                sessions.remove(entry.getKey());
                SseEventSink sink = sseSinks.remove(entry.getKey());
                if (sink != null) {
                    sink.close();
                }
            }
        }
    }

    void onReady(@ObservesAsync ReadyBellEvent e) {
        String msg = "UDP RECV 'Ready " + e.uuid() + "'";
        DemoInfo updated = sessions.computeIfPresent(e.uuid(), (k, info) -> info.withExtraEvent(msg));
        apply(e.uuid(), updated, List.of(msg));
        checkOne(e.uuid(), Instant.now());
    }

    private void checkOne(UUID uuid, Instant now) {
        DemoInfo info = sessions.get(uuid);
        if (info == null || Duration.between(info.lastCheck(), now).toSeconds() <= 30) {
            return;
        }

        DemoStatus pending = info.status();
        List<String> messages = new ArrayList<>();

        if (pending == DemoStatus.NEW) {
            int code = objectStatus(uuid + "/input.txt");
            messages.add("HTTP GET /" + uuid + "/input.txt " + code);
            if (code == 200) {
                pending = DemoStatus.IN_PROGRESS;
                messages.add("status IN_PROGRESS");
            }
        }
        if (pending == DemoStatus.IN_PROGRESS) {
            int code = objectStatus(uuid + "/output.txt");
            messages.add("HTTP GET /" + uuid + "/output.txt " + code);
            if (code == 200) {
                pending = DemoStatus.READY;
                messages.add("status READY");
            }
        }
        if ((pending == DemoStatus.NEW || pending == DemoStatus.IN_PROGRESS)
                && Duration.between(info.created(), now).toMinutes() >= 2) {
            pending = DemoStatus.FAILED;
            messages.add("status FAILED");
        }

        DemoStatus finalStatus = pending;
        DemoInfo updated = sessions.computeIfPresent(uuid, (k, current) -> {
            DemoInfo next = current;
            for (String m : messages) {
                next = next.withExtraEvent(m);
            }
            if (next.status() != finalStatus) {
                next = next.withStatus(finalStatus);
            }
            return next.withLastCheck(now);
        });
        apply(uuid, updated, messages);
    }

    private void apply(UUID uuid, DemoInfo next, List<String> newEvents) {
        if (next == null || newEvents.isEmpty()) {
            return;
        }
        SseEventSink sink = sseSinks.get(uuid);
        if (sink != null && !sink.isClosed()) {
            for (String event : newEvents) {
                sink.send(sse.newEvent(event));
            }
        }
    }

    private DemoInfo buildSession(UUID uuid, String secret) {
        String inputKey = uuid + "/input.txt";
        String outputKey = uuid + "/output.txt";
        if (objectStatus(inputKey) == 200) {
            throw new SessionAlreadyClaimedException(uuid);
        }

        String putUrl = presigner.presignPutObject(b -> b
                .signatureDuration(Duration.ofMinutes(1))
                .putObjectRequest(r -> r.bucket(bucket).key(inputKey)))
                .url().toString();
        String getUrl = presigner.presignGetObject(b -> b
                .signatureDuration(Duration.ofHours(1))
                .getObjectRequest(r -> r.bucket(bucket).key(outputKey)))
                .url().toString();

        try {
            readyBell.sendListen(uuid, 60);
        } catch (Exception e) {
            Log.warn("Failed to send Listen for: %s", e.getMessage(), e);
        }

        Instant now = Instant.now();
        DemoInfo base = new DemoInfo(uuid, secret, putUrl, getUrl, now, now, DemoStatus.NEW, List.of());
        return base.withExtraEvent("Created")
                .withExtraEvent("status NEW")
                .withExtraEvent("UDP SEND 'Listen " + uuid + " 60'");
    }

    private int objectStatus(String key) {
        try {
            s3.headObject(r -> r.bucket(bucket).key(key));
            return 200;
        } catch (NoSuchKeyException e) {
            return 404;
        } catch (S3Exception e) {
            return e.statusCode();
        }
    }

    private DemoInfoView toView(DemoInfo info) {
        return new DemoInfoView(info.uuid(), info.status(), info.putUrl(), info.getUrl(), info.created());
    }

    private record DemoInfoView(UUID uuid, DemoStatus status, String putUrl, String getUrl, Instant created) {
    }

    private static class SessionAlreadyClaimedException extends RuntimeException {
        SessionAlreadyClaimedException(UUID uuid) {
            super("uuid already used: " + uuid);
        }
    }
}
