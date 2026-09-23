package readybell.hellocloud;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;
import jakarta.ws.rs.sse.OutboundSseEvent;
import jakarta.ws.rs.sse.Sse;
import jakarta.ws.rs.sse.SseEventSink;
import readybell.ReadyBellEvent;
import readybell.ReadyBellService;
import software.amazon.awssdk.services.s3.model.S3Exception;

import static readybell.hellocloud.HelloCloudStatus.*;

@ApplicationScoped
@Path("/hello-cloud")
public class HelloCloudResource {

    private static final Duration POLL_INTERVAL = Duration.ofSeconds(10); // how often check s3
    private static final Duration JOB_TIMEOUT = Duration.ofMinutes(2); // when to set FAILED
    private static final Duration JOB_TTL = Duration.ofHours(2); // when to delete job from s3
    private static final Duration PING_INTERVAL = Duration.ofMinutes(1); // how often send SSE ping
    private static final Duration MAX_IDLE_WAIT = Duration.ofMinutes(1); // longest the background loop will sleep

    @Inject
    HelloCloudS3Client s3Client;

    @Inject
    ReadyBellService readyBell;

    @Inject
    Executor executor;

    @Context
    Sse sse;

    @Context
    UriInfo uriInfo;

    private Map<UUID, HelloCloudJob> jobs;
    private final LinkedList<SseConnection> sseConnections = new LinkedList<>();

    private volatile boolean running = true;
    private volatile Thread backgroundThread;

    // --- Application lifecycle ---

    public void onStart(@Observes StartupEvent ev) {
        setJobs(null);
        executor.execute(this::loadInitialJobs);
        executor.execute(this::backgroundLoop);
    }

    public void onStop(@Observes ShutdownEvent ev) {
        running = false;
        Thread thread = backgroundThread;
        if (thread != null) {
            thread.interrupt();
        }
    }

    public void onReady(@Observes ReadyBellEvent e) {
        Log.debugf("Observer %s", e);
        broadcastSseMessages(e.uuid(), "ready", "UDP received: Ready " + e.uuid());
        initiateJobRecheck(e.uuid());
    }

    // --- REST endpoints ---

    @POST
    @Path("/{uuid}")
    @Produces(MediaType.APPLICATION_JSON)
    public synchronized Response restPostJob(@PathParam("uuid") UUID uuid) {
        if (jobs == null) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).build();
        }

        HelloCloudJob job = jobs.get(uuid);
        if (job == null) {
            job = buildJob(uuid);
            jobs.put(uuid, job);
            notifyAll();
        }

        URI location = uriInfo.getBaseUriBuilder()
                .path(HelloCloudResource.class)
                .path(HelloCloudResource.class, "restGetJob")
                .build(uuid);
        return Response.created(location).entity(job.toJson()).build();
    }

    @GET
    @Path("/{uuid}")
    @Produces(MediaType.APPLICATION_JSON)
    public synchronized Response restGetJob(@PathParam("uuid") UUID uuid) {
        if (jobs == null) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).build();
        }

        HelloCloudJob job = jobs.get(uuid);
        if (job == null) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
        return Response.ok(job.toJson()).build();
    }

    @GET
    @Path("/{uuid}/sse")
    @Produces(MediaType.SERVER_SENT_EVENTS)
    public synchronized void restGetJobSse(@PathParam("uuid") UUID uuid, @Context SseEventSink sink) {
        if (jobs == null) {
            sink.send(sse.newEventBuilder().name("error").data("service starting, try again").build());
            sink.close();
            return;
        }

        HelloCloudJob job = jobs.get(uuid);
        if (job == null) {
            sink.send(sse.newEventBuilder().name("error").data("unknown uuid").build());
            sink.close();
            return;
        }

        sink.send(sse.newEventBuilder().name("status").data(job.getStatus().name()).build());

        if (job.getFinished() != null) {
            sink.send(sse.newEventBuilder().name("end").data("end of stream").build());
            sink.close();
            return;
        }

        sseConnections.add(new SseConnection(uuid, sink, Instant.now()));
    }

    // --- Background scheduling loop ---

    private void backgroundLoop() {
        backgroundThread = Thread.currentThread();
        try {
            while (running) {
                Plan plan = planNextAction();
                if (plan.hasExpiredJobToReclaim()) {
                    reclaimExpiredJob(plan.expiredJobToReclaim());
                } else if (plan.hasJobToCheck()) {
                    checkJobStatus(plan.jobToCheck());
                } else if (plan.hasConnectionToPing()) {
                    pingConnection(plan.connectionToPing());
                } else if (plan.timeoutMillis() > 0) {
                    synchronized (this) {
                        wait(plan.timeoutMillis());
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private synchronized Plan planNextAction() {
        Instant now = Instant.now();
        Instant earliest = now.plus(MAX_IDLE_WAIT);

        if (jobs != null) {
            for (HelloCloudJob job : jobs.values()) {
                Instant ttlDeadline = job.getCreated().plus(JOB_TTL);
                if (ttlDeadline.isBefore(earliest)) {
                    earliest = ttlDeadline;
                }
                if (!ttlDeadline.isAfter(now)) {
                    return Plan.reclaimExpiredJob(job.getUuid());
                }
            }
            for (HelloCloudJob job : jobs.values()) {
                if (job.getStatus().isTerminal()) {
                    continue;
                }
                if (job.getPlannedCheck().isBefore(earliest)) {
                    earliest = job.getPlannedCheck();
                }
                if (!job.getPlannedCheck().isAfter(now)) {
                    return Plan.checkJob(job.getUuid());
                }
            }
        }

        for (SseConnection connection : sseConnections) {
            Instant pingDeadline = connection.getLastMessageSent().plus(PING_INTERVAL);
            if (pingDeadline.isBefore(earliest)) {
                earliest = pingDeadline;
            }
            if (!pingDeadline.isAfter(now)) {
                return Plan.pingConnection(connection);
            }
        }

        long millis = Duration.between(now, earliest).toMillis();
        return Plan.waitFor(Math.max(millis, 0));
    }

    private void checkJobStatus(UUID uuid) {
        Instant now = Instant.now();
        Instant created;
        HelloCloudStatus currentStatus;
        synchronized (this) {
            var job = jobs != null ? jobs.get(uuid) : null;
            if (job == null) {
                return;
            }
            created = job.getCreated();
            currentStatus = job.getStatus();
            if (currentStatus.isTerminal()) {
                return;
            }
            var nextCheck = now.plus(POLL_INTERVAL);
            var jobDeadline = created.plus(JOB_TIMEOUT);
            job.setPlannedCheck(nextCheck.isAfter(jobDeadline) ? jobDeadline : nextCheck);
            job.setModified(now);
        }

        readyBell.sendListen(uuid, 60);

        if (currentStatus == NEW) {
            int inputFileHttpStatus = s3Client.objectStatus(uuid + "/input.txt");
            broadcastSseMessages(uuid, "check_s3", "HEAD input.txt " + inputFileHttpStatus);
            if (inputFileHttpStatus == 200) {
                currentStatus = IN_PROGRESS;
                updateJobStatus(uuid, currentStatus);
            }
        }

        int outputFileHttpStatus = s3Client.objectStatus(uuid + "/output.txt");
        broadcastSseMessages(uuid, "check_s3", "HEAD output.txt " + outputFileHttpStatus);
        if (outputFileHttpStatus == 200) {
            currentStatus = READY;
            updateJobStatus(uuid, currentStatus);
        }

        if (!currentStatus.isTerminal()) {
            if (now.isAfter(created.plus(JOB_TIMEOUT))) {
                broadcastSseMessages(uuid, "timeout", "Job timed out");
                currentStatus = FAILED;
                updateJobStatus(uuid, currentStatus);
            }
        }
    }

    private synchronized void initiateJobRecheck(UUID uuid) {
        HelloCloudJob job = (jobs != null ? jobs.get(uuid) : null);
        if (job == null) {
            return;
        }
        if (!job.getStatus().isTerminal()) {
            job.setPlannedCheck(Instant.now());
            notifyAll();
        }
    }

    private synchronized void updateJobStatus(UUID uuid, HelloCloudStatus status) {
        HelloCloudJob job = (jobs != null ? jobs.get(uuid) : null);
        if (job == null) {
            return;
        }
        Instant now = Instant.now();
        job.setStatus(status);
        job.setModified(now);
        broadcastSseMessages(uuid, "status", status.name());
        if (status.isTerminal()) {
            job.setFinished(now);
            broadcastSseMessages(uuid, "end", "end of stream");
            closeAndRemoveSinksFor(uuid);
        }
    }

    // --- SSE messaging ---

    private synchronized void broadcastSseMessages(UUID uuid, String eventName, String data) {
        if (sse == null || sseConnections.isEmpty()) {
            return;
        }
        OutboundSseEvent event = sse.newEventBuilder().name(eventName).data(data).build();
        for (SseConnection connection : List.copyOf(sseConnections)) {
            if (connection.getUuid().equals(uuid)) {
                sendTo(connection, event);
            }
        }
    }

    private void pingConnection(SseConnection connection) {
        sendTo(connection, sse.newEventBuilder().name("ping").data("ping").build());
    }

    private synchronized void sendTo(SseConnection connection, OutboundSseEvent event) {
        connection.getSink().send(event).exceptionally(e -> {
            Log.debugf(e, "Cant send SSE");
            closeAndRemove(connection);
            return null;
        });
        connection.setLastMessageSent(Instant.now());
    }

    private synchronized void closeAndRemoveSinksFor(UUID uuid) {
        for (SseConnection connection : List.copyOf(sseConnections)) {
            if (connection.getUuid().equals(uuid)) {
                closeAndRemove(connection);
            }
        }
    }

    private synchronized void closeAndRemove(SseConnection connection) {
        if (!connection.getSink().isClosed()) {
            connection.getSink().close();
        }
        sseConnections.remove(connection);
    }

    // --- Job lifecycle & S3 persistence ---

    private void loadInitialJobs() {
        var loaded = new ConcurrentHashMap<>(restoreFromS3());
        setJobs(loaded);
    }

    private synchronized void setJobs(Map<UUID, HelloCloudJob> loaded) {
        this.jobs = loaded;
        notifyAll();
    }

    private Map<UUID, HelloCloudJob> restoreFromS3() {
        List<JobFolderInfo> s3folders;
        try {
            s3folders = s3Client.listJobFolders();
        } catch (S3Exception e) {
            s3folders = List.of();
            Log.warnf(e, "Failed to list job folders on s3: %s", e.getMessage());
        }
        Map<UUID, HelloCloudJob> restored = s3folders.stream()
                .map(this::buildRestoredJob)
                .collect(Collectors.toMap(HelloCloudJob::getUuid, Function.identity()));
        restored.values().forEach(job -> Log.debugf("Restored job %s with status %s", job.getUuid(), job.getStatus()));
        return restored;
    }

    private HelloCloudJob buildRestoredJob(JobFolderInfo folder) {
        var status = folder.outputTxtCreated() != null ? READY : IN_PROGRESS;
        var created = Stream.of(folder.inputTxtCreated(), folder.outputTxtCreated())
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(Instant.now());
        var inputKey = folder.uuid() + "/input.txt";
        var outputKey = folder.uuid() + "/output.txt";
        var presignedPutUrl = s3Client.presignPut(inputKey);
        var presignedGetUrl = s3Client.presignGet(outputKey);
        var job = new HelloCloudJob(folder.uuid(), presignedPutUrl, presignedGetUrl, created);
        job.setStatus(status);
        if (status.isTerminal()) {
            job.setFinished(Instant.now());
        }
        return job;
    }

    private HelloCloudJob buildJob(UUID uuid) {
        String inputKey = uuid + "/input.txt";
        String outputKey = uuid + "/output.txt";
        return new HelloCloudJob(uuid, s3Client.presignPut(inputKey), s3Client.presignGet(outputKey), Instant.now());
    }

    private void reclaimExpiredJob(UUID uuid) {
        removeJobFromMap(uuid);
        closeAndRemoveSinksFor(uuid);
        try {
            s3Client.deleteObjectsWithPrefix(uuid + "/");
        } catch (S3Exception e) {
            Log.warnf(e, "Failed to delete S3 objects under %s: %s", uuid, e.getMessage());
        }
    }

    private synchronized void removeJobFromMap(UUID uuid) {
        if (jobs != null) {
            jobs.remove(uuid);
        }
    }
}
