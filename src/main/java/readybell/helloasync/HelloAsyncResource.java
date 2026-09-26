package readybell.helloasync;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;
import readybell.ReadyBellService;

@ApplicationScoped
@Path("/hello-async")
public class HelloAsyncResource {

    @Inject
    HelloAsyncDao dao;

    @Inject
    ReadyBellService readyBell;

    @Context
    UriInfo uriInfo;

    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    // --- Application lifecycle ---

    public void onStart(@Observes StartupEvent ev) {
        dao.findAllInProgress()
                .forEach(job -> executor.submit(() -> processJob(job)));
    }

    public void onStop(@Observes ShutdownEvent ev) {
        executor.shutdownNow();
    }

    // --- REST endpoints ---

    @POST
    @Path("/{uuid}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response restPostJob(@PathParam("uuid") UUID uuid, JsonObject body) {
        HelloAsyncJob candidate = HelloAsyncJob.fromRequest(uuid, body);

        URI location = uriInfo.getBaseUriBuilder()
                .path(HelloAsyncResource.class)
                .path(HelloAsyncResource.class, "restGetJob")
                .build(uuid);

        Optional<HelloAsyncJob> inserted = dao.insertIfAbsent(candidate);
        if (inserted.isPresent()) {
            executor.submit(() -> processJob(inserted.get()));
            return Response.created(location).entity(inserted.get().toJson()).build();
        }

        return dao.findById(uuid)
                .filter(existing -> Objects.equals(existing.inputName(), candidate.inputName())
                        && Objects.equals(existing.inputDbgDelaySec(), candidate.inputDbgDelaySec()))
                .map(existing -> Response.created(location).entity(existing.toJson()).build())
                .orElseGet(() -> Response.status(Response.Status.CONFLICT)
                        .entity(Json.createObjectBuilder().add("error", "uuid already used with different input").build())
                        .build());
    }

    @GET
    @Path("/{uuid}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response restGetJob(@PathParam("uuid") UUID uuid) {
        return dao.findById(uuid)
                .map(job -> Response.ok(job.toJson()).build())
                .orElseGet(() -> Response.status(Response.Status.NOT_FOUND).build());
    }

    // --- Job execution ---

    private void processJob(HelloAsyncJob job) {
        // Emulate long operation
        Duration timeUntilDue = Duration.between(Instant.now(), job.dueAt());
        if (timeUntilDue.isPositive()) {
            try {
                Thread.sleep(timeUntilDue);
            } catch (InterruptedException e) {
                // Shutting down mid-job: leave the job IN_PROGRESS, it resumes on next startup.
                Thread.currentThread().interrupt();
                Log.debugf("hello-async job %s interrupted before completing", job.id());
                return;
            }
        }

        try {
            HelloAsyncJob updated = job.withReady("Hello, " + job.inputName() + "!", Instant.now());

            // Save completed job state
            Optional<HelloAsyncJob> saved = dao.save(updated);
            if (saved.isEmpty()) {
                Log.warnf("hello-async job %s changed concurrently while completing", job.id());
                return;
            }

            // Notify ready listeners
            readyBell.sendNotify(job.id());
        } catch (Exception e) {
            Log.warnf(e, "hello-async job %s failed: %s", job.id(), e.getMessage());
            dao.save(job.withFailed(e.getMessage(), Instant.now()));
            readyBell.sendNotify(job.id());
        }
    }
}
