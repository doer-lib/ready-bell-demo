package readybell.helloasync;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.bin932.readybelldemo.ReadyBellService;

import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
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

@ApplicationScoped
@Path("/hello-async")
public class HelloAsyncResource {

    @Inject
    HelloAsyncDao dao;

    @Inject
    ReadyBellService readyBell;

    @Context
    UriInfo uriInfo;

    ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    public void onStart(@Observes StartupEvent ev) {
        dao.findAllInProgress()
                .forEach(job -> executor.submit(() -> asyncJob(job)));
    }

    public void onStop(@Observes ShutdownEvent ev) {
        executor.shutdownNow();
    }

    @POST
    @Path("/{uuid}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response create(@PathParam("uuid") UUID uuid, JsonObject body) {
        HelloAsyncJob candidate = HelloAsyncJob.fromRequest(uuid, body);

        URI location = uriInfo.getBaseUriBuilder()
                .path(HelloAsyncResource.class)
                .path(HelloAsyncResource.class, "get")
                .build(uuid);

        Optional<HelloAsyncJob> inserted = dao.insertIfAbsent(candidate);
        if (inserted.isPresent()) {
            executor.submit(() -> asyncJob(inserted.get()));
            return Response.created(location).entity(inserted.get().toJson()).build();
        }

        return dao.findById(uuid)
                .filter(existing -> Objects.equals(existing.inputName(), candidate.inputName())
                        && Objects.equals(existing.inputDbgDelaySec(), candidate.inputDbgDelaySec()))
                .map(existing -> Response.created(location).entity(existing.toJson()).build())
                .orElseGet(() -> Response.status(Response.Status.CONFLICT)
                        .entity("uuid already used with different input").build());
    }

    @GET
    @Path("/{uuid}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response get(@PathParam("uuid") UUID uuid) {
        return dao.findById(uuid)
                .map(job -> Response.ok(job.toJson()).build())
                .orElseGet(() -> Response.status(Response.Status.NOT_FOUND).build());
    }

    private HelloAsyncJob asyncJob(HelloAsyncJob job) throws Exception {
        // Emulate long operation
        Duration d = Duration.between(Instant.now(), job.dueAt());
        if (d.isPositive()) {
            Thread.sleep(d);
        }
        HelloAsyncJob updated = job.withReady("Hello, " + job.inputName() + "!", Instant.now());

        // Save completed oration state.
        Optional<HelloAsyncJob> saved = dao.save(updated);
        if (saved.isEmpty()) {
            Log.warnf("hello-async job %s changed concurrently while completing", job.id());
            return null;
        }

        // Notiry ready listeners
        readyBell.sendNotify(job.id());
        return saved.get();
    }
}
