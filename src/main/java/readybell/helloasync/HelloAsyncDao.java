package readybell.helloasync;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import javax.sql.DataSource;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;

@ApplicationScoped
public class HelloAsyncDao {

    @Inject
    DataSource dataSource;

    public Optional<HelloAsyncJob> insertIfAbsent(HelloAsyncJob candidate) {
        String sql = """
                INSERT INTO hello_async_job (id, created, modified, status, version, json_data)
                VALUES (?, ?, ?, 'IN_PROGRESS', 0, ?::jsonb)
                ON CONFLICT (id) DO NOTHING
                RETURNING id, created, modified, status, version, json_data
                """;
        try (Connection c = dataSource.getConnection();
                PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setObject(1, candidate.id());
            ps.setTimestamp(2, Timestamp.from(candidate.created()));
            ps.setTimestamp(3, Timestamp.from(candidate.modified()));
            ps.setString(4, toStorageJson(candidate).toString());
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(mapRow(rs)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new RuntimeException("insertIfAbsent failed", e);
        }
    }

    public Optional<HelloAsyncJob> findById(UUID id) {
        String sql = """
                SELECT id, created, modified, status, version, json_data
                FROM hello_async_job WHERE id = ?
                """;
        try (Connection c = dataSource.getConnection();
                PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setObject(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(mapRow(rs)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new RuntimeException("findById failed", e);
        }
    }

    public List<HelloAsyncJob> findAllInProgress() {
        String sql = """
                SELECT id, created, modified, status, version, json_data
                FROM hello_async_job
                WHERE status = 'IN_PROGRESS'
                ORDER BY created, id
                """;
        List<HelloAsyncJob> jobs = new ArrayList<>();
        try (Connection c = dataSource.getConnection();
                Statement st = c.createStatement();
                ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                jobs.add(mapRow(rs));
            }
            return jobs;
        } catch (SQLException e) {
            throw new RuntimeException("findAllInProgress failed", e);
        }
    }

    /** Optimistic-locking update; empty if the row was concurrently modified (version no longer matches). */
    public Optional<HelloAsyncJob> save(HelloAsyncJob job) {
        String sql = """
                UPDATE hello_async_job
                SET status = ?, modified = ?, version = version + 1, json_data = ?::jsonb
                WHERE id = ? AND version = ?
                RETURNING id, created, modified, status, version, json_data
                """;
        try (Connection c = dataSource.getConnection();
                PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, job.status().name());
            ps.setTimestamp(2, Timestamp.from(job.modified()));
            ps.setString(3, toStorageJson(job).toString());
            ps.setObject(4, job.id());
            ps.setInt(5, job.version());
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(mapRow(rs)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new RuntimeException("save failed", e);
        }
    }

    /** Builds the JSON stored in the json_data column from a job's non-column fields. */
    private static JsonObject toStorageJson(HelloAsyncJob job) {
        JsonObjectBuilder builder = Json.createObjectBuilder()
                .add("inputName", job.inputName());
        if (job.inputDbgDelaySec() != null) {
            builder.add("inputDbgDelaySec", job.inputDbgDelaySec());
        } else {
            builder.addNull("inputDbgDelaySec");
        }
        if (job.outputGreeting() != null) {
            builder.add("outputGreeting", job.outputGreeting());
        } else {
            builder.addNull("outputGreeting");
        }
        if (job.errorMessage() != null) {
            builder.add("errorMessage", job.errorMessage());
        } else {
            builder.addNull("errorMessage");
        }
        return builder.build();
    }

    private static String getNullableString(JsonObject data, String key) {
        return data.containsKey(key) && !data.isNull(key) ? data.getString(key) : null;
    }

    private static Integer getNullableInt(JsonObject data, String key) {
        return data.containsKey(key) && !data.isNull(key) ? data.getInt(key) : null;
    }

    private HelloAsyncJob mapRow(ResultSet rs) throws SQLException {
        JsonObject data;
        try (var reader = Json.createReader(new StringReader(rs.getString("json_data")))) {
            data = reader.readObject();
        }
        return new HelloAsyncJob(
                (UUID) rs.getObject("id"),
                rs.getTimestamp("created").toInstant(),
                rs.getTimestamp("modified").toInstant(),
                HelloAsyncStatus.valueOf(rs.getString("status")),
                rs.getInt("version"),
                getNullableString(data, "inputName"),
                getNullableInt(data, "inputDbgDelaySec"),
                getNullableString(data, "outputGreeting"),
                getNullableString(data, "errorMessage"));
    }
}
