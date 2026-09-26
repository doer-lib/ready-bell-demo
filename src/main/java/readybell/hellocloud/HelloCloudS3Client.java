package readybell.hellocloud;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;

@ApplicationScoped
public class HelloCloudS3Client {

    @ConfigProperty(name = "ready-bell-demo.bucket")
    String bucket;

    @Inject
    S3Client s3;

    @Inject
    S3Presigner presigner;

    public List<JobFolderInfo> listJobFolders() throws S3Exception {
        List<UUID> uuids = new ArrayList<>();
        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(bucket)
                .delimiter("/")
                .build();

        for (CommonPrefix prefix : s3.listObjectsV2Paginator(request).commonPrefixes()) {
            String name = prefix.prefix();
            if (name.endsWith("/")) {
                name = name.substring(0, name.length() - 1);
            }
            try {
                uuids.add(UUID.fromString(name));
            } catch (IllegalArgumentException e) {
                // not a job directory, skip
            }
        }

        List<JobFolderInfo> jobs = new ArrayList<>();
        for (UUID uuid : uuids) {
            Instant inputCreated = objectLastModified(uuid + "/input.txt").orElse(null);
            Instant outputCreated = objectLastModified(uuid + "/output.txt").orElse(null);
            jobs.add(new JobFolderInfo(uuid, inputCreated, outputCreated));
        }
        return jobs;
    }

    public int objectStatus(String key) {
        try {
            s3.headObject(r -> r.bucket(bucket).key(key));
            return 200;
        } catch (NoSuchKeyException e) {
            return 404;
        } catch (S3Exception e) {
            return e.statusCode();
        }
    }

    public Optional<Instant> objectLastModified(String key) {
        try {
            return Optional.of(s3.headObject(r -> r.bucket(bucket).key(key)).lastModified());
        } catch (S3Exception e) {
            return Optional.empty();
        }
    }

    public String presignPut(String key) {
        return presigner.presignPutObject(b -> b
                .signatureDuration(Duration.ofMinutes(1))
                .putObjectRequest(r -> r.bucket(bucket).key(key)))
                .url().toString();
    }

    public String presignGet(String key) {
        return presigner.presignGetObject(b -> b
                .signatureDuration(Duration.ofHours(1))
                .getObjectRequest(r -> r.bucket(bucket).key(key)))
                .url().toString();
    }

    public void deleteObjectsWithPrefix(String prefix) throws S3Exception {
        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(prefix)
                .build();
        List<ObjectIdentifier> keys = s3.listObjectsV2Paginator(request).contents().stream()
                .map(object -> ObjectIdentifier.builder().key(object.key()).build())
                .toList();
        if (keys.isEmpty()) {
            return;
        }
        s3.deleteObjects(r -> r.bucket(bucket).delete(d -> d.objects(keys)));
    }
}
