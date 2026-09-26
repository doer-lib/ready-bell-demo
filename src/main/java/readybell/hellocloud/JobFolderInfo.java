package readybell.hellocloud;

import java.time.Instant;
import java.util.UUID;

record JobFolderInfo(UUID uuid, Instant inputTxtCreated, Instant outputTxtCreated) {
}
