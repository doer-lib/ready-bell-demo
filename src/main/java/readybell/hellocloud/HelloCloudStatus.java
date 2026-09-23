package readybell.hellocloud;

public enum HelloCloudStatus {
    NEW,
    IN_PROGRESS,
    READY,
    FAILED;

    public boolean isTerminal() {
        return this == READY || this == FAILED;
    }
}
