package readybell;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.Executor;

import io.quarkus.logging.Log;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.inject.Inject;

@ApplicationScoped
public class ReadyBellService {

    private static final String REMOTE_HOST = "ready-bell.com";
    private static final int REMOTE_PORT = 3137;

    @Inject
    Executor executor;
    
    @Inject
    Event<ReadyBellEvent> readyEvent;
    
    private DatagramSocket socket;

    @PostConstruct
    public void init() throws Exception {
        socket = new DatagramSocket();
        executor.execute(this::listenLoop);
        Log.infof("ReadyBellService bound to port: %s", socket.getLocalPort());
    }

    @PreDestroy
    public void destroy() {
        if (socket != null && !socket.isClosed()) {
            socket.close();
            Log.info("ReadyBellService UDP socket closed.");
        }
    }

    public boolean sendListen(UUID uuid, int seconds) {
        return send("Listen " + uuid + " " + seconds);
    }

    public boolean sendNotify(UUID uuid) {
        return send("Notify " + uuid);
    }

    private boolean send(String message) {
        InetSocketAddress target = new InetSocketAddress(REMOTE_HOST, REMOTE_PORT);
        if (target.isUnresolved()) {
            Log.debugf("Failed to resolve %s, dropping message: %s", REMOTE_HOST, message);
            return false;
        }

        try {
            var bytes = message.getBytes(StandardCharsets.UTF_8);
            socket.send(new DatagramPacket(bytes, bytes.length, target));
            Log.debugf("Sent UDP packet: %s", message);
            return true;
        } catch (IOException e) {
            Log.warnf(e, "Failed to send UDP packet: %s", message);
            return false;
        }
    }

    private void listenLoop() {
        var buffer = new byte[512];
        var packet = new DatagramPacket(buffer, buffer.length);

        while (!socket.isClosed() && !Thread.currentThread().isInterrupted()) {
            try {
                socket.receive(packet);
                var message = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8).trim();
                Log.debugf("Received UDP packet: %s", message);

                if (message.startsWith("Ready ")) {
                    UUID uuid = UUID.fromString(message.substring("Ready ".length()).trim());
                    readyEvent.fire(new ReadyBellEvent(uuid));
                }
            } catch (SocketException e) {
                Log.infof("UDP socket closed, stopping listener.");
                break;
            } catch (Exception e) {
                Log.errorf(e, "UDP reader error: %s", e.getMessage());
            }
        }
    }
}
