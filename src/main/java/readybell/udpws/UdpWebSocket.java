package readybell.udpws;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import io.quarkus.logging.Log;
import io.quarkus.websockets.next.CloseReason;
import io.quarkus.websockets.next.OnClose;
import io.quarkus.websockets.next.OnOpen;
import io.quarkus.websockets.next.OnPongMessage;
import io.quarkus.websockets.next.OnTextMessage;
import io.quarkus.websockets.next.WebSocket;
import io.quarkus.websockets.next.WebSocketConnection;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;
import jakarta.inject.Inject;

/**
 * Lets a browser act like {@code nc -u ready-bell.com 3137}: every websocket gets its own UDP socket.
 *
 * <pre>
 * browser → server  {"type":"send","data":"Listen &lt;uuid&gt; 60"}   sends data as one UDP packet
 * browser → server  {"type":"close"}                              closes the UDP socket and the websocket
 * server → browser  {"type":"recv","data":"Ready &lt;uuid&gt;"}       a UDP packet was received
 * server → browser  {"type":"error","data":"..."}                 the last command was rejected
 * </pre>
 *
 * The server sends a websocket ping frame every minute and closes the connection (code 4000)
 * if the browser has not answered the previous one; browsers answer pings automatically.
 */
@WebSocket(path = "/udp-ws")
public class UdpWebSocket {

    static final int TRY_AGAIN_LATER = 1013;

    @ConfigProperty(name = "udp-ws.remote-host", defaultValue = "ready-bell.com")
    String remoteHost;

    @ConfigProperty(name = "udp-ws.remote-port", defaultValue = "3137")
    int remotePort;

    @ConfigProperty(name = "udp-ws.max-connections", defaultValue = "100")
    int maxConnections;

    @ConfigProperty(name = "udp-ws.max-send-bytes", defaultValue = "512")
    int maxSendBytes;

    @ConfigProperty(name = "udp-ws.ping-interval", defaultValue = "1M")
    Duration pingInterval;

    @Inject
    Vertx vertx;

    private final Map<String, UdpSession> sessions = new ConcurrentHashMap<>();

    @OnOpen
    public void onOpen(WebSocketConnection connection) {
        // resolve DNS before taking the lock, so a slow lookup doesn't hold up other connections
        var remote = new InetSocketAddress(remoteHost, remotePort);
        if (remote.isUnresolved()) {
            Log.warnf("udp-ws %s: cant resolve %s", connection.id(), remoteHost);
            close(connection, new CloseReason(CloseReason.INTERNAL_SERVER_ERROR.getCode(), "cant resolve " + remoteHost));
            return;
        }

        UdpSession session;
        synchronized (sessions) {
            if (sessions.size() >= maxConnections) {
                Log.debugf("udp-ws %s: rejected, %d connections open", connection.id(), sessions.size());
                close(connection, new CloseReason(TRY_AGAIN_LATER, "too many connections"));
                return;
            }
            try {
                session = new UdpSession(connection, remote);
            } catch (IOException | RuntimeException e) {
                Log.warnf(e, "udp-ws %s: cant open UDP socket to %s", connection.id(), remote);
                close(connection, new CloseReason(CloseReason.INTERNAL_SERVER_ERROR.getCode(), "cant open UDP socket"));
                return;
            }
            sessions.put(connection.id(), session);
        }
        session.setPingTimerId(vertx.setPeriodic(pingInterval.toMillis(), id -> session.ping()));
        Log.debugf("udp-ws %s: opened, UDP local port %d", connection.id(), session.getLocalPort());
    }

    @OnTextMessage
    public void onMessage(String message, WebSocketConnection connection) {
        UdpSession session = sessions.get(connection.id());
        if (session == null) {
            return;
        }

        JsonObject json;
        try {
            json = new JsonObject(message);
        } catch (DecodeException e) {
            sendError(connection, "invalid JSON");
            return;
        }

        switch (String.valueOf(json.getValue("type"))) {
            case "send" -> {
                String data = Objects.requireNonNullElse(json.getString("data"), "");
                byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
                if (bytes.length > maxSendBytes) {
                    sendError(connection, "data is longer than " + maxSendBytes + " bytes");
                    return;
                }
                try {
                    session.send(bytes);
                    Log.debugf("udp-ws %s: sent UDP packet: %s", connection.id(), data);
                } catch (IOException e) {
                    Log.debugf(e, "udp-ws %s: cant send UDP packet", connection.id());
                    sendError(connection, "cant send UDP packet: " + e.getMessage());
                }
            }
            case "close" -> {
                closeSession(connection.id());
                connection.closeAndAwait();
            }
            default -> sendError(connection, "unknown type, expected send or close");
        }
    }

    @OnPongMessage
    public void onPong(Buffer data, WebSocketConnection connection) {
        UdpSession session = sessions.get(connection.id());
        if (session != null) {
            session.pongReceived();
        }
    }

    @OnClose
    public void onClose(WebSocketConnection connection) {
        closeSession(connection.id());
        Log.debugf("udp-ws %s: closed", connection.id());
    }

    private void closeSession(String connectionId) {
        UdpSession session = sessions.remove(connectionId);
        if (session != null) {
            vertx.cancelTimer(session.getPingTimerId());
            session.close();
        }
    }

    private static void close(WebSocketConnection connection, CloseReason reason) {
        connection.close(reason).subscribe().with(v -> {}, e -> Log.debugf(e, "Cant close websocket"));
    }

    private void sendError(WebSocketConnection connection, String error) {
        connection.sendTextAndAwait(new JsonObject().put("type", "error").put("data", error).encode());
    }
}
