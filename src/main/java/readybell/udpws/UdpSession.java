package readybell.udpws;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;

import io.quarkus.logging.Log;
import io.quarkus.websockets.next.CloseReason;
import io.quarkus.websockets.next.WebSocketConnection;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

/**
 * One browser websocket and the UDP socket it owns, like one running {@code nc -u host port}.
 */
class UdpSession {

    static final int PONG_TIMEOUT = 4000;

    private final WebSocketConnection connection;
    private final InetSocketAddress remote;
    private final DatagramSocket socket;
    private volatile boolean awaitingPong;
    private volatile long pingTimerId = -1;

    UdpSession(WebSocketConnection connection, InetSocketAddress remote) throws SocketException {
        this.connection = connection;
        this.remote = remote;
        // not connect()ed: a connected DatagramSocket silently drops empty packets,
        // and packets from any address are passed to the browser
        this.socket = new DatagramSocket();
        Thread.ofVirtual().name("udp-ws-" + connection.id()).start(this::receiveLoop);
    }

    void setPingTimerId(long pingTimerId) {
        this.pingTimerId = pingTimerId;
    }

    long getPingTimerId() {
        return pingTimerId;
    }

    int getLocalPort() {
        return socket.getLocalPort();
    }

    void send(byte[] bytes) throws IOException {
        socket.send(new DatagramPacket(bytes, bytes.length, remote));
    }

    /** Called by the ping timer: closes the connection if the previous ping got no pong. */
    void ping() {
        if (awaitingPong) {
            Log.debugf("udp-ws %s: no pong, closing", connection.id());
            connection.close(new CloseReason(PONG_TIMEOUT, "pong timeout")).subscribe().with(v -> {}, e -> {});
            return;
        }
        awaitingPong = true;
        connection.sendPing(Buffer.buffer("ping")).subscribe().with(v -> {}, e -> Log.debugf(e, "Cant send ping"));
    }

    void pongReceived() {
        awaitingPong = false;
    }

    void close() {
        if (!socket.isClosed()) {
            socket.close();
            Log.debugf("udp-ws %s: UDP socket closed", connection.id());
        }
    }

    private void receiveLoop() {
        var buffer = new byte[64 * 1024];
        var packet = new DatagramPacket(buffer, buffer.length);

        while (!socket.isClosed()) {
            try {
                socket.receive(packet);
                var data = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
                Log.debugf("udp-ws %s: received UDP packet: %s", connection.id(), data);
                connection.sendTextAndAwait(new JsonObject().put("type", "recv").put("data", data).encode());
            } catch (SocketException e) {
                break; // socket closed
            } catch (Exception e) {
                if (connection.isClosed()) {
                    break;
                }
                Log.warnf(e, "udp-ws %s: receive error: %s", connection.id(), e.getMessage());
            }
        }
    }
}
