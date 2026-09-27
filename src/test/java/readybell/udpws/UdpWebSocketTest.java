package readybell.udpws;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import io.vertx.core.json.JsonObject;

@QuarkusTest
class UdpWebSocketTest {

    private static DatagramSocket echoServer;

    @TestHTTPResource("/udp-ws")
    URI uri;

    @BeforeAll
    static void startEchoServer() throws SocketException {
        echoServer = new DatagramSocket(13137, InetAddress.getLoopbackAddress());
        Thread.ofVirtual().start(() -> {
            var packet = new DatagramPacket(new byte[1024], 1024);
            while (!echoServer.isClosed()) {
                try {
                    echoServer.receive(packet);
                    var reply = "echo " + new String(packet.getData(), 0, packet.getLength());
                    echoServer.send(new DatagramPacket(reply.getBytes(), reply.length(), packet.getSocketAddress()));
                } catch (Exception e) {
                    // closed
                }
            }
        });
    }

    @AfterAll
    static void stopEchoServer() {
        echoServer.close();
    }

    @Test
    void eachConnectionHasItsOwnUdpSocket() throws Exception {
        var a = connect();
        var b = connect();

        a.send("send", "Listen 1 60");
        b.send("send", "Listen 2 60");

        assertEquals(recv("echo Listen 1 60"), a.next());
        assertEquals(recv("echo Listen 2 60"), b.next());
        assertTrue(a.messages.isEmpty());
        assertTrue(b.messages.isEmpty());

        a.close();
        b.close();
    }

    @Test
    void sendWithoutDataSendsEmptyPacket() throws Exception {
        var a = connect();

        a.ws.sendText("{\"type\":\"send\"}", true).join();
        assertEquals(recv("echo "), a.next());

        a.send("send", null);
        assertEquals(recv("echo "), a.next());

        a.close();
    }

    @Test
    void rejectsBadCommands() throws Exception {
        var a = connect();

        a.send("send", "x".repeat(513));
        assertEquals("error", a.next().getString("type"));

        a.ws.sendText("not json", true).join();
        assertEquals("error", a.next().getString("type"));

        a.send("bogus", "");
        assertEquals("error", a.next().getString("type"));

        a.close();
    }

    @Test
    void closeCommandClosesWebsocket() throws Exception {
        var a = connect();
        a.send("close", null);
        assertTrue(a.closed.get(5, TimeUnit.SECONDS) >= 1000);
    }

    @Test
    void limitsConnections() throws Exception {
        List<Client> clients = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            clients.add(connect());
        }
        var rejected = connect();
        assertEquals(UdpWebSocket.TRY_AGAIN_LATER, rejected.closed.get(5, TimeUnit.SECONDS));

        for (Client c : clients) {
            c.close();
        }
    }

    @Test
    void pingsAndStaysOpenWhileAnswered() throws Exception {
        var a = connect();
        Thread.sleep(3500); // ping interval is 1s in tests; the JDK client answers pongs itself
        assertTrue(a.pings.get() >= 2);
        assertFalse(a.closed.isDone());
        a.close();
    }

    private static JsonObject recv(String data) {
        return new JsonObject().put("type", "recv").put("data", data);
    }

    private Client connect() {
        var client = new Client();
        client.ws = HttpClient.newHttpClient().newWebSocketBuilder()
                .buildAsync(URI.create(uri.toString().replaceFirst("^http", "ws")), client)
                .join();
        return client;
    }

    static class Client implements WebSocket.Listener {
        WebSocket ws;
        final BlockingQueue<JsonObject> messages = new LinkedBlockingQueue<>();
        final CompletableFuture<Integer> closed = new CompletableFuture<>();
        final AtomicInteger pings = new AtomicInteger();
        private final StringBuilder partial = new StringBuilder();

        void send(String type, String data) {
            ws.sendText(new JsonObject().put("type", type).put("data", data).encode(), true).join();
        }

        JsonObject next() throws InterruptedException {
            return messages.poll(5, TimeUnit.SECONDS);
        }

        void close() throws Exception {
            ws.sendClose(WebSocket.NORMAL_CLOSURE, "").join();
            closed.get(5, TimeUnit.SECONDS);
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            partial.append(data);
            if (last) {
                messages.add(new JsonObject(partial.toString()));
                partial.setLength(0);
            }
            webSocket.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onPing(WebSocket webSocket, ByteBuffer message) {
            pings.incrementAndGet();
            return WebSocket.Listener.super.onPing(webSocket, message);
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            closed.complete(statusCode);
            return null;
        }
    }
}
