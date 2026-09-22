package com.bin932.readybelldemo;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.inject.Inject;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.logging.Logger;

@ApplicationScoped
public class ReadyBellService {

    private static final Logger log = Logger.getLogger(ReadyBellService.class.getName());
    private final InetSocketAddress remoteServer = new InetSocketAddress("ready-bell.com", 3137);

    private DatagramSocket socket;

    @Inject
    Executor executor;

    @Inject
    Event<ReadyBellRegistered> registeredEvent;

    @Inject
    Event<ReadyBellReady> readyEvent;

    @PostConstruct
    public void init() {
        try {
            socket = new DatagramSocket();
            executor.execute(this::listenLoop);
            log.info(() -> "ReadyBellService bound to port: " + socket.getLocalPort());
        } catch (SocketException e) {
            throw new RuntimeException("Failed to initialize UDP socket", e);
        }
    }

    private void listenLoop() {
        var buffer = new byte[512];
        var packet = new DatagramPacket(buffer, buffer.length);

        while (!socket.isClosed() && !Thread.currentThread().isInterrupted()) {
            try {
                socket.receive(packet);
                String message = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8).trim();
                log.info(() -> "Received UDP packet: " + message);

                if (message.startsWith("Registered ")) {
                    onRegistered(message.substring("Registered ".length()).trim());
                } else if (message.startsWith("Ready ")) {
                    onReady(UUID.fromString(message.substring("Ready ".length()).trim()));
                }
            } catch (SocketException e) {
                log.info("UDP socket closed, stopping listener.");
                break;
            } catch (Exception e) {
                log.severe("UDP reader error: " + e.getMessage());
            }
        }
    }

    private void onRegistered(String rest) {
        String[] parts = rest.split("\\s+");
        UUID uuid = UUID.fromString(parts[0]);
        String ip = parts[1];
        int port = Integer.parseInt(parts[2]);
        int ttlSeconds = Integer.parseInt(parts[3]);
        registeredEvent.fireAsync(new ReadyBellRegistered(uuid, ip, port, ttlSeconds));
    }

    public void onReady(UUID uuid) {
        readyEvent.fireAsync(new ReadyBellReady(uuid));
    }

    public void sendListen(UUID uuid, int seconds) throws Exception {
        var bytes = ("Listen " + uuid + " " + seconds).getBytes(StandardCharsets.UTF_8);
        socket.send(new DatagramPacket(bytes, bytes.length, remoteServer));
    }

    public void sendNotify(UUID uuid) throws Exception {
        var bytes = ("Notify " + uuid).getBytes(StandardCharsets.UTF_8);
        socket.send(new DatagramPacket(bytes, bytes.length, remoteServer));
    }

    @PreDestroy
    public void destroy() {
        if (socket != null && !socket.isClosed()) {
            socket.close();
            log.info("ReadyBellService UDP socket closed.");
        }
    }
}
