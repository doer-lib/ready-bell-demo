package com.bin932.readybelldemo;

import java.util.UUID;

public record ReadyBellRegistered(UUID uuid, String ip, int port, int ttlSeconds) {
}
