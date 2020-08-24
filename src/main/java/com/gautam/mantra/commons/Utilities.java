package com.gautam.mantra.commons;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;

public class Utilities {

    public static boolean isPortAvailable(int port) {
        try (ServerSocket ignored = new ServerSocket(port); DatagramSocket ignored1 = new DatagramSocket(port)) {
            return true;
        } catch (IOException e) {
            return false;
        }
    }
}
