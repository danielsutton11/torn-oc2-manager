package com.Torn.Helpers;

import com.Torn.FactionCrimes.Available.GetAvailableCrimes;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public class HttpTriggerServer {
    private static final Logger logger = LoggerFactory.getLogger(HttpTriggerServer.class);
    private static final int DEFAULT_PORT = 8080;
    private final GetAvailableCrimes.CrimesPollingJob pollingJob;
    private HttpServer server;

    public HttpTriggerServer(GetAvailableCrimes.CrimesPollingJob pollingJob) {
        this.pollingJob = pollingJob;
    }

    public void start() throws IOException {
        int port = getPort();
        server = HttpServer.create(new InetSocketAddress(port), 0);

        // Health check endpoint
        server.createContext("/health", new HealthHandler());

        // Manual trigger endpoint
        server.createContext("/trigger", new TriggerHandler());

        // Status endpoint
        server.createContext("/status", new StatusHandler());

        server.setExecutor(Executors.newFixedThreadPool(3));
        server.start();

        logger.info("HTTP server started on port {}", port);
        logger.info("Available endpoints:");
        logger.info("  GET /health - Health check");
        logger.info("  POST /trigger - Manual data fetch");
        logger.info("  GET /status - Application status");
    }

    private int getPort() {
        String portStr = System.getenv("PORT");
        if (portStr != null && !portStr.isEmpty()) {
            try {
                return Integer.parseInt(portStr);
            } catch (NumberFormatException e) {
                logger.warn("Invalid PORT value: {}. Using default {}", portStr, DEFAULT_PORT);
            }
        }
        return DEFAULT_PORT;
    }

    class HealthHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String response = "{\"status\":\"healthy\",\"timestamp\":\"" +
                    java.time.LocalDateTime.now().toString() + "\"}";
            sendResponse(exchange, 200, response);
        }
    }

    class TriggerHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"POST".equals(exchange.getRequestMethod())) {
                sendResponse(exchange, 405, "{\"error\":\"Method not allowed. Use POST.\"}");
                return;
            }

            try {
                logger.info("Manual trigger requested via HTTP endpoint");
                pollingJob.execute(null);

                String response = "{\"status\":\"success\",\"message\":\"Data fetch completed\",\"timestamp\":\"" +
                        java.time.LocalDateTime.now().toString() + "\"}";
                sendResponse(exchange, 200, response);

            } catch (Exception e) {
                logger.error("Manual trigger failed", e);
                String response = "{\"status\":\"error\",\"message\":\"" + e.getMessage() + "\"}";
                sendResponse(exchange, 500, response);
            }
        }
    }

    class StatusHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String pollingInterval = System.getenv("POLLING_INTERVAL_MINUTES");
            if (pollingInterval == null) pollingInterval = "5";

            String response = String.format(
                    "{\"status\":\"running\",\"polling_interval_minutes\":%s,\"timestamp\":\"" +
                            java.time.LocalDateTime.now().toString() + "\"}",
                    pollingInterval
            );
            sendResponse(exchange, 200, response);
        }
    }

    private void sendResponse(HttpExchange exchange, int statusCode, String response) throws IOException {
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(statusCode, response.getBytes().length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(response.getBytes());
        }
    }

    public void stop() {
        if (server != null) {
            server.stop(0);
            logger.info("HTTP server stopped");
        }
    }
}