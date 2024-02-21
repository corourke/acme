package corourke.datagen;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

public class StatusPageServer {
  private HttpServer server;
  private int port;

  public StatusPageServer(int port) {
    this.port = port;
  }

  public void start() throws IOException {
    server = HttpServer.create(new InetSocketAddress(port), 0);
    server.createContext("/status", new StatusHandler());
    server.createContext("/shutdown", new ShutdownHandler(this));
    server.setExecutor(null); // creates a default executor
    server.start();
    System.out.println("Server started on port " + port);
  }

  public void stop() {
    server.stop(0);
    System.out.println("Server stopped.");
  }

  static class StatusHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange exchange) throws IOException {
      String response = "Server is running";
      exchange.sendResponseHeaders(200, response.getBytes().length);
      OutputStream os = exchange.getResponseBody();
      os.write(response.getBytes());
      os.close();
    }
  }

  static class ShutdownHandler implements HttpHandler {
    private StatusPageServer simpleHttpServer;

    public ShutdownHandler(StatusPageServer simpleHttpServer) {
      this.simpleHttpServer = simpleHttpServer;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
      String response = "Shutting down server...";
      exchange.sendResponseHeaders(200, response.getBytes().length);
      OutputStream os = exchange.getResponseBody();
      os.write(response.getBytes());
      os.close();

      new Thread(() -> {
        try {
          Thread.sleep(1000); // wait for 1 second before shutting down
          simpleHttpServer.stop();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }).start();
    }
  }
}
