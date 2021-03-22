package org.apache.hadoop.fs.http.server;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Objects;

public class HttpFSStatusServer {

  private Server server;
  private final int port;
  private final String hostname;
  private final boolean enabled;

  public HttpFSStatusServer(Boolean enabled, Integer port, String hostname) {
    this.enabled = enabled;
    this.port = Objects.requireNonNull(port, "Status port is null");
    this.hostname = Objects.requireNonNull(hostname, "Status hostname is null");
  }

  public void start() throws Exception {
    if (enabled) {
      final QueuedThreadPool threadPool = new QueuedThreadPool();
      threadPool.setDaemon(true);
      threadPool.setMaxThreads(5);
      server = new Server(threadPool);

      ServerConnector connector = new ServerConnector(server);
      connector.setPort(port);
      connector.setHost(hostname);
      server.addConnector(connector);

      ContextHandler context = new ContextHandler();
      context.setContextPath("/status");
      context.setAllowNullPathInfo(true);
      context.setHandler(new HttpFSStatusHandler());

      HandlerCollection handlers = new HandlerCollection();
      handlers.setHandlers(new Handler[]{context});
      server.setHandler(handlers);
      server.start();
    }
  }

  public void stop() throws Exception {
    if (server != null) {
      server.stop();
    }
  }

  class HttpFSStatusHandler extends AbstractHandler {
    public void handle(String target,
                       Request baseRequest,
                       HttpServletRequest request,
                       HttpServletResponse response)
            throws IOException {
      response.setContentType("text/plain");
      response.setCharacterEncoding("UTF-8");
      response.setStatus(HttpServletResponse.SC_OK);
      baseRequest.setHandled(true);
      response.getWriter().write("0");
    }
  }
}
