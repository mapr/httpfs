package org.apache.hadoop.fs.http.server;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_WEBAPPS_CUSTOM_HEADERS_PATH;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.conf.Configuration;

/**
 * Filter which adds the jetty headers to every response
 */
public class JettyHeadersAppenderFilter implements Filter {

  private static final String DEFAULT_CUSTOM_HEADERS_PATH = "etc/hadoop/jetty-headers.xml";

  private Properties headers = new Properties();

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    Configuration config = HttpFSServerWebApp.get().getConfig();
    boolean isSecurityEnabled = !config.get("httpfs.hadoop.authentication.type").equalsIgnoreCase("simple");
    String path = config.get(HADOOP_WEBAPPS_CUSTOM_HEADERS_PATH);

    if (path == null && isSecurityEnabled) {
      path = DEFAULT_CUSTOM_HEADERS_PATH;
    }

    if (path != null) {
      String homeDir = HttpFSServerWebApp.get().getHomeDir();
      path = path.startsWith(homeDir) ? path : homeDir + "/" + path;
      File headersConf = new File(path);
      try {
        headers.loadFromXML(new FileInputStream(headersConf));
      } catch (IOException e) {
        throw new RuntimeException("Unable to load jetty headers!");
      }
    }
  }

  @Override
  public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
    HttpServletResponse httpResponse = (HttpServletResponse) servletResponse;
    if (!headers.isEmpty()) {
      for (String headerName : headers.stringPropertyNames()) {
        httpResponse.addHeader(headerName, headers.getProperty(headerName));
      }
    }
    filterChain.doFilter(servletRequest, httpResponse);
  }

  @Override
  public void destroy() {
  }

}
