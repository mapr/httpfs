/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.http.server;

import static org.apache.hadoop.util.StringUtils.startupShutdownMessage;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.ConfigurationWithLogging;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The HttpFS web server.
 */
@InterfaceAudience.Private
public class HttpFSServerWebServer {
  private static final Logger LOG =
      LoggerFactory.getLogger(HttpFSServerWebServer.class);

  private static final String HTTPFS_CONFIG_DIR_KEY="httpfs.config.dir";
  private static final String HTTPFS_DEFAULT_XML = "httpfs-default.xml";
  private static final String HTTPFS_SITE_XML = "httpfs-site.xml";

  // HTTP properties
  static final String HTTP_PORT_KEY = "httpfs.http.port";
  private static final int HTTP_PORT_DEFAULT = 14000;
  static final String HTTP_HOST_KEY = "httpfs.http.host";
  private static final String HTTP_HOST_DEFAULT = "0.0.0.0";

  // Status server properties
  static final String STATUS_SERVER_ENABLED_KEY = "httpfs.status.server.enabled";
  private static final boolean STATUS_SERVER_ENABLED_DEFAULT = true;
  static final String STATUS_SERVER_PORT_KEY = "httpfs.status.server.port";
  private static final int STATUS_SERVER_PORT_DEFAULT = 24000;
  static final String STATUS_SERVER_HOSTNAME_KEY = "httpfs.status.server.hostname";
  private static final String STATUS_SERVER_HOSTNAME_DEFAULT = "localhost";

  // SSL properties
  private static final String SSL_ENABLED_KEY = "httpfs.ssl.enabled";
  private static final boolean SSL_ENABLED_DEFAULT = false;

  private static final String HTTP_ADMINS_KEY =
      "httpfs.http.administrators";

  private static final String NAME = "webhdfs";
  private static final String SERVLET_PATH = "/webhdfs";

  static {
    Configuration.addDefaultResource(HTTPFS_DEFAULT_XML);
    Configuration.addDefaultResource(HTTPFS_SITE_XML);
  }

  private final HttpServer2 httpServer;
  private final HttpFSStatusServer statusServer;
  private final String scheme;

  HttpFSServerWebServer(Configuration conf, Configuration sslConf) throws
      Exception {

    boolean sslEnabled = conf.getBoolean(SSL_ENABLED_KEY,
        SSL_ENABLED_DEFAULT);
    scheme = sslEnabled ? HttpServer2.HTTPS_SCHEME : HttpServer2.HTTP_SCHEME;

    String host = conf.get(HTTP_HOST_KEY, HTTP_HOST_DEFAULT);
    int port = conf.getInt(HTTP_PORT_KEY, HTTP_PORT_DEFAULT);
    URI endpoint = new URI(scheme, null, host, port, null, null, null);

    httpServer = new HttpServer2.Builder()
        .setName(NAME)
        .setConf(conf)
        .setSSLConf(sslConf)
        .authFilterConfigurationPrefix(HttpFSAuthenticationFilter.CONF_PREFIX)
        .setACL(new AccessControlList(conf.get(HTTP_ADMINS_KEY, " ")))
        .addEndpoint(endpoint)
        .build();
    boolean statusServerEnabled = conf.getBoolean(STATUS_SERVER_ENABLED_KEY, STATUS_SERVER_ENABLED_DEFAULT);
    String statusServerHostname = conf.get(STATUS_SERVER_HOSTNAME_KEY, STATUS_SERVER_HOSTNAME_DEFAULT);
    int statusServerPort = conf.getInt(STATUS_SERVER_PORT_KEY, STATUS_SERVER_PORT_DEFAULT);
    statusServer = new HttpFSStatusServer(statusServerEnabled, statusServerPort, statusServerHostname);
  }

  public void start() throws Exception {
    httpServer.start();
    statusServer.start();
  }

  public void join() throws InterruptedException {
    httpServer.join();
  }

  public void stop() throws Exception {
    statusServer.stop();
    httpServer.stop();
  }

  public URL getUrl() {
    InetSocketAddress addr = httpServer.getConnectorAddress(0);
    if (null == addr) {
      return null;
    }
    try {
      return new URL(scheme, addr.getHostName(), addr.getPort(),
          SERVLET_PATH);
    } catch (MalformedURLException ex) {
      throw new RuntimeException("It should never happen: " + ex.getMessage(),
          ex);
    }
  }

  public static void main(String[] args) throws Exception {
    startupShutdownMessage(HttpFSServerWebServer.class, args, LOG);
    Configuration conf = new ConfigurationWithLogging(
        new Configuration(true));

    String configDir = System.getProperty(HTTPFS_CONFIG_DIR_KEY, null);
    if (configDir != null){
      conf.addResource(new Path(configDir + File.separator + HTTPFS_SITE_XML));
    }

    Configuration sslConf = new ConfigurationWithLogging(
        readSSLConfiguration(conf, SSLFactory.Mode.SERVER, configDir));

    HttpFSServerWebServer webServer =
        new HttpFSServerWebServer(conf, sslConf);
    webServer.start();
    webServer.join();
  }

  public static Configuration readSSLConfiguration(Configuration conf, SSLFactory.Mode mode, String confDir) {

        Configuration sslConf = new Configuration(false);
        sslConf.setBoolean(SSLFactory.SSL_REQUIRE_CLIENT_CERT_KEY, conf.getBoolean(
                SSLFactory.SSL_REQUIRE_CLIENT_CERT_KEY, SSLFactory.SSL_REQUIRE_CLIENT_CERT_DEFAULT));
        String sslConfResource;
        if (mode == org.apache.hadoop.security.ssl.SSLFactory.Mode.CLIENT) {
            sslConfResource = conf.get(SSLFactory.SSL_CLIENT_CONF_KEY,
                    SSLFactory.SSL_CLIENT_CONF_DEFAULT);
        } else {
            sslConfResource = conf.get(SSLFactory.SSL_SERVER_CONF_KEY,
                    SSLFactory.SSL_SERVER_CONF_DEFAULT);
        }
        if (confDir != null){
            sslConf.addResource(new Path(confDir+ File.separator+sslConfResource));
        } else {
            sslConf.addResource(sslConfResource);
        }

        String provider_path = conf.get(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH);
        if (provider_path != null) {
            sslConf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, provider_path);
        }
        return sslConf;
    }
}
