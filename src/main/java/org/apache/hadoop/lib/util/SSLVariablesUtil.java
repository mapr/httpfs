package org.apache.hadoop.lib.util;

import com.mapr.web.security.SslConfig;
import com.mapr.web.security.WebSecurityManager;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;


public class SSLVariablesUtil {

  public static void main(String[] args) {
    System.exit(new SSLVariablesUtil().run(args));
  }

  private int run(String[] args) {
    if (args.length != 1 && args.length !=2) {
      System.err.println("Wrong number of arguments");
      return 1;
    }
    try (SslConfig sslConfig = WebSecurityManager.getSslConfig()) {
      switch (args[0]) {
        case "keystoreFile":
          System.out.println(sslConfig.getClientKeystoreLocation());
          break;
        case "keystorePass":
          System.out.println(sslConfig.getClientKeystorePassword());
          break;
        case "truststoreFile":
          System.out.println(sslConfig.getClientTruststoreLocation());
          break;
        case "truststorePass":
          System.out.println(sslConfig.getClientTruststorePassword());
          break;
        case "updateSslConf":
          if (args.length != 2 && Files.notExists(Paths.get(args[1]))) {
            System.err.println("Please check path to HttpFS configuration file:" + args[1]);
            return 1;
          }
          updateSslConfiguration(new String(sslConfig.getClientKeystorePassword()),
              new String(sslConfig.getClientTruststorePassword()), args[1]);
          break;
        default:
          System.err.println("Unknown option.");
          System.err.println("Usage: org.apache.oozie.tools.OozieSSLVariablesCLI [keystoreFile | keystorePass | " +
              "truststoreFile | truststorePass | updateSslConf httpfsConfPath]");
          return 1;
      }
    }
    return 0;
  }

  private void updateSslConfiguration(String keystorePass, String truststorePass, String httpfsConf) {
    try {
      FileReader fileReader = new FileReader(httpfsConf);
      FileWriter fileWriter = new FileWriter(httpfsConf + ".tmp");
      BufferedReader reader = new BufferedReader(fileReader);
      BufferedWriter writer = new BufferedWriter(fileWriter);
      String line;
      while ((line = reader.readLine()) != null) {
        if(line.matches(".*keystorePass=\".*\"*")){
          line = line.replaceAll("keystorePass=\".*\"", "keystorePass=\""+keystorePass+"\"");
        }
        if(line.matches(".*truststorePass=\".*\"*")){
          line = line.replaceAll("truststorePass=\".*\"", "truststorePass=\""+truststorePass+"\"");
        }
        writer.write(line);
        writer.newLine();
      }
      reader.close();
      writer.close();
      Files.delete(Paths.get(httpfsConf));
      Files.move(Paths.get(httpfsConf + ".tmp"), Paths.get(httpfsConf));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
