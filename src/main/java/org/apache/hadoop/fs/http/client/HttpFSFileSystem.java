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
package org.apache.hadoop.fs.http.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.Authenticator;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.HttpExceptionUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

/**
 * HttpFSServer implementation of the FileSystemAccess FileSystem.
 * <p/>
 * This implementation allows a user to access HDFS over HTTP via a HttpFSServer server.
 */
public class HttpFSFileSystem extends FileSystem {

  public static final String SERVICE_NAME = "/webhdfs";

  public static final String SERVICE_VERSION = "/v1";

  public static final String SERVICE_PREFIX = SERVICE_NAME + SERVICE_VERSION;

  public static final String OP_PARAM = "op";
  public static final String DO_AS_PARAM = "doas";
  public static final String OVERWRITE_PARAM = "overwrite";
  public static final String REPLICATION_PARAM = "replication";
  public static final String BLOCKSIZE_PARAM = "blocksize";
  public static final String PERMISSION_PARAM = "permission";
  public static final String DESTINATION_PARAM = "destination";
  public static final String RECURSIVE_PARAM = "recursive";
  public static final String OWNER_PARAM = "owner";
  public static final String GROUP_PARAM = "group";
  public static final String MODIFICATION_TIME_PARAM = "modificationtime";
  public static final String ACCESS_TIME_PARAM = "accesstime";
  public static final String RENEWER_PARAM = "renewer";
  public static final String OFFSET_PARAM = "offset";
  public static final String LENGTH_PARAM = "length";
  public static final String FSACTION_PARAM = "fsaction";

  public static final Short DEFAULT_PERMISSION = 0755;

  public static final String RENAME_JSON = "boolean";

  public static final String DELETE_JSON = "boolean";

  public static final String MKDIRS_JSON = "boolean";

  public static final String HOME_DIR_JSON = "Path";

  public static final String SET_REPLICATION_JSON = "boolean";

  public static final String UPLOAD_CONTENT_TYPE= "application/octet-stream";

  public static enum FILE_TYPE {
    FILE, DIRECTORY, SYMLINK;

    public static FILE_TYPE getType(FileStatus fileStatus) {
      if (!fileStatus.isDir()) {
        return FILE;
      }
      if (fileStatus.isDir()) {
        return DIRECTORY;
      }
      throw new IllegalArgumentException("Could not determine filetype for: " +
                                         fileStatus.getPath());
    }
  }

  public static final String FILE_STATUSES_JSON = "FileStatuses";
  public static final String FILE_STATUS_JSON = "FileStatus";
  public static final String PATH_SUFFIX_JSON = "pathSuffix";
  public static final String TYPE_JSON = "type";
  public static final String LENGTH_JSON = "length";
  public static final String OWNER_JSON = "owner";
  public static final String GROUP_JSON = "group";
  public static final String PERMISSION_JSON = "permission";
  public static final String ACCESS_TIME_JSON = "accessTime";
  public static final String MODIFICATION_TIME_JSON = "modificationTime";
  public static final String BLOCK_SIZE_JSON = "blockSize";
  public static final String REPLICATION_JSON = "replication";

  public static final String FILE_CHECKSUM_JSON = "FileChecksum";
  public static final String CHECKSUM_ALGORITHM_JSON = "algorithm";
  public static final String CHECKSUM_BYTES_JSON = "bytes";
  public static final String CHECKSUM_LENGTH_JSON = "length";

  public static final String CONTENT_SUMMARY_JSON = "ContentSummary";
  public static final String CONTENT_SUMMARY_DIRECTORY_COUNT_JSON = "directoryCount";
  public static final String CONTENT_SUMMARY_FILE_COUNT_JSON = "fileCount";
  public static final String CONTENT_SUMMARY_LENGTH_JSON = "length";
  public static final String CONTENT_SUMMARY_QUOTA_JSON = "quota";
  public static final String CONTENT_SUMMARY_SPACE_CONSUMED_JSON = "spaceConsumed";
  public static final String CONTENT_SUMMARY_SPACE_QUOTA_JSON = "spaceQuota";

  public static final String DELEGATION_TOKEN_JSON = "Token";
  public static final String DELEGATION_TOKEN_URL_STRING_JSON = "urlString";

  public static final String ERROR_JSON = "RemoteException";
  public static final String ERROR_EXCEPTION_JSON = "exception";
  public static final String ERROR_CLASSNAME_JSON = "javaClassName";
  public static final String ERROR_MESSAGE_JSON = "message";
  public static final String LOCATED_BLOCKS_JSON = "LocatedBlocks";
  public static final String LOCATED_BLOCKS_LOCATIONS_JSON = "locations";
  public static final String LOCATED_BLOCKS_LOCATION_CORRUPT_JSON = "corrupt";
  public static final String LOCATED_BLOCKS_LOCATION_OFFSET_JSON = "offset";
  public static final String LOCATED_BLOCKS_LOCATION_LENGTH_JSON = "length";
  public static final String LOCATED_BLOCKS_LOCATION_HOSTS_JSON = "hosts";
  public static final String LOCATED_BLOCKS_LOCATION_NAMES_JSON = "names";
  public static final String LOCATED_BLOCKS_LOCATION_TOPOLOGYPATHS_JSON =
          "topologyPaths";

  public static final int HTTP_TEMPORARY_REDIRECT = 307;

  private static final String HTTP_GET = "GET";
  private static final String HTTP_PUT = "PUT";
  private static final String HTTP_POST = "POST";
  private static final String HTTP_DELETE = "DELETE";

  public enum Operation {
    CHECKACCESS(HTTP_GET), OPEN(HTTP_GET), GETFILESTATUS(HTTP_GET),
    LISTSTATUS(HTTP_GET), GETHOMEDIRECTORY(HTTP_GET),
    GETCONTENTSUMMARY(HTTP_GET), GETFILECHECKSUM(HTTP_GET),
    GET_BLOCK_LOCATIONS(HTTP_GET), INSTRUMENTATION(HTTP_GET),
    APPEND(HTTP_POST),
    CREATE(HTTP_PUT), MKDIRS(HTTP_PUT), RENAME(HTTP_PUT), SETOWNER(HTTP_PUT),
    SETPERMISSION(HTTP_PUT), SETREPLICATION(HTTP_PUT), SETTIMES(HTTP_PUT),
    DELETE(HTTP_DELETE);

    private String httpMethod;

    Operation(String httpMethod) {
      this.httpMethod = httpMethod;
    }

    public String getMethod() {
      return httpMethod;
    }

  }


  private AuthenticatedURL.Token authToken = new AuthenticatedURL.Token();
  private URI uri;
  private Path workingDir;
  private String doAs;

  /**
   * Convenience method that creates a <code>HttpURLConnection</code> for the
   * HttpFSServer file system operations.
   * <p/>
   * This methods performs and injects any needed authentication credentials
   * via the {@link #getConnection(URL, String)} method
   *
   * @param method the HTTP method.
   * @param params the query string parameters.
   * @param path the file path
   * @param makeQualified if the path should be 'makeQualified'
   *
   * @return a <code>HttpURLConnection</code> for the HttpFSServer server,
   *         authenticated and ready to use for the specified path and file system operation.
   *
   * @throws IOException thrown if an IO error occurrs.
   */
  private HttpURLConnection getConnection(String method, Map<String, String> params,
                                          Path path, boolean makeQualified) throws IOException {
    params.put(DO_AS_PARAM, doAs);
    if (makeQualified) {
      path = makeQualified(path);
    }
    URI uri = path.toUri();
    StringBuilder sb = new StringBuilder();
    sb.append(uri.getScheme()).append("://").append(uri.getAuthority()).
      append(SERVICE_PREFIX).append(uri.getPath());

    String separator = "?";
    for (Map.Entry<String, String> entry : params.entrySet()) {
      sb.append(separator).append(entry.getKey()).append("=").
        append(URLEncoder.encode(entry.getValue(), "UTF8"));
      separator = "&";
    }
    URL url = new URL(sb.toString());
    return getConnection(url, method);
  }

  /**
   * Convenience method that creates a <code>HttpURLConnection</code> for the specified URL.
   * <p/>
   * This methods performs and injects any needed authentication credentials.
   *
   * @param url url to connect to.
   * @param method the HTTP method.
   *
   * @return a <code>HttpURLConnection</code> for the HttpFSServer server, authenticated and ready to use for
   *         the specified path and file system operation.
   *
   * @throws IOException thrown if an IO error occurrs.
   */
  private HttpURLConnection getConnection(URL url, String method) throws IOException {
    Class<? extends Authenticator> klass =
      getConf().getClass("httpfs.authenticator.class", HttpKerberosAuthenticator.class, Authenticator.class);
    Authenticator authenticator = ReflectionUtils.newInstance(klass, getConf());
    try {
      HttpURLConnection conn = new AuthenticatedURL(authenticator).openConnection(url, authToken);
      conn.setRequestMethod(method);
      if (method.equals(HTTP_POST) || method.equals(HTTP_PUT)) {
        conn.setDoOutput(true);
      }
      return conn;
    } catch (Exception ex) {
      throw new IOException(ex);
    }
  }

  /**
   * Convenience method that JSON Parses the <code>InputStream</code> of a <code>HttpURLConnection</code>.
   *
   * @param conn the <code>HttpURLConnection</code>.
   *
   * @return the parsed JSON object.
   *
   * @throws IOException thrown if the <code>InputStream</code> could not be JSON parsed.
   */
  private static Object jsonParse(HttpURLConnection conn) throws IOException {
    try {
      JSONParser parser = new JSONParser();
      return parser.parse(new InputStreamReader(conn.getInputStream()));
    } catch (ParseException ex) {
      throw new IOException("JSON parser error, " + ex.getMessage(), ex);
    }
  }

  /**
   * Validates the status of an <code>HttpURLConnection</code> against an expected HTTP
   * status code. If the current status code is not the expected one it throws an exception
   * with a detail message using Server side error messages if available.
   *
   * @param conn the <code>HttpURLConnection</code>.
   * @param expected the expected HTTP status code.
   *
   * @throws IOException thrown if the current status code does not match the expected one.
   */
  private static void validateResponse(HttpURLConnection conn, int expected) throws IOException {
    int status = conn.getResponseCode();
    if (status != expected) {
      try {
        JSONObject json = (JSONObject) jsonParse(conn);
        json = (JSONObject) json.get(ERROR_JSON);
        String message = (String) json.get(ERROR_MESSAGE_JSON);
        String exception = (String) json.get(ERROR_EXCEPTION_JSON);
        String className = (String) json.get(ERROR_CLASSNAME_JSON);

        try {
          ClassLoader cl = HttpFSFileSystem.class.getClassLoader();
          Class klass = cl.loadClass(className);
          Constructor constr = klass.getConstructor(String.class);
          throw (IOException) constr.newInstance(message);
        } catch (IOException ex) {
          throw ex;
        } catch (Exception ex) {
          throw new IOException(MessageFormat.format("{0} - {1}", exception, message));
        }
      } catch (IOException ex) {
        if (ex.getCause() instanceof IOException) {
          throw (IOException) ex.getCause();
        }
        throw new IOException(MessageFormat.format("HTTP status [{0}], {1}", status, conn.getResponseMessage()));
      }
    }
  }

  /**
   * Called after a new FileSystem instance is constructed.
   *
   * @param name a uri whose authority section names the host, port, etc. for this FileSystem
   * @param conf the configuration
   */
  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    doAs = ugi.getUserName();
    super.initialize(name, conf);
    try {
      uri = new URI(name.getScheme() + "://" + name.getHost() + ":" + name.getPort());
    } catch (URISyntaxException ex) {
      throw new IOException(ex);
    }
  }

  /**
   * Returns a URI whose scheme and authority identify this FileSystem.
   *
   * @return the URI whose scheme and authority identify this FileSystem.
   */
  @Override
  public URI getUri() {
    return uri;
  }

  /**
   * HttpFSServer subclass of the <code>FSDataInputStream</code>.
   * <p/>
   * This implementation does not support the
   * <code>PositionReadable</code> and <code>Seekable</code> methods.
   */
  private static class HttpFSDataInputStream extends FilterInputStream implements Seekable, PositionedReadable {

    protected HttpFSDataInputStream(InputStream in, int bufferSize) {
      super(new BufferedInputStream(in, bufferSize));
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void seek(long pos) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getPos() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Opens an FSDataInputStream at the indicated Path.
   * </p>
   * IMPORTANT: the returned <code><FSDataInputStream/code> does not support the
   * <code>PositionReadable</code> and <code>Seekable</code> methods.
   *
   * @param f the file name to open
   * @param bufferSize the size of the buffer to be used.
   */
  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM, Operation.OPEN.toString());
    HttpURLConnection conn = getConnection(Operation.OPEN.getMethod(), params,
                                           f, true);
    validateResponse(conn, HttpURLConnection.HTTP_OK);
    return new FSDataInputStream(
      new HttpFSDataInputStream(conn.getInputStream(), bufferSize));
  }

  /**
   * HttpFSServer subclass of the <code>FSDataOutputStream</code>.
   * <p/>
   * This implementation closes the underlying HTTP connection validating the Http connection status
   * at closing time.
   */
  private static class HttpFSDataOutputStream extends FSDataOutputStream {
    private HttpURLConnection conn;
    private int closeStatus;

    public HttpFSDataOutputStream(HttpURLConnection conn, OutputStream out, int closeStatus, Statistics stats)
      throws IOException {
      super(out, stats);
      this.conn = conn;
      this.closeStatus = closeStatus;
    }

    @Override
    public void close() throws IOException {
      try {
        super.close();
      } finally {
        validateResponse(conn, closeStatus);
      }
    }

  }

  /**
   * Converts a <code>FsPermission</code> to a Unix octal representation.
   *
   * @param p the permission.
   *
   * @return the Unix string symbolic reprentation.
   */
  public static String permissionToString(FsPermission p) {
    return  Integer.toString((p == null) ? DEFAULT_PERMISSION : p.toShort(), 8);
  }

  /*
   * Common handling for uploading data for create and append operations.
   */
  private FSDataOutputStream uploadData(String method, Path f, Map<String, String> params,
                                        int bufferSize, int expectedStatus) throws IOException {
    HttpURLConnection conn = getConnection(method, params, f, true);
    conn.setInstanceFollowRedirects(false);
    boolean exceptionAlreadyHandled = false;
    try {
      if (conn.getResponseCode() == HTTP_TEMPORARY_REDIRECT) {
        exceptionAlreadyHandled = true;
        String location = conn.getHeaderField("Location");
        if (location != null) {
          conn = getConnection(new URL(location), method);
          conn.setRequestProperty("Content-Type", UPLOAD_CONTENT_TYPE);
          try {
            OutputStream os = new BufferedOutputStream(conn.getOutputStream(), bufferSize);
            return new HttpFSDataOutputStream(conn, os, expectedStatus, statistics);
          } catch (IOException ex) {
            validateResponse(conn, expectedStatus);
            throw ex;
          }
        } else {
          validateResponse(conn, HTTP_TEMPORARY_REDIRECT);
          throw new IOException("Missing HTTP 'Location' header for [" + conn.getURL() + "]");
        }
      } else {
        throw new IOException(
          MessageFormat.format("Expected HTTP status was [307], received [{0}]",
                               conn.getResponseCode()));
      }
    } catch (IOException ex) {
      if (exceptionAlreadyHandled) {
        throw ex;
      } else {
        validateResponse(conn, HTTP_TEMPORARY_REDIRECT);
        throw ex;
      }
    }
  }


  /**
   * Opens an FSDataOutputStream at the indicated Path with write-progress
   * reporting.
   * <p/>
   * IMPORTANT: The <code>Progressable</code> parameter is not used.
   *
   * @param f the file name to open.
   * @param permission file permission.
   * @param overwrite if a file with this name already exists, then if true,
   * the file will be overwritten, and if false an error will be thrown.
   * @param bufferSize the size of the buffer to be used.
   * @param replication required block replication for the file.
   * @param blockSize block size.
   * @param progress progressable.
   *
   * @throws IOException
   * @see #setPermission(Path, FsPermission)
   */
  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
                                   boolean overwrite, int bufferSize,
                                   short replication, long blockSize,
                                   Progressable progress) throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM, Operation.CREATE.toString());
    params.put(OVERWRITE_PARAM, Boolean.toString(overwrite));
    params.put(REPLICATION_PARAM, Short.toString(replication));
    params.put(BLOCKSIZE_PARAM, Long.toString(blockSize));
    params.put(PERMISSION_PARAM, permissionToString(permission));
    return uploadData(Operation.CREATE.getMethod(), f, params, bufferSize,
                      HttpURLConnection.HTTP_CREATED);
  }


  /**
   * Append to an existing file (optional operation).
   * <p/>
   * IMPORTANT: The <code>Progressable</code> parameter is not used.
   *
   * @param f the existing file to be appended.
   * @param bufferSize the size of the buffer to be used.
   * @param progress for reporting progress if it is not null.
   *
   * @throws IOException
   */
  @Override
  public FSDataOutputStream append(Path f, int bufferSize,
                                   Progressable progress) throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM, Operation.APPEND.toString());
    return uploadData(Operation.APPEND.getMethod(), f, params, bufferSize,
                      HttpURLConnection.HTTP_OK);
  }

  /**
   * Renames Path src to Path dst.  Can take place on local fs
   * or remote DFS.
   */
  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM, Operation.RENAME.toString());
    params.put(DESTINATION_PARAM, dst.toString());
    HttpURLConnection conn = getConnection(Operation.RENAME.getMethod(),
                                           params, src, true);
    validateResponse(conn, HttpURLConnection.HTTP_OK);
    JSONObject json = (JSONObject) jsonParse(conn);
    return (Boolean) json.get(RENAME_JSON);
  }

  /**
   * Return an array containing hostnames, offset and size of portions of the
   * given file. For a nonexistent file or regions, null will be returned.
   *
   * This call is most helpful with DFS, where it returns hostnames of machines
   * that contain the given file.
   *
   * @param file FilesStatus to get data from
   * @param start offset into the given file
   * @param len length for which to get locations for
   * @return the block location array
   */
  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file, long start,
                                               long len) throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM, Operation.GET_BLOCK_LOCATIONS.toString());
    params.put(OFFSET_PARAM, Long.toString(start));
    params.put(LENGTH_PARAM, Long.toString(len));
    HttpURLConnection conn =
            getConnection(Operation.GET_BLOCK_LOCATIONS.getMethod(), params,
                    file.getPath(), true);
    HttpExceptionUtils.validateResponse(conn, HttpURLConnection.HTTP_OK);
    JSONObject json = (JSONObject) jsonParse(conn);
    json = (JSONObject) json.get(LOCATED_BLOCKS_JSON);
    JSONArray jsonArray = (JSONArray) json.get(LOCATED_BLOCKS_LOCATIONS_JSON);
    BlockLocation[] locations = new BlockLocation[jsonArray.size()];
    for (int i = 0; i < jsonArray.size(); i++) {
      locations[i] = createBlockLocation((JSONObject) jsonArray.get(i));
    }
    return locations;
  }

  private BlockLocation createBlockLocation(JSONObject json) throws IOException {
    BlockLocation location = new BlockLocation();
    location.setCorrupt((Boolean) json
            .get(LOCATED_BLOCKS_LOCATION_CORRUPT_JSON));
    location.setOffset((Long) json.get(LOCATED_BLOCKS_LOCATION_OFFSET_JSON));
    location.setLength((Long) json.get(LOCATED_BLOCKS_LOCATION_LENGTH_JSON));

    JSONArray array = (JSONArray) json.get(LOCATED_BLOCKS_LOCATION_HOSTS_JSON);
    String[] hosts = new String[array.size()];
    for (int i = 0; i < array.size(); i++) {
      hosts[i] = (String) array.get(i);
    }
    location.setHosts(hosts);
    array = (JSONArray) json.get(LOCATED_BLOCKS_LOCATION_NAMES_JSON);
    String[] names = new String[array.size()];
    for (int i = 0; i < array.size(); i++) {
      names[i] = (String) array.get(i);
    }
    location.setNames(names);

    array = (JSONArray) json.get(LOCATED_BLOCKS_LOCATION_TOPOLOGYPATHS_JSON);
    String[] topologyPaths = new String[array.size()];
    for (int i = 0; i < array.size(); i++) {
      topologyPaths[i] = (String) array.get(i);
    }
    location.setTopologyPaths(topologyPaths);
    return location;
  }

  /**
   * Delete a file.
   *
   * @deprecated Use delete(Path, boolean) instead
   */
  @SuppressWarnings({"deprecation"})
  @Deprecated
  @Override
  public boolean delete(Path f) throws IOException {
    return delete(f, false);
  }

  /**
   * Delete a file.
   *
   * @param f the path to delete.
   * @param recursive if path is a directory and set to
   * true, the directory is deleted else throws an exception. In
   * case of a file the recursive can be set to either true or false.
   *
   * @return true if delete is successful else false.
   *
   * @throws IOException
   */
  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM, Operation.DELETE.toString());
    params.put(RECURSIVE_PARAM, Boolean.toString(recursive));
    HttpURLConnection conn = getConnection(Operation.DELETE.getMethod(),
                                           params, f, true);
    validateResponse(conn, HttpURLConnection.HTTP_OK);
    JSONObject json = (JSONObject) jsonParse(conn);
    return (Boolean) json.get(DELETE_JSON);
  }

  /**
   * List the statuses of the files/directories in the given path if the path is
   * a directory.
   *
   * @param f given path
   *
   * @return the statuses of the files/directories in the given patch
   *
   * @throws IOException
   */
  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM, Operation.LISTSTATUS.toString());
    HttpURLConnection conn = getConnection(Operation.LISTSTATUS.getMethod(),
                                           params, f, true);
    validateResponse(conn, HttpURLConnection.HTTP_OK);
    JSONObject json = (JSONObject) jsonParse(conn);
    json = (JSONObject) json.get(FILE_STATUSES_JSON);
    JSONArray jsonArray = (JSONArray) json.get(FILE_STATUS_JSON);
    FileStatus[] array = new FileStatus[jsonArray.size()];
    f = makeQualified(f);
    for (int i = 0; i < jsonArray.size(); i++) {
      array[i] = createFileStatus(f, (JSONObject) jsonArray.get(i));
    }
    return array;
  }

  /**
   * Set the current working directory for the given file system. All relative
   * paths will be resolved relative to it.
   *
   * @param newDir new directory.
   */
  @Override
  public void setWorkingDirectory(Path newDir) {
    workingDir = newDir;
  }

  /**
   * Get the current working directory for the given file system
   *
   * @return the directory pathname
   */
  @Override
  public Path getWorkingDirectory() {
    if (workingDir == null) {
      workingDir = getHomeDirectory();
    }
    return workingDir;
  }

  /**
   * Make the given file and all non-existent parents into
   * directories. Has the semantics of Unix 'mkdir -p'.
   * Existence of the directory hierarchy is not an error.
   */
  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM, Operation.MKDIRS.toString());
    params.put(PERMISSION_PARAM, permissionToString(permission));
    HttpURLConnection conn = getConnection(Operation.MKDIRS.getMethod(),
                                           params, f, true);
    validateResponse(conn, HttpURLConnection.HTTP_OK);
    JSONObject json = (JSONObject) jsonParse(conn);
    return (Boolean) json.get(MKDIRS_JSON);
  }

  /**
   * Return a file status object that represents the path.
   *
   * @param f The path we want information from
   *
   * @return a FileStatus object
   *
   * @throws FileNotFoundException when the path does not exist;
   * IOException see specific implementation
   */
  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM, Operation.GETFILESTATUS.toString());
    HttpURLConnection conn = getConnection(Operation.GETFILESTATUS.getMethod(),
                                           params, f, true);
    validateResponse(conn, HttpURLConnection.HTTP_OK);
    JSONObject json = (JSONObject) jsonParse(conn);
    json = (JSONObject) json.get(FILE_STATUS_JSON);
    f = makeQualified(f);
    return createFileStatus(f, json);
  }

  /**
   * Return the current user's home directory in this filesystem.
   * The default implementation returns "/user/$USER/".
   */
  @Override
  public Path getHomeDirectory() {
    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM, Operation.GETHOMEDIRECTORY.toString());
    try {
      HttpURLConnection conn =
        getConnection(Operation.GETHOMEDIRECTORY.getMethod(), params,
                      new Path(getUri().toString(), "/"), false);
      validateResponse(conn, HttpURLConnection.HTTP_OK);
      JSONObject json = (JSONObject) jsonParse(conn);
      return new Path((String) json.get(HOME_DIR_JSON));
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Set owner of a path (i.e. a file or a directory).
   * The parameters username and groupname cannot both be null.
   *
   * @param p The path
   * @param username If it is null, the original username remains unchanged.
   * @param groupname If it is null, the original groupname remains unchanged.
   */
  @Override
  public void setOwner(Path p, String username, String groupname)
    throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM, Operation.SETOWNER.toString());
    params.put(OWNER_PARAM, username);
    params.put(GROUP_PARAM, groupname);
    HttpURLConnection conn = getConnection(Operation.SETOWNER.getMethod(),
                                           params, p, true);
    validateResponse(conn, HttpURLConnection.HTTP_OK);
  }

  /**
   * Set permission of a path.
   *
   * @param p path.
   * @param permission permission.
   */
  @Override
  public void setPermission(Path p, FsPermission permission) throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM, Operation.SETPERMISSION.toString());
    params.put(PERMISSION_PARAM, permissionToString(permission));
    HttpURLConnection conn = getConnection(Operation.SETPERMISSION.getMethod(), params, p, true);
    validateResponse(conn, HttpURLConnection.HTTP_OK);
  }

  /**
   * Set access time of a file
   *
   * @param p The path
   * @param mtime Set the modification time of this file.
   * The number of milliseconds since Jan 1, 1970.
   * A value of -1 means that this call should not set modification time.
   * @param atime Set the access time of this file.
   * The number of milliseconds since Jan 1, 1970.
   * A value of -1 means that this call should not set access time.
   */
  @Override
  public void setTimes(Path p, long mtime, long atime) throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM, Operation.SETTIMES.toString());
    params.put(MODIFICATION_TIME_PARAM, Long.toString(mtime));
    params.put(ACCESS_TIME_PARAM, Long.toString(atime));
    HttpURLConnection conn = getConnection(Operation.SETTIMES.getMethod(),
                                           params, p, true);
    validateResponse(conn, HttpURLConnection.HTTP_OK);
  }

  /**
   * Set replication for an existing file.
   *
   * @param src file name
   * @param replication new replication
   *
   * @return true if successful;
   *         false if file does not exist or is a directory
   *
   * @throws IOException
   */
  @Override
  public boolean setReplication(Path src, short replication)
    throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM, Operation.SETREPLICATION.toString());
    params.put(REPLICATION_PARAM, Short.toString(replication));
    HttpURLConnection conn =
      getConnection(Operation.SETREPLICATION.getMethod(), params, src, true);
    validateResponse(conn, HttpURLConnection.HTTP_OK);
    JSONObject json = (JSONObject) jsonParse(conn);
    return (Boolean) json.get(SET_REPLICATION_JSON);
  }

  /**
   * Creates a <code>FileStatus</code> object using a JSON file-status payload
   * received from a HttpFSServer server.
   *
   * @param json a JSON file-status payload received from a HttpFSServer server
   *
   * @return the corresponding <code>FileStatus</code>
   */
  private FileStatus createFileStatus(Path parent, JSONObject json) {
    String pathSuffix = (String) json.get(PATH_SUFFIX_JSON);
    Path path = (pathSuffix.equals("")) ? parent : new Path(parent, pathSuffix);
    FILE_TYPE type = FILE_TYPE.valueOf((String) json.get(TYPE_JSON));
    long len = (Long) json.get(LENGTH_JSON);
    String owner = (String) json.get(OWNER_JSON);
    String group = (String) json.get(GROUP_JSON);
    FsPermission permission =
      new FsPermission(Short.parseShort((String) json.get(PERMISSION_JSON), 8));
    long aTime = (Long) json.get(ACCESS_TIME_JSON);
    long mTime = (Long) json.get(MODIFICATION_TIME_JSON);
    long blockSize = (Long) json.get(BLOCK_SIZE_JSON);
    short replication = ((Long) json.get(REPLICATION_JSON)).shortValue();
    FileStatus fileStatus = null;

    switch (type) {
      case FILE:
      case DIRECTORY:
        fileStatus = new FileStatus(len, (type == FILE_TYPE.DIRECTORY),
                                    replication, blockSize, mTime, aTime,
                                    permission, owner, group, path);
        break;
      case SYMLINK:
        throw new IllegalArgumentException("SYMLINKs are not supported in cdh3 : " +
                                           fileStatus.getPath());
    }
    return fileStatus;
  }

  /**
   * Checks if the user can access a path.  The mode specifies which access
   * checks to perform.  If the requested permissions are granted, then the
   * method returns normally.  If access is denied, then the method throws an
   * {@link AccessControlException}.
   * <p/>
   * The default implementation of this method calls {@link #getFileStatus(Path)}
   * and checks the returned permissions against the requested permissions.
   * Note that the getFileStatus call will be subject to authorization checks.
   * Typically, this requires search (execute) permissions on each directory in
   * the path's prefix, but this is implementation-defined.  Any file system
   * that provides a richer authorization model (such as ACLs) may override the
   * default implementation so that it checks against that model instead.
   * <p>
   * In general, applications should avoid using this method, due to the risk of
   * time-of-check/time-of-use race conditions.  The permissions on a file may
   * change immediately after the access call returns.  Most applications should
   * prefer running specific file system actions as the desired user represented
   * by a {@link UserGroupInformation}.
   *
   * @param path Path to check
   * @param mode type of access to check
   * @throws AccessControlException if access is denied
   * @throws FileNotFoundException if the path does not exist
   * @throws IOException see specific implementation
   */
  @Override
  public void access(Path path, FsAction mode) throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM, Operation.CHECKACCESS.toString());
    params.put(FSACTION_PARAM, mode.toString());
    HttpURLConnection conn = getConnection(Operation.CHECKACCESS.getMethod(),
            params, path, true);
    validateResponse(conn,HttpURLConnection.HTTP_OK);

  }

  @Override
  public ContentSummary getContentSummary(Path f) throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM, Operation.GETCONTENTSUMMARY.toString());
    HttpURLConnection conn =
      getConnection(Operation.GETCONTENTSUMMARY.getMethod(), params, f, true);
    validateResponse(conn, HttpURLConnection.HTTP_OK);
    JSONObject json =
      (JSONObject) ((JSONObject) jsonParse(conn)).get(CONTENT_SUMMARY_JSON);
    return new ContentSummary((Long) json.get(CONTENT_SUMMARY_LENGTH_JSON),
                              (Long) json.get(CONTENT_SUMMARY_FILE_COUNT_JSON),
                              (Long) json.get(CONTENT_SUMMARY_DIRECTORY_COUNT_JSON),
                              (Long) json.get(CONTENT_SUMMARY_QUOTA_JSON),
                              (Long) json.get(CONTENT_SUMMARY_SPACE_CONSUMED_JSON),
                              (Long) json.get(CONTENT_SUMMARY_SPACE_QUOTA_JSON)
    );
  }

  @Override
  public FileChecksum getFileChecksum(Path f) throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM, Operation.GETFILECHECKSUM.toString());
    HttpURLConnection conn =
      getConnection(Operation.GETFILECHECKSUM.getMethod(), params, f, true);
    validateResponse(conn, HttpURLConnection.HTTP_OK);
    final JSONObject json =
      (JSONObject) ((JSONObject) jsonParse(conn)).get(FILE_CHECKSUM_JSON);
    return new FileChecksum() {
      @Override
      public String getAlgorithmName() {
        return (String) json.get(CHECKSUM_ALGORITHM_JSON);
      }

      @Override
      public int getLength() {
        return ((Long) json.get(CHECKSUM_LENGTH_JSON)).intValue();
      }

      @Override
      public byte[] getBytes() {
        return StringUtils.hexStringToByte((String) json.get(CHECKSUM_BYTES_JSON));
      }

      @Override
      public void write(DataOutput out) throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public void readFields(DataInput in) throws IOException {
        throw new UnsupportedOperationException();
      }
    };
  }

}
