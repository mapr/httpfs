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

import org.apache.hadoop.fs.http.client.HttpFSFileSystem;
import org.apache.hadoop.fs.http.client.HttpFSFileSystem.Operation;
import org.apache.hadoop.lib.wsrs.BooleanParam;
import org.apache.hadoop.lib.wsrs.EnumParam;
import org.apache.hadoop.lib.wsrs.LongParam;
import org.apache.hadoop.lib.wsrs.Param;
import org.apache.hadoop.lib.wsrs.ParametersProvider;
import org.apache.hadoop.lib.wsrs.ShortParam;
import org.apache.hadoop.lib.wsrs.StringParam;

import javax.ws.rs.ext.Provider;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * HttpFS ParametersProvider.
 */
@Provider
public class HttpFSParametersProvider extends ParametersProvider {

  private static final Map<Enum, Class<Param<?>>[]> PARAMS_DEF =
    new HashMap<Enum, Class<Param<?>>[]>();

  static {
    PARAMS_DEF.put(Operation.OPEN,
      new Class[]{OffsetParam.class, LenParam.class});
    PARAMS_DEF.put(Operation.GETFILESTATUS, new Class[]{});
    PARAMS_DEF.put(Operation.LISTSTATUS, new Class[]{FilterParam.class});
    PARAMS_DEF.put(Operation.GETHOMEDIRECTORY, new Class[]{});
    PARAMS_DEF.put(Operation.GETCONTENTSUMMARY, new Class[]{});
    PARAMS_DEF.put(Operation.GETFILECHECKSUM, new Class[]{});
    PARAMS_DEF.put(Operation.GET_BLOCK_LOCATIONS, new Class[]{});
    PARAMS_DEF.put(Operation.INSTRUMENTATION, new Class[]{});
    PARAMS_DEF.put(Operation.APPEND, new Class[]{DataParam.class});
    PARAMS_DEF.put(Operation.CREATE,
      new Class[]{PermissionParam.class, OverwriteParam.class,
                  ReplicationParam.class, BlockSizeParam.class, DataParam.class});
    PARAMS_DEF.put(Operation.MKDIRS, new Class[]{PermissionParam.class});
    PARAMS_DEF.put(Operation.RENAME, new Class[]{DestinationParam.class});
    PARAMS_DEF.put(Operation.SETOWNER,
        new Class[]{OwnerParam.class, GroupParam.class});
    PARAMS_DEF.put(Operation.SETPERMISSION, new Class[]{PermissionParam.class});
    PARAMS_DEF.put(Operation.SETREPLICATION,
        new Class[]{ReplicationParam.class});
    PARAMS_DEF.put(Operation.SETTIMES,
      new Class[]{ModifiedTimeParam.class, AccessTimeParam.class});
    PARAMS_DEF.put(Operation.DELETE, new Class[]{RecursiveParam.class});
    PARAMS_DEF.put(Operation.CHECKACCESS,new Class[]{FSActionParam.class});
  }

  public HttpFSParametersProvider() {
    super(HttpFSFileSystem.OP_PARAM, HttpFSFileSystem.Operation.class,
          PARAMS_DEF);
  }

  /**
   * Class for access-time parameter.
   */
  public static class AccessTimeParam extends LongParam {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.ACCESS_TIME_PARAM;
    /**
     * Constructor.
     */
    public AccessTimeParam() {
      super(NAME, -1l);
    }
  }

  public static class FSActionParam extends StringParam {

    public static final String NAME = HttpFSFileSystem.FSACTION_PARAM;

    public FSActionParam() { super(NAME, null);}
  }

  /**
   * Class for block-size parameter.
   */
  public static class BlockSizeParam extends LongParam {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.BLOCKSIZE_PARAM;

    /**
     * Constructor.
     */
    public BlockSizeParam() {
      super(NAME, -1l);
    }
  }

  /**
   * Class for data parameter.
   */
  public static class DataParam extends BooleanParam {

    /**
     * Parameter name.
     */
    public static final String NAME = "data";

    /**
     * Constructor.
     */
    public DataParam() {
      super(NAME, false);
    }
  }

  /**
   * Class for operation parameter.
   */
  public static class OperationParam extends EnumParam<HttpFSFileSystem.Operation> {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.OP_PARAM;
    /**
     * Constructor.
     */
    public OperationParam(String operation) {
      super(NAME, HttpFSFileSystem.Operation.class,
            HttpFSFileSystem.Operation.valueOf(operation.toUpperCase()));
    }
  }

  /**
   * Class for delete's recursive parameter.
   */
  public static class RecursiveParam extends BooleanParam {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.RECURSIVE_PARAM;

    /**
     * Constructor.
     */
    public RecursiveParam() {
      super(NAME, false);
    }
  }


  /**
   * Class for filter parameter.
   */
  public static class FilterParam extends StringParam {

    /**
     * Parameter name.
     */
    public static final String NAME = "filter";

    /**
     * Constructor.
     */
    public FilterParam() {
      super(NAME, null);
    }

  }

  /**
   * Class for group parameter.
   */
  public static class GroupParam extends StringParam {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.GROUP_PARAM;

    /**
     * Constructor.
     */
    public GroupParam() {
      super(NAME, null);
    }

  }

  /**
   * Class for len parameter.
   */
  public static class LenParam extends LongParam {

    /**
     * Parameter name.
     */
    public static final String NAME = "length";

    /**
     * Old parameter name, saved for backward compatibility.
     */
    @Deprecated
    private static final String OLD_NAME = "len";

    public static String getOldName() {
      return OLD_NAME;
    }

    /**
     * Constructor.
     */
    public LenParam() {
      super(NAME, -1l);
    }
  }

  /**
   * Class for modified-time parameter.
   */
  public static class ModifiedTimeParam extends LongParam {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.MODIFICATION_TIME_PARAM;

    /**
     * Constructor.
     */
    public ModifiedTimeParam() {
      super(NAME, -1l);
    }
  }

  /**
   * Class for offset parameter.
   */
  public static class OffsetParam extends LongParam {

    /**
     * Parameter name.
     */
    public static final String NAME = "offset";

    /**
     * Constructor.
     */
    public OffsetParam() {
      super(NAME, 0l);
    }
  }

  /**
   * Class for overwrite parameter.
   */
  public static class OverwriteParam extends BooleanParam {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.OVERWRITE_PARAM;

    /**
     * Constructor.
     */
    public OverwriteParam() {
      super(NAME, true);
    }
  }

  /**
   * Class for owner parameter.
   */
  public static class OwnerParam extends StringParam {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.OWNER_PARAM;

    /**
     * Constructor.
     */
    public OwnerParam() {
      super(NAME, null);
    }

  }

  /**
   * Class for permission parameter.
   */
  public static class PermissionParam extends ShortParam {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.PERMISSION_PARAM;


    /**
     * Constructor.
     */
    public PermissionParam() {
      super(NAME, HttpFSFileSystem.DEFAULT_PERMISSION, 8);
    }

  }

  /**
   * Class for replication parameter.
   */
  public static class ReplicationParam extends ShortParam {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.REPLICATION_PARAM;

    /**
     * Constructor.
     */
    public ReplicationParam() {
      super(NAME, (short) -1);
    }
  }

  /**
   * Class for to-path parameter.
   */
  public static class DestinationParam extends StringParam {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.DESTINATION_PARAM;

    /**
     * Constructor.
     */
    public DestinationParam() {
      super(NAME, null);
    }
  }
}
