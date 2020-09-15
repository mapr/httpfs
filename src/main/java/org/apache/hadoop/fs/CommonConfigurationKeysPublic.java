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
package org.apache.hadoop.fs;

/**
 * Minimal backport from Hadoop 2.
 */
public class CommonConfigurationKeysPublic {

  public static final String FS_DEFAULT_NAME_KEY = "fs.default.name";
  public static final String HADOOP_WEBAPPS_CUSTOM_HEADERS_PATH = "hadoop.webapps.custom.headers.path";

  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String HADOOP_SECURITY_SENSITIVE_CONFIG_KEYS =
          "hadoop.security.sensitive-config-keys";
  public static final String HADOOP_SECURITY_SENSITIVE_CONFIG_KEYS_DEFAULT =
          String.join(",",
                  "secret$",
                  "password$",
                  "ssl.keystore.pass$",
                  "fs.s3.*[Ss]ecret.?[Kk]ey",
                  "fs.s3a.*.server-side-encryption.key",
                  "fs.azure\\.account.key.*",
                  "credential$",
                  "oauth.*token$",
                  HADOOP_SECURITY_SENSITIVE_CONFIG_KEYS);

  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String HADOOP_SECURITY_AUTHORIZATION =
          "hadoop.security.authorization";
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String HADOOP_SECURITY_INSTRUMENTATION_REQUIRES_ADMIN =
          "hadoop.security.instrumentation.requires.admin";
}
