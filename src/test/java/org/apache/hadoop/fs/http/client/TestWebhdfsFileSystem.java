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

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.test.TestJettyHelper;
import org.junit.Assert;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(value = Parameterized.class)
public class TestWebhdfsFileSystem extends TestHttpFSFileSystem {

  public TestWebhdfsFileSystem(TestHttpFSFileSystem.Operation operation) {
    super(operation);
  }

/* TODO: remove this when webhdfs is backported to cdh3u4, webhdfs:// will then be avail
  @Override
  protected Class getFileSystemClass() {
    return WebHdfsFileSystem.class;
  }
*/
}
