/*
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

package org.apache.hadoop.fs.s3a;

import static org.apache.hadoop.fs.s3a.Constants.SERVER_SIDE_ENCRYPTION_KEY;

import java.io.IOException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;

/**
 * Utility methods for S3A code.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class S3AUtils {

  /** Reuse the S3AFileSystem log. */
  private static final Logger LOG = S3AFileSystem.LOG;

  /**
   * Get a password from a configuration, or, if a value is passed in,
   * pick that up instead.
   *
   * @param conf configuration
   * @param key key to look up
   * @param val current value: if non empty this is used instead of querying the configuration.
   * @return a password or "".
   * @throws IOException on any problem
   */
  static String getPassword(Configuration conf, String key, String val)
      throws IOException {
    return StringUtils.isEmpty(val)
        ? lookupPassword(conf, key, "")
        : val;
  }

  /**
   * Get a password from a configuration/configured credential providers.
   *
   * @param conf configuration
   * @param key key to look up
   * @param defVal value to return if there is no password
   * @return a password or the value in {@code defVal}
   * @throws IOException on any problem
   */
  static String lookupPassword(Configuration conf, String key, String defVal)
      throws IOException {
    try {
      final char[] pass = conf.getPassword(key);
      return pass != null ?
          new String(pass).trim()
          : defVal;
    } catch (IOException ioe) {
      throw new IOException("Cannot find password option " + key, ioe);
    }
  }

  static String getServerSideEncryptionKey(Configuration conf) {
    try {
      return getPassword(conf, Constants.SERVER_SIDE_ENCRYPTION_KEY,
          conf.getTrimmed(SERVER_SIDE_ENCRYPTION_KEY));
    } catch (IOException e) {
      LOG.error("Cannot retrieve SERVER_SIDE_ENCRYPTION_KEY", e);
    }
    return null;
  }

}
