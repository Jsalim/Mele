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

package com.nearinfinity.mele;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/** @author Aaron McCurry (amccurry@nearinfinity.com) */
public class MeleConfiguration extends Properties implements MeleConstants {
    
    private static final long serialVersionUID = 6054100377853038915L;
    private MeleDirectoryFactory directoryFactory;
    
    public MeleConfiguration() throws IOException {
        this.load(getClass().getResourceAsStream("/mele-default.properties"));
        InputStream site = getClass().getResourceAsStream("/mele-site.properties");
        if (site != null) {
            this.load(site);
        }
    }
    
    public String getZooKeeperConnectionString() {
        return getProperty(MELE_ZOOKEEPER_CONNECTION);
    }

    public void setZooKeeperConnectionString(String zooKeeperConnectionString) {
        setProperty(MELE_ZOOKEEPER_CONNECTION, zooKeeperConnectionString);
    }

    public int getZooKeeperSessionTimeout() {
        return getPropertyInt(MELE_ZOOKEEPER_SESSION_TIMEOUT, DEFAULT_ZOOKEEPER_SESSION_TIMEOUT);
    }

    public void setZooKeeperSessionTimeout(int zooKeeperSessionTimeout) {
        setPropertyInt(MELE_ZOOKEEPER_SESSION_TIMEOUT, zooKeeperSessionTimeout);
    }

    public String getBaseZooKeeperPath() {
        return getProperty(MELE_BASE_ZOOKEEPER_PATH);
    }

    public void setBaseZooKeeperPath(String baseZooKeeperPath) {
        setProperty(MELE_BASE_ZOOKEEPER_PATH, baseZooKeeperPath);
    }

    public List<String> getLocalReplicationPathList() {
        String paths = getProperty(MELE_LOCAL_REPLICATION_PATHS);
        return Arrays.asList(paths.split(","));
    }

    public void setLocalReplicationPathList(List<String> localReplicationPathList) {
        StringBuilder paths = new StringBuilder();
        for (String s : localReplicationPathList) {
            if (paths.length() != 0) {
                paths.append(',');
            }
            paths.append(s);
        }
        setProperty(MELE_LOCAL_REPLICATION_PATHS, paths.toString());
    }

    public String getBaseHdfsPath() {
        return getProperty(MELE_BASE_HDFS_PATH);
    }

    public void setBaseHdfsPath(String baseHdfsPath) {
        setProperty(MELE_BASE_HDFS_PATH, baseHdfsPath);
    }

    public boolean isReplicatedLocally() {
        return true;
    }

    public String getCassandraKeySpace() {
        return getProperty(MELE_CASSANDRA_KEYSPACE);
    }

    public String getCassandraColumnFamily() {
        return getProperty(MELE_CASSANDRA_COLUMNFAMILY);
    }

    public int getCassandraPoolSize() {
        return getPropertyInt(MELE_CASSANDRA_POOLSIZE,10);
    }

    public String getCassandraHostName() {
        return getProperty(MELE_CASSANDRA_HOSTNAME);
    }

    public int getCassandraPort() {
        return getPropertyInt(MELE_CASSANDRA_PORT,10);
    }
    
    public MeleDirectoryFactory getDirectoryFactory() {
        return directoryFactory;
    }

    public void setDirectoryFactory(MeleDirectoryFactory directoryFactory) {
        this.directoryFactory = directoryFactory;
    }
    
    private int getPropertyInt(String name, int i) {
        String property = getProperty(name);
        if (property == null) {
            return i;
        }
        return Integer.parseInt(property);
    }
    
    private void setPropertyInt(String name, int i) {
        setProperty(name, Integer.toString(i));
    }
}
