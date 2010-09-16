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
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.zookeeper.Watcher;

import com.nearinfinity.mele.store.zookeeper.NoOpWatcher;

/** @author Aaron McCurry (amccurry@nearinfinity.com) */
public class MeleConfiguration extends Configuration implements MeleConstants {
    
    private FileSystem hdfsFileSystem;
    private Watcher watcher = new NoOpWatcher();
    
    public MeleConfiguration() {
        addResource("mele-default.xml");
        addResource("mele-site.xml");
    }

    public String getZooKeeperConnectionString() {
        return get(MELE_ZOOKEEPER_CONNECTION);
    }

    public void setZooKeeperConnectionString(String zooKeeperConnectionString) {
        set(MELE_ZOOKEEPER_CONNECTION,zooKeeperConnectionString);
    }

    public int getZooKeeperSessionTimeout() {
        return getInt(MELE_ZOOKEEPER_SESSION_TIMEOUT, DEFAULT_ZOOKEEPER_SESSION_TIMEOUT);
    }

    public void setZooKeeperSessionTimeout(int zooKeeperSessionTimeout) {
        setInt(MELE_ZOOKEEPER_SESSION_TIMEOUT, zooKeeperSessionTimeout);
    }

    public String getBaseZooKeeperPath() {
        return get(MELE_BASE_ZOOKEEPER_PATH);
    }

    public void setBaseZooKeeperPath(String baseZooKeeperPath) {
        set(MELE_BASE_ZOOKEEPER_PATH, baseZooKeeperPath);
    }

    public List<String> getLocalReplicationPathList() {
        String paths = get(MELE_LOCAL_REPLICATION_PATHS);
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
        set(MELE_LOCAL_REPLICATION_PATHS,paths.toString());
    }

    public String getBaseHdfsPath() {
        return get(MELE_BASE_HDFS_PATH);
    }

    public void setBaseHdfsPath(String baseHdfsPath) {
        set(MELE_BASE_HDFS_PATH, baseHdfsPath);
    }
    
    
    //Object configuration------------
    
    public FileSystem getHdfsFileSystem() {
        if (hdfsFileSystem == null) {
            try {
                return FileSystem.get(this);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return hdfsFileSystem;
    }

    public void setHdfsFileSystem(FileSystem hdfsFileSystem) {
        this.hdfsFileSystem = hdfsFileSystem;
    }

    public Watcher getWatcher() {
        return watcher;
    }

    public void setWatcher(Watcher watcher) {
        this.watcher = watcher;
    }

}
