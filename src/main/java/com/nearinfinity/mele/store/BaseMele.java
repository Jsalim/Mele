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

package com.nearinfinity.mele.store;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.store.Directory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.nearinfinity.mele.Mele;
import com.nearinfinity.mele.MeleConfiguration;
import com.nearinfinity.mele.store.hdfs.ReplicationIndexDeletionPolicy;
import com.nearinfinity.mele.store.util.ZkUtils;
import com.nearinfinity.mele.store.zookeeper.ZooKeeperFactory;
import com.nearinfinity.mele.store.zookeeper.ZookeeperIndexDeletionPolicy;

/** @author Aaron McCurry (amccurry@nearinfinity.com) */
public abstract class BaseMele extends Mele implements Watcher {

    private static final Log LOG = LogFactory.getLog(BaseMele.class);
    protected ZooKeeper zk;
    protected String basePath;
    protected MeleConfiguration configuration;
    protected Map<String, Map<String, Directory>> remoteDirs = new ConcurrentHashMap<String, Map<String, Directory>>();
    protected Map<String, Map<String, Directory>> localDirs = new ConcurrentHashMap<String, Map<String, Directory>>();
    protected List<String> pathList;

    public BaseMele(MeleConfiguration configuration) throws IOException {
        this.configuration = configuration;
        this.basePath = configuration.getBaseZooKeeperPath();
        this.zk = ZooKeeperFactory.create(configuration, this);
        this.pathList = configuration.getLocalReplicationPathList();
    }

    @Override
    public void createDirectory(String directoryCluster, String directoryName) {
        if (ZkUtils.exists(zk, basePath, directoryCluster, directoryName)) {
            LOG.info("Directory [" + directoryName +
                    "] in cluster [" + directoryCluster +
                    "] already exists.");
        }
        ZkUtils.mkNodes(zk, basePath, directoryCluster, directoryName);
    }

    @Override
    public void createDirectoryCluster(String directoryCluster) {
        if (ZkUtils.exists(zk, basePath, directoryCluster)) {
            LOG.info("Cluster [" + directoryCluster + "] already exists.");
        }
        ZkUtils.mkNodes(zk, basePath, directoryCluster);
    }

    @Override
    public List<String> listClusters() {
        if (ZkUtils.exists(zk, basePath)) {
            try {
                return zk.getChildren(ZkUtils.getPath(basePath), false);
            }
            catch (KeeperException e) {
                throw new RuntimeException(e);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        return new ArrayList<String>();
    }

    @Override
    public List<String> listDirectories(String directoryCluster) {
        if (!ZkUtils.exists(zk, basePath, directoryCluster)) {
            throw new RuntimeException("Cluster [" + directoryCluster + "] does not exists.");
        }
        try {
            return zk.getChildren(ZkUtils.getPath(basePath, directoryCluster), false);
        }
        catch (KeeperException e) {
            throw new RuntimeException(e);
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Directory open(String directoryCluster, String directoryName) throws IOException {
        if (!ZkUtils.exists(zk, basePath, directoryCluster, directoryName)) {
            throw new RuntimeException("Directory [" + directoryName +
                    "] in Cluster [" + directoryCluster + "] does not exists.");
        }
        //probably should do something with zookeeper to show that this
        //process is opening the directory
        return internalOpen(directoryCluster, directoryName);
    }

    protected abstract Directory internalOpen(String directoryCluster, String directoryName) throws IOException;

    @Override
    public void removeDirectory(String directoryCluster, String directoryName) throws IOException {
        if (!ZkUtils.exists(zk, basePath, directoryCluster, directoryName)) {
            throw new RuntimeException("Directory [" + directoryName +
                    "] in Cluster [" + directoryCluster + "] does not exists.");
        }
        try {
            zk.delete(ZkUtils.getPath(basePath, directoryName), -1);
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        catch (KeeperException e) {
            throw new RuntimeException(e);
        }
        internalDelete(directoryCluster, directoryName);
    }

    protected void internalDelete(String directoryCluster, String directoryName) throws IOException {
        if (isDirectoryLocal(directoryCluster, directoryName)) {
            File file = getExistingLocalPath(directoryCluster, directoryName);
            rm(file);
        }
        throw new FileNotFoundException(directoryCluster + " " + directoryName);
    }

    @Override
    public void removeDirectoryCluster(String directoryCluster) throws IOException {
        if (!ZkUtils.exists(zk, basePath, directoryCluster)) {
            throw new RuntimeException("Cluster [" + directoryCluster + "] does not exists.");
        }
        List<String> directories = listDirectories(directoryCluster);
        for (String directory : directories) {
            removeDirectory(directoryCluster, directory);
        }
        try {
            zk.delete(ZkUtils.getPath(basePath, directoryCluster), -1);
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        catch (KeeperException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public abstract void process(WatchedEvent event);

    public static String getReferencePath(MeleConfiguration configuration, String directoryCluster,
                                          String directoryName) {
        return ZkUtils.getPath(configuration.getBaseZooKeeperPath(),
                directoryCluster, directoryName,
                configuration.getZooKeeperReferenceNodeName());
    }

    public static String getLockPath(MeleConfiguration configuration, String directoryCluster, String directoryName) {
        return ZkUtils.getPath(configuration.getBaseZooKeeperPath(),
                directoryCluster, directoryName,
                configuration.getZooKeeperLockNodeName());
    }

    @Override
    public IndexDeletionPolicy getIndexDeletionPolicy(String directoryCluster, String directoryName)
            throws IOException {
        Directory local = getFromCache(directoryCluster, directoryName, localDirs);
        Directory remote = getFromCache(directoryCluster, directoryName, remoteDirs);
        if (local == null || remote == null) {
            throw new RuntimeException("local or remote dir for [" + directoryCluster +
                    "] [" + directoryName + "] cannot be null.");
        }
        IndexDeletionPolicy policy =
                new ZookeeperIndexDeletionPolicy(getReferencePath(configuration, directoryCluster, directoryName));
        return new ReplicationIndexDeletionPolicy(policy, local, remote);
    }

    protected static Directory getFromCache(String directoryCluster, String directoryName,
                                          Map<String, Map<String, Directory>> dirs) {
        Map<String, Directory> map = dirs.get(directoryCluster);
        if (map == null) {
            return null;
        }
        return map.get(directoryName);
    }

    protected File getExistingLocalPath(String directoryCluster, String directoryName) {
        for (String localPath : pathList) {
            File filePath = getFilePath(localPath, directoryCluster, directoryName);
            if (filePath.exists()) {
                return filePath;
            }
        }
        throw new RuntimeException("[" + directoryCluster +
                "] [" + directoryName +
                "] not found locally.");
    }

    protected boolean isDirectoryLocal(String directoryCluster, String directoryName) {
        for (String localPath : pathList) {
            if (getFilePath(localPath, directoryCluster, directoryName).exists()) {
                return true;
            }
        }
        return false;
    }

    private File getFilePath(String localPath, String directoryCluster, String directoryName) {
        return new File(new File(localPath, directoryCluster), directoryName);
    }

    private void rm(File file) {
        if (file.isDirectory()) {
            for (File f : file.listFiles()) {
                rm(f);
            }
        }
        file.delete();
    }
}