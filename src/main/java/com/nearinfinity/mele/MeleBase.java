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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.nearinfinity.mele.replication.ReplicationIndexDeletionPolicy;
import com.nearinfinity.mele.util.ZkUtils;
import com.nearinfinity.mele.zookeeper.ZookeeperIndexDeletionPolicy;
import com.nearinfinity.mele.zookeeper.ZookeeperWrapperDirectory;

/** @author Aaron McCurry (amccurry@nearinfinity.com) */
public class MeleBase implements Watcher, MeleConstants, Mele {

    private static final Log LOG = LogFactory.getLog(MeleBase.class);

    private ZooKeeper zk;
    private String basePath;
    private MeleConfiguration configuration;
    private Map<String, Map<String, Directory>> remoteDirs = new ConcurrentHashMap<String, Map<String, Directory>>();
    private Map<String, Map<String, Directory>> localDirs = new ConcurrentHashMap<String, Map<String, Directory>>();
    private List<String> pathList;
    private Random random = new Random();
    private MeleDirectoryFactory directoryFactory;

    public MeleBase(MeleDirectoryFactory directoryFactory, MeleConfiguration configuration, ZooKeeper zk) throws IOException {
        this.zk = zk;
        this.pathList = configuration.getLocalReplicationPathList();
        this.basePath = configuration.getBaseZooKeeperPath();
        this.configuration = configuration;
        this.directoryFactory = directoryFactory;
    }

    /* (non-Javadoc)
     * @see com.nearinfinity.mele.Mele#getIndexDeletionPolicy(java.lang.String, java.lang.String)
     */
    public IndexDeletionPolicy getIndexDeletionPolicy(String directoryCluster, String directoryName) throws IOException {
        Directory local = getFromCache(directoryCluster, directoryName, localDirs);
        Directory remote = getFromCache(directoryCluster, directoryName, remoteDirs);
        if (local == null || remote == null) {
            throw new RuntimeException("local or remote dir for [" + directoryCluster + "] [" + directoryName
                    + "] cannot be null.");
        }
        IndexDeletionPolicy policy = new ZookeeperIndexDeletionPolicy(zk, getReferencePath(configuration, directoryCluster, directoryName));
        return new ReplicationIndexDeletionPolicy(policy, local, remote);
    }

    /* (non-Javadoc)
     * @see com.nearinfinity.mele.Mele#open(java.lang.String, java.lang.String)
     */
    public Directory open(String directoryCluster, String directoryName) throws IOException {
        if (!ZkUtils.exists(zk, basePath, directoryCluster, directoryName)) {
            throw new RuntimeException("Directory [" + directoryName + "] in Cluster [" + directoryCluster
                    + "] does not exists.");
        }
        // probably should do something with zookeeper to show that this
        // process is opening the directory
        return internalOpen(directoryCluster, directoryName, localDirs, remoteDirs, 
                pathList, configuration, zk, directoryFactory, random);
    }

    /* (non-Javadoc)
     * @see com.nearinfinity.mele.Mele#createDirectoryCluster(java.lang.String)
     */
    public void createDirectoryCluster(String directoryCluster) {
        if (ZkUtils.exists(zk, basePath, directoryCluster)) {
            LOG.info("Cluster [" + directoryCluster + "] already exists.");
        }
        ZkUtils.mkNodes(zk, basePath, directoryCluster);
    }

    /* (non-Javadoc)
     * @see com.nearinfinity.mele.Mele#createDirectory(java.lang.String, java.lang.String)
     */
    public void createDirectory(String directoryCluster, String directoryName) {
        if (ZkUtils.exists(zk, basePath, directoryCluster, directoryName)) {
            LOG.info("Directory [" + directoryName + "] in cluster [" + directoryCluster + "] already exists.");
        }
        ZkUtils.mkNodes(zk, basePath, directoryCluster, directoryName);
    }

    /* (non-Javadoc)
     * @see com.nearinfinity.mele.Mele#listClusters()
     */
    public List<String> listClusters() {
        if (ZkUtils.exists(zk, basePath)) {
            try {
                return zk.getChildren(ZkUtils.getPath(basePath), false);
            } catch (KeeperException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        return new ArrayList<String>();
    }

    /* (non-Javadoc)
     * @see com.nearinfinity.mele.Mele#listDirectories(java.lang.String)
     */
    public List<String> listDirectories(String directoryCluster) {
        if (!ZkUtils.exists(zk, basePath, directoryCluster)) {
            throw new RuntimeException("Cluster [" + directoryCluster + "] does not exists.");
        }
        try {
            return zk.getChildren(ZkUtils.getPath(basePath, directoryCluster), false);
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /* (non-Javadoc)
     * @see com.nearinfinity.mele.Mele#listLocalDirectories(java.lang.String)
     */
    public List<String> listLocalDirectories(String directoryCluster) {
        List<String> result = new ArrayList<String>();
        for (String localPath : pathList) {
            File file = new File(localPath, directoryCluster);
            if (file.exists() && file.isDirectory()) {
                File[] listFiles = file.listFiles();
                for (File f : listFiles) {
                    if (f.isDirectory()) {
                        result.add(f.getName());
                    }
                }
            }
        }
        return result;
    }

    /* (non-Javadoc)
     * @see com.nearinfinity.mele.Mele#removeDirectoryCluster(java.lang.String)
     */
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
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        }
    }

    /* (non-Javadoc)
     * @see com.nearinfinity.mele.Mele#removeDirectory(java.lang.String, java.lang.String)
     */
    public void removeDirectory(String directoryCluster, String directoryName) throws IOException {
        if (!ZkUtils.exists(zk, basePath, directoryCluster, directoryName)) {
            throw new RuntimeException("Directory [" + directoryName + "] in Cluster [" + directoryCluster
                    + "] does not exists.");
        }
        try {
            zk.delete(ZkUtils.getPath(basePath, directoryName), -1);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        }
        internalDelete(pathList, directoryCluster, directoryName);
    }

    @Override
    public void process(WatchedEvent event) {
        //do nothing
    }

    public static String getReferencePath(MeleConfiguration configuration, String directoryCluster, String directoryName) {
        return ZkUtils.getPath(configuration.getBaseZooKeeperPath(), directoryCluster, directoryName,
                MELE_ZOOKEEPER_REFS_NAME);
    }

    public static String getLockPath(MeleConfiguration configuration, String directoryCluster, String directoryName) {
        return ZkUtils.getPath(configuration.getBaseZooKeeperPath(), directoryCluster, directoryName,
                MELE_ZOOKEEPER_LOCK_NAME);
    }
    
    private static Directory internalOpen(String directoryCluster, String directoryName, 
            Map<String, Map<String, Directory>> localDirs, Map<String, Map<String, Directory>> remoteDirs, 
            List<String> pathList, MeleConfiguration configuration, ZooKeeper zk, MeleDirectoryFactory directoryFactory,
            Random random) throws IOException {
        Directory dir = getFromCache(directoryCluster, directoryName, localDirs);
        if (dir != null) {
            return dir;
        }
        File localPath;
        if (isDirectoryLocal(pathList, directoryCluster, directoryName)) {
            localPath = getExistingLocalPath(pathList, directoryCluster, directoryName);
        } else {
            localPath = getNewLocalPath(pathList,directoryCluster, directoryName, random);
        }
        FSDirectory local = FSDirectory.open(localPath);
        Directory remote = directoryFactory.getDirectory(local, directoryCluster, directoryName);
        addToCache(directoryCluster, directoryName, remote, remoteDirs);
        addToCache(directoryCluster, directoryName, local, localDirs);
        return new ZookeeperWrapperDirectory(zk, local, MeleBase.getReferencePath(configuration, directoryCluster,
                directoryName), MeleBase.getLockPath(configuration, directoryCluster, directoryName));
    }

    private static void internalDelete(List<String> pathList, String directoryCluster, String directoryName) throws IOException {
        if (isDirectoryLocal(pathList, directoryCluster, directoryName)) {
            File file = getExistingLocalPath(pathList, directoryCluster, directoryName);
            rm(file);
        }
        throw new FileNotFoundException(directoryCluster + " " + directoryName);
    }

    private static Directory getFromCache(String directoryCluster, String directoryName,
            Map<String, Map<String, Directory>> dirs) {
        Map<String, Directory> map = dirs.get(directoryCluster);
        if (map == null) {
            return null;
        }
        return map.get(directoryName);
    }

    private static File getExistingLocalPath(List<String> pathList, String directoryCluster, String directoryName) {
        for (String localPath : pathList) {
            File filePath = getFilePath(localPath, directoryCluster, directoryName);
            if (filePath.exists()) {
                return filePath;
            }
        }
        throw new RuntimeException("[" + directoryCluster + "] [" + directoryName + "] not found locally.");
    }

    private static boolean isDirectoryLocal(List<String> pathList, String directoryCluster, String directoryName) {
        for (String localPath : pathList) {
            if (getFilePath(localPath, directoryCluster, directoryName).exists()) {
                return true;
            }
        }
        return false;
    }

    private static File getFilePath(String localPath, String directoryCluster, String directoryName) {
        return new File(new File(localPath, directoryCluster), directoryName);
    }

    private static void rm(File file) {
        if (file.isDirectory()) {
            for (File f : file.listFiles()) {
                rm(f);
            }
        }
        file.delete();
    }

    private synchronized static void addToCache(String directoryCluster, String directoryName, Directory dir,
            Map<String, Map<String, Directory>> dirs) {
        Map<String, Directory> map = dirs.get(directoryCluster);
        if (map == null) {
            map = new ConcurrentHashMap<String, Directory>();
            dirs.put(directoryCluster, map);
        }
        map.put(directoryName, dir);
    }

    private static File getNewLocalPath(List<String> pathList, String directoryCluster, String directoryName, Random random) {
        Collection<String> attempts = new HashSet<String>();
        while (true) {
            if (attempts.size() == pathList.size()) {
                throw new RuntimeException("no local writable dirs");
            }
            int index = random.nextInt(pathList.size());
            String pathname = pathList.get(index);
            attempts.add(pathname);
            File file = new File(pathname);
            file.mkdirs();
            File testFile = new File(file, UUID.randomUUID().toString());
            try {
                if (testFile.createNewFile()) {
                    testFile.delete();
                    File dirFile = new File(new File(file, directoryCluster), directoryName);
                    dirFile.mkdirs();
                    return dirFile;
                }
            } catch (IOException e) {
                LOG.error("Can not create file on [" + file.getAbsolutePath() + "]");
            }
        }
    }
}
