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

package com.nearinfinity.mele.store.zookeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import com.nearinfinity.mele.store.util.ZkUtils;

/** @author Aaron McCurry (amccurry@nearinfinity.com) */
public class ZookeeperIndexDeletionPolicy implements IndexDeletionPolicy {

    private final static Log LOG = LogFactory.getLog(ZookeeperIndexDeletionPolicy.class);
    private String indexRefPath;
    private ZooKeeper zk;

    public ZookeeperIndexDeletionPolicy(ZooKeeper zooKeeper, String indexRefPath) {
        this.zk = zooKeeper;
        this.indexRefPath = indexRefPath;
        ZkUtils.mkNodesStr(zk, indexRefPath);
    }

    @Override
    public void onCommit(List<? extends IndexCommit> commits) throws IOException {
        List<String> filesCurrentlyBeingReferenced = getListOfReferencedFiles(zk, indexRefPath);
        int size = commits.size();
        Collection<String> previouslyReferencedFiles = new TreeSet<String>();
        OUTER: for (int i = size - 2; i >= 0; i--) {
            IndexCommit indexCommit = commits.get(i);
            LOG.info("Processing index commit generation " + indexCommit.getGeneration());
            Collection<String> fileNames = new TreeSet<String>(indexCommit.getFileNames());
            // remove all filenames that were references in newer index commits,
            // this way older index commits can be released without the fear of
            // broken references.
            fileNames.removeAll(previouslyReferencedFiles);
            for (String fileName : fileNames) {
                if (filesCurrentlyBeingReferenced.contains(fileName)) {
                    previouslyReferencedFiles.addAll(fileNames);
                    continue OUTER;
                }
            }
            LOG.info("Index Commit " + indexCommit.getGeneration() + " no longer needed, releasing " + fileNames);
            indexCommit.delete();
        }
    }

    @Override
    public void onInit(List<? extends IndexCommit> commits) throws IOException {
        onCommit(commits);
    }

    public static List<String> getListOfReferencedFiles(ZooKeeper zk, String indexRefPath) {
        try {
            List<String> files = new ArrayList<String>();
            List<String> children = zk.getChildren(indexRefPath, false);
            for (String child : children) {
                String name = getName(child);
                if (!files.contains(name)) {
                    files.add(name);
                }
            }
            return files;
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static String getName(String name) {
        int index = name.lastIndexOf('.');
        return name.substring(0, index);
    }

    public static String createRef(ZooKeeper zk, String indexRefPath, String name) {
        try {
            String path = zk.create(indexRefPath + "/" + name + ".", null, Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL_SEQUENTIAL);
            LOG.debug("Created reference path " + path);
            return path;
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void removeRef(ZooKeeper zk, String refPath) {
        try {
            LOG.debug("Removing reference path " + refPath);
            zk.delete(refPath, 0);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        }
    }
}
