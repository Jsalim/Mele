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

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.zookeeper.ZooKeeper;

import com.nearinfinity.mele.store.util.ZkUtils;

/** @author Aaron McCurry (amccurry@nearinfinity.com) */
public class ZookeeperWrapperDirectory extends Directory {

    private ZooKeeper zk;
    private Directory directory;
    private String indexRefPath;

    public ZookeeperWrapperDirectory(ZooKeeper zooKeeper, Directory directory, String indexRefPath, String indexLockPath) {
        this.zk = zooKeeper;
        this.directory = directory;
        this.indexRefPath = indexRefPath;
        ZkUtils.mkNodesStr(zk, indexRefPath);
        this.setLockFactory(new ZookeeperLockFactory(zk, indexLockPath));
    }

    public String getIndexRefPath() {
        return indexRefPath;
    }

    public void clearLock(String name) throws IOException {
        directory.clearLock(name);
    }

    public void close() throws IOException {
        directory.close();
    }

    public IndexOutput createOutput(String name) throws IOException {
        return directory.createOutput(name);
    }

    public void deleteFile(String name) throws IOException {
        directory.deleteFile(name);
    }

    public boolean equals(Object obj) {
        return directory.equals(obj);
    }

    public boolean fileExists(String name) throws IOException {
        return directory.fileExists(name);
    }

    public long fileLength(String name) throws IOException {
        return directory.fileLength(name);
    }

    public long fileModified(String name) throws IOException {
        return directory.fileModified(name);
    }

    public LockFactory getLockFactory() {
        return directory.getLockFactory();
    }

    public String getLockID() {
        return directory.getLockID();
    }

    public int hashCode() {
        return directory.hashCode();
    }

    public String[] listAll() throws IOException {
        return directory.listAll();
    }

    public Lock makeLock(String name) {
        return directory.makeLock(name);
    }

    public IndexInput openInput(String name, int bufferSize) throws IOException {
        return wrapRef(name, directory.openInput(name, bufferSize));
    }

    public IndexInput openInput(String name) throws IOException {
        return wrapRef(name, directory.openInput(name));
    }

    public void setLockFactory(LockFactory lockFactory) {
        directory.setLockFactory(lockFactory);
    }

    public void sync(String name) throws IOException {
        directory.sync(name);
    }

    public String toString() {
        return "zk:{\"ref\":\"" + indexRefPath + "\",\"dir\":" + directory.toString() + "}";
    }

    public void touchFile(String name) throws IOException {
        directory.touchFile(name);
    }

    private IndexInput wrapRef(final String name, final IndexInput indexInput) {
        final String refPath = ZookeeperIndexDeletionPolicy.createRef(zk, indexRefPath, name);
        return new IndexInput() {

            @Override
            public void close() throws IOException {
                indexInput.close();
                ZookeeperIndexDeletionPolicy.removeRef(zk, refPath);
            }

            @Override
            public long getFilePointer() {
                return indexInput.getFilePointer();
            }

            @Override
            public long length() {
                return indexInput.length();
            }

            @Override
            public byte readByte() throws IOException {
                return indexInput.readByte();
            }

            @Override
            public void readBytes(byte[] b, int offset, int len) throws IOException {
                indexInput.readBytes(b, offset, len);
            }

            @Override
            public void seek(long pos) throws IOException {
                indexInput.seek(pos);
            }

            @Override
            public Object clone() {
                return indexInput.clone();
            }
        };
    }
}
