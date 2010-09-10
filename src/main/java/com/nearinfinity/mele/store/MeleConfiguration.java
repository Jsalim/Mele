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

import java.util.List;

import org.apache.hadoop.fs.FileSystem;

/**
 * @author Aaron McCurry (amccurry@nearinfinity.com)
 */
public class MeleConfiguration {

	private List<String> localReplicationPathList;
	private String baseHdfsPath;
	private FileSystem hdfsFileSystem;
	private boolean usingHdfs;
	private String baseZooKeeperPath = "/mele";
	private String zooKeeperConnectionString = "localhost";
	private int zooKeeperSessionTimeout = 3000;

	public String getZooKeeperConnectionString() {
		return zooKeeperConnectionString ;
	}

	public int getZooKeeperSessionTimeout() {
		return zooKeeperSessionTimeout;
	}

	public String getBaseZooKeeperPath() {
		return baseZooKeeperPath ;
	}

	public String getZooKeeperReferenceNodeName() {
		return "refs";
	}

	public String getZooKeeperLockNodeName() {
		return "locks";
	}

	public List<String> getLocalReplicationPathList() {
		return localReplicationPathList;
	}

	public String getBaseHdfsPath() {
		return baseHdfsPath;
	}

	public FileSystem getHdfsFileSystem() {
		return hdfsFileSystem;
	}

	public boolean isUsingHdfs() {
		return usingHdfs;
	}

	public void setLocalReplicationPathList(
			List<String> localReplicationPathList) {
		this.localReplicationPathList = localReplicationPathList;
	}

	public void setBaseHdfsPath(String baseHdfsPath) {
		this.baseHdfsPath = baseHdfsPath;
	}

	public void setHdfsFileSystem(FileSystem hdfsFileSystem) {
		this.hdfsFileSystem = hdfsFileSystem;
	}

	public void setUsingHdfs(boolean usingHdfs) {
		this.usingHdfs = usingHdfs;
	}
	
	public void setBaseZooKeeperPath(String baseZooKeeperPath) {
		this.baseZooKeeperPath = baseZooKeeperPath;
	}

	public void setZooKeeperConnectionString(String zooKeeperConnectionString) {
		this.zooKeeperConnectionString = zooKeeperConnectionString;
	}

	public void setZooKeeperSessionTimeout(int zooKeeperSessionTimeout) {
		this.zooKeeperSessionTimeout = zooKeeperSessionTimeout;
	}
}