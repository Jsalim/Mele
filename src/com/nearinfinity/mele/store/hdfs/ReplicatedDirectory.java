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

package com.nearinfinity.mele.store.hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Timer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NoLockFactory;

/**
 * @author Aaron McCurry (amccurry@nearinfinity.com)
 */
public class ReplicatedDirectory extends Directory {

	private static final Log LOG = LogFactory.getLog(HdfsMele.class);
	private static final int BUFFER_SIZE = 65536;
	private Directory remote;
	private Directory local;
	private boolean writing;
	private Timer timer;
	private Collection<String> fileNameCache = Collections.synchronizedSet(new HashSet<String>());

	public ReplicatedDirectory(Directory local, Directory remote, boolean writing) throws IOException {
		this.local = local;
		this.remote = remote;
		this.writing = writing;
		if (writing) {
			syncToLocal(remote);
			fileNameCache.addAll(Arrays.asList(local.listAll()));
		} else {
			fileNameCache.addAll(Arrays.asList(remote.listAll()));
		}
		setLockFactory(new NoLockFactory());
	}

	@Override
	public void close() throws IOException {
		local.close();
		remote.close();
		timer.cancel();
	}

	@Override
	public void deleteFile(String name) throws IOException {
		if (fileExists(name)) {
			fileNameCache.remove(name);
			local.deleteFile(name);
			remote.deleteFile(name);
			return;
		}
		throw new FileNotFoundException(name);
	}

	@Override
	public boolean fileExists(String name) throws IOException {
		boolean localFileExists = local.fileExists(name);
		boolean remoteFileExists = remote.fileExists(name);
		if (remoteFileExists == localFileExists) {// they agree
			return remoteFileExists;
		}
		sync(name);
		return fileExists(name);
	}

	@Override
	public long fileLength(String name) throws IOException {
		if (fileExists(name)) {
			return local.fileLength(name);
		}
		throw new FileNotFoundException(name);
	}

	@Override
	public long fileModified(String name) throws IOException {
		if (fileExists(name)) {
			return local.fileModified(name);
		}
		throw new FileNotFoundException(name);
	}

	@Override
	public String[] listAll() throws IOException {
		return fileNameCache.toArray(new String[]{});
	}

	@Override
	public void touchFile(String name) throws IOException {
		// do nothing
	}

	@Override
	public IndexOutput createOutput(final String name) throws IOException {
		if (writing) {
			return new IndexOutput() {
				
				private IndexOutput output = local.createOutput(name);

				@Override
				public void close() throws IOException {
					output.close();
					fileNameCache.add(name);
				}

				@Override
				public void flush() throws IOException {
					output.flush();
				}

				@Override
				public long getFilePointer() {
					return output.getFilePointer();
				}

				@Override
				public long length() throws IOException {
					return output.length();
				}

				@Override
				public void seek(long pos) throws IOException {
					output.seek(pos);
				}

				@Override
				public void writeByte(byte b) throws IOException {
					output.writeByte(b);
				}

				@Override
				public void writeBytes(byte[] b, int offset, int length) throws IOException {
					output.writeBytes(b,offset,length);
				}
	
			};
		}
		throw new IOException("read only mode");
	}

	@Override
	public IndexInput openInput(String name) throws IOException {
		if (fileExists(name)) {
			return local.openInput(name);
		}
		throw new FileNotFoundException(name);
	}

	@Override
	public void sync(String name) throws IOException {
		LOG.debug("sync file [" + name + "]");
		if (writing) {
			copyFile(name, local, remote);
		} else {
			try {
				copyFile(name, remote, local);
				fileNameCache.add(name);
			} catch (FileNotFoundException e) {
				LOG.debug("Error during sync, skipping file [" + name + "]");
			}
			prune();
		}
	}
	
	private void syncToLocal(Directory src) throws IOException {
		for (String name : src.listAll()) {
			if (!"segments.gen".equals(name)) {
				copyFile(name, remote, local);
			}
		}
	}

	private void prune() throws IOException {
		if (!writing) {
			for (String localFile : local.listAll()) {
				if (!remote.fileExists(localFile)) {
					local.deleteFile(localFile);
				}
			}
		}
	}

	public static void copyFile(String name, Directory src, Directory dest) throws IOException {
		if (src.fileExists(name) && dest.fileExists(name)) {
			if (src.fileLength(name) == dest.fileLength(name)) {
				//already there
				return;
			} else {
				dest.deleteFile(name);
			}
		}
		
		if (!src.fileExists(name) && dest.fileExists(name)) {
			dest.deleteFile(name);
			return;
		}
		
		LOG.info("copying file [" + name + "] from " + src + " to " + dest);
		
		byte[] buf = new byte[BUFFER_SIZE];
		IndexOutput os = null;
		IndexInput is = null;
		try {
			// create file in dest directory
			os = dest.createOutput(name);
			// read current file
			is = src.openInput(name);
			// and copy to dest directory
			long len = is.length();
			long readCount = 0;
			while (readCount < len) {
				int toRead = readCount + BUFFER_SIZE > len ? (int) (len - readCount) : BUFFER_SIZE;
				is.readBytes(buf, 0, toRead);
				os.writeBytes(buf, toRead);
				readCount += toRead;
			}
		} finally {
			// graceful cleanup
			try {
				if (os != null)
					os.close();
			} finally {
				if (is != null)
					is.close();
			}
		}
	}
}
