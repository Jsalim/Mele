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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

/**
 * @author Aaron McCurry (amccurry@nearinfinity.com)
 */
public class HdfsDirectory extends Directory {
	
	private static final Log LOG = LogFactory.getLog(ReplicatedDirectory.class);
	private static final int BUFFER_SIZE = 65536;
	private Path hdfsDirPath;
	private FileSystem fileSystem;
	
	public HdfsDirectory(Path hdfsDirPath, FileSystem fileSystem) {
		this.hdfsDirPath = hdfsDirPath;
		this.fileSystem = fileSystem;
		try {
			if (!fileSystem.exists(hdfsDirPath)) {
				fileSystem.mkdirs(hdfsDirPath);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() throws IOException {
		
	}

	@Override
	public IndexOutput createOutput(String name) throws IOException {
		final FSDataOutputStream outputStream = fileSystem.create(new Path(hdfsDirPath,name));
		return new IndexOutput() {
			
			private long length = 0;

			@Override
			public void close() throws IOException {
				outputStream.close();
			}

			@Override
			public void flush() throws IOException {
				outputStream.flush();
			}

			@Override
			public long getFilePointer() {
				return length;
			}

			@Override
			public long length() throws IOException {
				return length;
			}

			@Override
			public void seek(long pos) throws IOException {
				throw new RuntimeException("not supported");
			}

			@Override
			public void writeByte(byte b) throws IOException {
				outputStream.write(b);
				length++;
			}

			@Override
			public void writeBytes(byte[] b, int off, int len) throws IOException {
				outputStream.write(b, off, len);
				length += len;
			}
		};
	}
	
	@Override
	public IndexInput openInput(final String name) throws IOException {
		final FSDataInputStream inputStream = fileSystem.open(new Path(hdfsDirPath,name));
		return new BufferedIndexInput() {
			
			private long length = fileLength(name);
			
			@Override
			public long length() {
				return length;
			}
			
			@Override
			public void close() throws IOException {
				inputStream.close();
			}
			
			@Override
			protected void seekInternal(long pos) throws IOException {
				
			}
			
			@Override
			protected void readInternal(byte[] b, int offset, int length) throws IOException {
				synchronized (inputStream) {
					long position = getFilePointer();
					inputStream.seek(position);
					inputStream.read(b, offset, length);
				}
			}
		};
	}

	@Override
	public void deleteFile(String name) throws IOException {
		fileSystem.delete(new Path(hdfsDirPath,name), false);
	}

	@Override
	public boolean fileExists(String name) throws IOException {
		return fileSystem.exists(new Path(hdfsDirPath,name));
	}

	@Override
	public long fileLength(String name) throws IOException {
		FileStatus fileStatus = fileSystem.getFileStatus(new Path(hdfsDirPath,name));
		return fileStatus.getLen();
	}

	@Override
	public long fileModified(String name) throws IOException {
		FileStatus fileStatus = fileSystem.getFileStatus(new Path(hdfsDirPath,name));
		return fileStatus.getModificationTime();
	}

	@Override
	public String[] listAll() throws IOException {
		FileStatus[] listStatus = fileSystem.listStatus(hdfsDirPath);
		List<String> files = new ArrayList<String>();
		for (FileStatus status : listStatus) {
			if (!status.isDirectory()) {
				files.add(status.getPath().getName());
			}
		}
		return files.toArray(new String[]{});
	}

	@Override
	public void touchFile(String name) throws IOException {
		//do nothing
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
