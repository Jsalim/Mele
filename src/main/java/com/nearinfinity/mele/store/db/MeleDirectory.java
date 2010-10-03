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

package com.nearinfinity.mele.store.db;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.BufferedIndexOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NoLockFactory;

/**
 * @author Aaron McCurry (amccurry@nearinfinity.com)
 */
public class MeleDirectory extends Directory {
    
    public enum BLOCK_SIZE {
        _1K(10),
        _2K(11),
        _4K(12),
        _8K(13),
        _16K(14),
        _32K(15),
        _64K(16),
        _128K(17),
        _256K(18),
        _512K(19),
        _1M(20),
        _2M(21),
        _4M(22);
        
        private long shift;
        private int size;
        private long mask;

        private BLOCK_SIZE(long shift) {
            this.shift = shift;
            this.size = 1 << shift;
            this.mask = size - 1;
        }
        
        public long getBlockShift() {
            return shift;
        }
        
        public int getBlockSize() {
            return size;
        }
        
        public long getBlockMask() {
            return mask;
        }
    }

	public static final long DEFAULT_BLOCK_SHIFT = 15;
	public static final int DEFAULT_BLOCK_SIZE = 1 << DEFAULT_BLOCK_SHIFT;
	public static final long DEFAULT_BLOCK_MASK = DEFAULT_BLOCK_SIZE - 1;
	
	public static long getBlock(long pos, long blockShift) {
		return pos >>> blockShift;
	}
	
	public static long getPosition(long pos, long blockMask) {
		return pos & blockMask;
	}
	
	public static long getRealPosition(long block, long positionInBlock, long blockShift) {
		return (block << blockShift) + positionInBlock;
	}

	private MeleDirectoryStore store;
	private long blockShift = DEFAULT_BLOCK_SHIFT;
	private int blockSize = DEFAULT_BLOCK_SIZE;
	private long blockMask = DEFAULT_BLOCK_MASK;
	
	public MeleDirectory(MeleDirectoryStore store) {
		this(store,BLOCK_SIZE._32K);
	}
	
	public MeleDirectory(MeleDirectoryStore store, BLOCK_SIZE blockSize) {
        this.store = store;
        this.blockShift = blockSize.getBlockShift();
        this.blockSize = blockSize.getBlockSize();
        this.blockMask = blockSize.getBlockMask();
        setLockFactory(new NoLockFactory());
    }
	
	@Override
	public void close() throws IOException {
		store.close();
	}

	@Override
	public void deleteFile(String name) throws IOException {
		long length = store.getFileLength(name);
		store.removeFileMetaData(name);
		if (length > 0) {
			long maxBlockId = getBlock(length - 1, blockShift);
			for (long l = 0; l <= maxBlockId; l++) {
				store.removeBlock(name,l);
			}
		}
	}

	@Override
	public boolean fileExists(String name) throws IOException {
		return store.fileExists(name);
	}

	@Override
	public long fileLength(String name) throws IOException {
		if (!store.fileExists(name)) {
			throw new FileNotFoundException(name);
		}
		return store.getFileLength(name);
	}

	@Override
	public long fileModified(String name) throws IOException {
		return store.getFileModified(name);
	}

	@Override
	public String[] listAll() throws IOException {
		return store.getAllFileNames().toArray(new String[]{});
	}

	@Override
	public void touchFile(String name) throws IOException {
		long fileLength = store.getFileLength(name);
		store.setFileLength(name, fileLength < 0 ? 0 : fileLength);
	}

	@Override
	public IndexOutput createOutput(final String name) throws IOException {
		store.setFileLength(name, 0);
		return new BufferedIndexOutput() {
			
			private long position;
			private long fileLength = 0;

			@Override
			public long length() throws IOException {
				return fileLength;
			}
			
			@Override
			protected void flushBuffer(byte[] b, int offset, int len) throws IOException {
				while (len > 0) {
					long blockId = getBlock(position, blockShift);
					int innerPosition = (int) getPosition(position, blockMask);
					byte[] block = store.fetchBlock(name,blockId);
					if (block == null) {
						block = new byte[blockSize];
					}
					int length = Math.min(len, block.length - innerPosition);
					System.arraycopy(b, offset, block, innerPosition, length);
					store.saveBlock(name,blockId,block);
					position += length;
					len -= length;
					offset += length;
				}
				if (position > fileLength) {
					setLength(position);
				}
			}

			@Override
			public void close() throws IOException {
				super.close();
				store.flush(name);
			}

			@Override
			public void seek(long pos) throws IOException {
				super.seek(pos);
				this.position = pos;
			}

			@Override
			public void setLength(final long length) throws IOException {
				super.setLength(length);
				fileLength = length;
				store.setFileLength(name,length);
			}
		};
	}


	@Override
	public IndexInput openInput(final String name) throws IOException {
		if (!fileExists(name)) {
			touchFile(name);
		}
		final long fileLength = fileLength(name);
		return new BufferedIndexInput(blockSize/2) {
			@Override
			public long length() {
				return fileLength;
			}
			
			@Override
			public void close() throws IOException {
				
			}
			
			@Override
			protected void seekInternal(long pos) throws IOException {
			}
			
			@Override
			protected void readInternal(byte[] b, int off, int len) throws IOException {
				long position = getFilePointer();
				while (len > 0) {
					long blockId = getBlock(position, blockShift);
					int innerPosition = (int) getPosition(position, blockMask);
					byte[] block = store.fetchBlock(name, blockId);
					int length = Math.min(len,block.length-innerPosition);
					System.arraycopy(block, innerPosition, b, off, length);
					position += length;
					len -= length;
					off += length;
				}
			}
		};
	}
}
