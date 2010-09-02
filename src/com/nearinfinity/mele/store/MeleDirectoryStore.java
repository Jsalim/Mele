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

import java.io.IOException;
import java.util.List;

/**
 * @author Aaron McCurry (amccurry@nearinfinity.com)
 */
public interface MeleDirectoryStore {
	
	/**
	 * Saves the block of file data to a persistent store.
	 * @param name the name of the file to save.
	 * @param blockId the block id to save.
	 * @param block the binary data to save.
	 * @throws IOException
	 */
	void saveBlock(String name, long blockId, byte[] block) throws IOException;
	
	/**
	 * Fetches a block of data from the persistent store.
	 * @param name the name of the file to fetch.
	 * @param blockId the the block id to fetch.
	 * @return the binary data of the block.
	 * @throws IOException
	 */
	byte[] fetchBlock(String name, long blockId) throws IOException;
	
	/**
	 * Lists all available files in this directory.
	 * @return the list of all the files in this directory.
	 * @throws IOException
	 */
	List<String> getAllFileNames() throws IOException;
	
	/**
	 * Checks to see if a file exists.
	 * @param name the file to check.
	 * @return boolean.
	 * @throws IOException
	 */
	boolean fileExists(String name) throws IOException;
	
	/**
	 * Gets last time the file was modified.
	 * @param name the file name.
	 * @return the last modified time stamp.
	 * @throws IOException
	 */
	long getFileModified(String name) throws IOException;
	
	/**
	 * Gets the file length.
	 * @param name the file name.
	 * @return the file length.
	 * @throws IOException
	 */
	long getFileLength(String name) throws IOException;
	
	/**
	 * Sets file length.
	 * @param name the file name.
	 * @param length the file length.
	 * @throws IOException
	 */
	void setFileLength(String name, long length) throws IOException;
	
	/**
	 * Closes this directory data access object.
	 */
	void close() throws IOException;

	/**
	 * Flushes the file to persistent store.
	 * @param name
	 * @throws IOException
	 */
	void flush(String name) throws IOException;

	/**
	 * Removes the block for the given file.
	 * @param name the filename.
	 * @param blockId the block id.
	 * @throws IOException
	 */
	void removeBlock(String name, long blockId) throws IOException;
	
	/**
	 * Removes the file meta data.
	 * @param name the filename.
	 * @throws IOException
	 */
	void removeFileMetaData(String name) throws IOException;

}
