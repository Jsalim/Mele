package com.nearinfinity.mele.store.cassandra.storageproxy;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SortedSet;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.SliceByNamesReadCommand;
import org.apache.cassandra.db.SliceFromReadCommand;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.thrift.transport.TTransportException;

import com.nearinfinity.mele.store.MeleDirectory;
import com.nearinfinity.mele.store.MeleDirectoryStore;
import com.nearinfinity.mele.store.util.Bytes;

public class CassandraStorageProxyStore implements MeleDirectoryStore {

	private static final String SEP = "/";
	private ConsistencyLevel consistencyLevel;
	private String keySpace;
	private String columnFamily;
	private String dirName;

	public static class EmbeddedCassandraService implements Runnable {
		private CassandraDaemon cassandraDaemon;

		public void init() throws TTransportException, IOException {
			cassandraDaemon = new CassandraDaemon();
			cassandraDaemon.init(null);
		}

		public void run() {
			cassandraDaemon.start();
		}
	}
	
	public static void start() {
		EmbeddedCassandraService service = new EmbeddedCassandraService();
		try {
			service.init();
		} catch (TTransportException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		new Thread(service).start();
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	public static void main(String... args) throws Exception {
		start();
		CassandraStorageProxyStore store = new CassandraStorageProxyStore("Keyspace1","Standard1","testing",ConsistencyLevel.ONE);
		MeleDirectory directory = new MeleDirectory(store);
		IndexOutput output = directory.createOutput("testing");
		output.writeInt(1234);
		output.close();
		IndexInput input = directory.openInput("testing");
		System.out.println(input.readInt());
		input.close();
	}

	public CassandraStorageProxyStore(String keySpace, String columnFamily, String dirName,
			ConsistencyLevel consistencyLevel) {
		this.keySpace = keySpace;
		this.columnFamily = columnFamily;
		this.dirName = dirName;
		this.consistencyLevel = consistencyLevel;
	}

	@Override
	public void removeFileMetaData(final String name) throws IOException {
		ColumnPath columnPath = new ColumnPath(columnFamily);
		columnPath.setColumn(Bytes.toBytes(name));

		QueryPath path = new QueryPath(columnPath);
		RowMutation mutation = new RowMutation(keySpace, getDirectoryId());
		mutation.delete(path, System.currentTimeMillis());
		
		mutateBlocking(mutation);
	}

	@Override
	public long getFileModified(final String name) throws IOException {
		byte[] columnName = Bytes.toBytes(name);
		List<Row> rows = readProtocol(getReadCommand(getDirectoryId(), columnName));
		if (rows.isEmpty()) {
			throw new FileNotFoundException(name);
		}
		Row row = rows.get(0);
		IColumn column = row.cf.getColumn(columnName);
		if (column.isMarkedForDelete()) {
			throw new FileNotFoundException(name);
		}
		return column.timestamp();
	}

	@Override
	public long getFileLength(final String name) throws IOException {
		byte[] columnName = Bytes.toBytes(name);
		List<Row> rows = readProtocol(getReadCommand(getDirectoryId(), columnName));
		if (rows.isEmpty()) {
			throw new FileNotFoundException(name);
		}
		Row row = rows.get(0);
		ColumnFamily cf = row.cf;
		if (cf == null) {
			throw new FileNotFoundException(name);
		}
		IColumn column = cf.getColumn(columnName);
		if (column == null || column.isMarkedForDelete()) {
			throw new FileNotFoundException(name);
		}
		return Bytes.toLong(column.value());
	}

	public void setFileLength(final String name, final long length) throws IOException {
		RowMutation rowMutation = new RowMutation(keySpace, getDirectoryId());
		rowMutation.add(new QueryPath(new ColumnPath(columnFamily).setColumn(Bytes.toBytes(name))), 
				Bytes.toBytes(length), System.currentTimeMillis());
		mutateBlocking(rowMutation);
	}

	@Override
	public void close() {
	}

	@Override
	public boolean fileExists(String name) throws IOException {
		byte[] columnName = Bytes.toBytes(name);
		List<Row> rows = readProtocol(getReadCommand(getDirectoryId(), columnName));
		if (rows.isEmpty()) {
			return false;
		}
		Row row = rows.get(0);
		if (row.cf == null) {
			return false;
		}
		IColumn column = row.cf.getColumn(columnName);
		if (column == null || column.isMarkedForDelete()) {
			return false;
		}
		return true;
	}

	@Override
	public List<String> getAllFileNames() throws IOException {
		ColumnParent columnParent = new ColumnParent(columnFamily);
		SlicePredicate predicate = new SlicePredicate();
		SliceRange sliceRange = new SliceRange(Bytes.EMPTY_BYTE_ARRAY, Bytes.EMPTY_BYTE_ARRAY, false, Integer.MAX_VALUE);
		predicate.setSlice_range(sliceRange);
		SliceFromReadCommand command = new SliceFromReadCommand(keySpace, getDirectoryId(), columnParent, sliceRange.start, sliceRange.finish, sliceRange.reversed, sliceRange.count);
		try {
			List<ReadCommand> commands = new ArrayList<ReadCommand>();
			commands.add(command);
			List<Row> readProtocol = StorageProxy.readProtocol(commands, consistencyLevel);
			List<String> result = new ArrayList<String>();
			for (Row row : readProtocol) {
				ColumnFamily cf = row.cf;
				if (cf != null) {
					SortedSet<byte[]> columnNames = cf.getColumnNames();
					for (byte[] name : columnNames) {
						result.add(Bytes.toString(name));
					}
				}
			}
			return result;
		} catch (UnavailableException e) {
			throw new RuntimeException(e);
		} catch (TimeoutException e) {
			throw new RuntimeException(e);
		} catch (InvalidRequestException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void flush(String name) throws IOException {

	}

	@Override
	public void removeBlock(final String name, final long blockId) throws IOException {
		ColumnPath columnPath = new ColumnPath(columnFamily).setColumn(Bytes.toBytes(blockId));

		QueryPath path = new QueryPath(columnPath);
		RowMutation mutation = new RowMutation(keySpace, getDirectoryId(name,blockId));
		mutation.delete(path, System.currentTimeMillis());
		
		mutateBlocking(mutation);
	}

	public void saveBlock(final String name, final long blockId, final byte[] block) throws IOException {
		ColumnPath columnPath = new ColumnPath(columnFamily).setColumn(Bytes.toBytes(blockId));
	
		QueryPath path = new QueryPath(columnPath);
		RowMutation mutation = new RowMutation(keySpace, getDirectoryId(name,blockId));
		mutation.add(path, block, System.currentTimeMillis());

		mutateBlocking(mutation);
	}

	public byte[] fetchBlock(final String name, final long blockId) throws IOException {
		byte[] columnName = Bytes.toBytes(blockId);
		List<Row> rows = readProtocol(getReadCommand(getDirectoryId(name,blockId), columnName));
		if (rows == null || rows.size() == 0) {
			return null;
		}
		Row row = rows.get(0);
		ColumnFamily cf = row.cf;
		if (cf == null) {
			return null;
		}
		IColumn column = cf.getColumn(columnName);
		if (column == null || column.isMarkedForDelete()) {
			return null;
		}
		return column.value();
	}

	private String getDirectoryId() {
		return dirName;
	}

	private String getDirectoryId(String name, long blockId) {
		return dirName + SEP + name + ":" + Long.toString(blockId);
	}

	private ReadCommand getReadCommand(String id, byte[] columnName) {
		return new SliceByNamesReadCommand(keySpace, id, new ColumnParent(
				columnFamily), Arrays.asList(columnName));
	}
	
	private void mutateBlocking(RowMutation... rowMutation) {
		try {
			StorageProxy.mutateBlocking(getMutations(rowMutation), consistencyLevel);
		} catch (UnavailableException e) {
			throw new RuntimeException(e);
		} catch (TimeoutException e) {
			throw new RuntimeException(e);
		}
	}
	
	private List<Row> readProtocol(ReadCommand... readCommand) {
		try {
			return StorageProxy.readProtocol(Arrays.asList(readCommand), consistencyLevel);
		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (UnavailableException e) {
			throw new RuntimeException(e);
		} catch (TimeoutException e) {
			throw new RuntimeException(e);
		} catch (InvalidRequestException e) {
			throw new RuntimeException(e);
		}
	}

	private List<RowMutation> getMutations(RowMutation... rowMutation) {
		return Arrays.asList(rowMutation);
	}
}
