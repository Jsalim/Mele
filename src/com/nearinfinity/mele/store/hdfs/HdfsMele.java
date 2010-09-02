package com.nearinfinity.mele.store.hdfs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.zookeeper.WatchedEvent;

import com.nearinfinity.mele.store.BaseMele;
import com.nearinfinity.mele.store.MeleConfiguration;
import com.nearinfinity.mele.store.rsync.RsyncMele;
import com.nearinfinity.mele.store.zookeeper.ZookeeperWrapperDirectory;

public class HdfsMele extends BaseMele {
	
	private static final Log LOG = LogFactory.getLog(RsyncMele.class);
	private List<String> pathList;
	private String baseHdfsPath;
	private FileSystem hdfsFileSystem;
	private Random random = new Random();

	public HdfsMele(MeleConfiguration configuration) throws IOException {
		super(configuration);
		this.pathList = configuration.getLocalReplicationPathList();
		this.baseHdfsPath = configuration.getBaseHdfsPath();
		this.hdfsFileSystem = configuration.getHdfsFileSystem();
	}

	@Override
	protected void internalDelete(String directoryCluster, String directoryName) throws IOException {
		if (isDirectoryLocal(directoryCluster, directoryName)) {
			File file = getExistingLocalPath(directoryCluster, directoryName);
			rm(file);
		}
		throw new FileNotFoundException(directoryCluster + " " + directoryName);
	}

	@Override
	protected Directory internalOpen(String directoryCluster, String directoryName) throws IOException {
		File localPath;
		if (isDirectoryLocal(directoryCluster,directoryName)) {
			localPath = getExistingLocalPath(directoryCluster,directoryName);
		} else {
			localPath = getNewLocalPath(directoryCluster,directoryName);
		}
		FSDirectory local = FSDirectory.open(localPath);
		Path hdfsDirPath = new Path(baseHdfsPath,directoryCluster);
		HdfsDirectory remote = new HdfsDirectory(new Path(hdfsDirPath,directoryName), hdfsFileSystem);
		ReplicatedDirectory directory = new ReplicatedDirectory(local, remote, true);
		return new ZookeeperWrapperDirectory(directory,
				BaseMele.getReferencePath(configuration,directoryCluster,directoryName),
				BaseMele.getLockPath(configuration,directoryCluster,directoryName));
	}

	@Override
	public void process(WatchedEvent event) {
		
	}

	@Override
	public List<String> listLocalDirectories(String directoryCluster) {
		List<String> result = new ArrayList<String>();
		for (String localPath : pathList) {
			File file = new File(localPath,directoryCluster);
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
	
	private File getNewLocalPath(String directoryCluster, String directoryName) {
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
			File testFile = new File(file,UUID.randomUUID().toString());
			try {
				if (testFile.createNewFile()) {
					testFile.delete();
					File dirFile = new File(new File(file,directoryCluster),directoryName);
					dirFile.mkdirs();
					return dirFile;
				}
			} catch (IOException e) {
				LOG.error("Can not create file on [" + file.getAbsolutePath() + "]");
			}
		}
	}

	private File getExistingLocalPath(String directoryCluster, String directoryName) {
		for (String localPath : pathList) {
			File filePath = getFilePath(localPath,directoryCluster,directoryName);
			if (filePath.exists()) {
				return filePath;
			}
		}
		throw new RuntimeException("[" + directoryCluster +
				"] [" + directoryName + 
				"] not found locally.");
	}

	private boolean isDirectoryLocal(String directoryCluster, String directoryName) {
		for (String localPath : pathList) {
			if (getFilePath(localPath,directoryCluster,directoryName).exists()) {
				return true;
			}
		}
		return false;
	}

	private File getFilePath(String localPath, String directoryCluster, String directoryName) {
		return new File(new File(localPath,directoryCluster),directoryName);
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
