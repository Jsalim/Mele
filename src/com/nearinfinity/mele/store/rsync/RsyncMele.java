package com.nearinfinity.mele.store.rsync;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.zookeeper.WatchedEvent;

import com.nearinfinity.mele.store.BaseMele;
import com.nearinfinity.mele.store.MeleConfiguration;
import com.nearinfinity.mele.store.zookeeper.ZookeeperLockFactory;

public class RsyncMele extends BaseMele {

	private static final Log LOG = LogFactory.getLog(RsyncMele.class);
	private RsyncDecision decision;
	private String rsyncBaseDir;

	public RsyncMele(MeleConfiguration configuration) throws IOException {
		super(configuration);
		rsyncBaseDir = configuration.getRsyncBaseDir();
		decision = new RsyncDecision(configuration);
	}

	@Override
	protected void internalDelete(String directoryCluster, String directoryName) {
		File path = new File(rsyncBaseDir, directoryCluster);
		rm(path);
	}

	@Override
	public void createDirectory(String directoryCluster, String directoryName) {
		super.createDirectory(directoryCluster, directoryName);
		File dir = getBaseDirectory(directoryCluster, directoryName);
		if (!dir.exists()) {
			dir.mkdirs();
		}
	}

	@Override
	protected Directory internalOpen(final String directoryCluster, final String directoryName) {
		String indexLockPath = BaseMele.getLockPath(configuration, directoryCluster, directoryName);
		LockFactory lockFactory = new ZookeeperLockFactory(zk, indexLockPath);
		File dir = getBaseDirectory(directoryCluster, directoryName);
		if (!dir.exists()) {
			throw new RuntimeException("rsync version can only open local directories.");
		}
		try {
			return new NIOFSDirectory(dir, lockFactory);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	protected File getBaseDirectory(String directoryCluster, String directoryName) {
		return new File(new File(rsyncBaseDir, directoryCluster), directoryName);
	}

	@Override
	public IndexDeletionPolicy getIndexDeletionPolicy(final String directoryCluster, final String directoryName) {
		return new IndexDeletionPolicy() {
			public void onInit(List<? extends IndexCommit> commits) {
				onCommit(commits);
			}

			public void onCommit(List<? extends IndexCommit> commits) {
				int size = commits.size();
				for (int i = 0; i < size - 1; i++) {
					commits.get(i).delete();
				}
				try {
					rsync(directoryCluster, directoryName);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		};
	}

	protected void rsync(String directoryCluster, String directoryName) throws IOException {
		File directory = getBaseDirectory(directoryCluster, directoryName);
//		List<Process> runningProcesses = new ArrayList<Process>();
		Map<String,Process> runningProcesses = new HashMap<String, Process>();
		for (String address : decision.getRemoteAddresses(directoryCluster, directoryName)) {
			String remoteDirectory = decision.getRemoteDirectory(address);
			String remoteLocation = address + ":" + getPath(remoteDirectory, directoryCluster);
			mkdirs(address, getPath(remoteDirectory, directoryCluster));
			ProcessBuilder processBuilder = new ProcessBuilder("rsync", "-r", "--delete", directory.getAbsolutePath(),
					remoteLocation);
			System.out.println(processBuilder.command());
			runningProcesses.put(address, processBuilder.redirectErrorStream(true).start());
		}
		for (String address : runningProcesses.keySet()) {
			Process p = runningProcesses.get(address);
			dumpToStandardOut(p.getInputStream());
			int status = 1;
			try {
				status = p.waitFor();
				if (status != 0) {
					LOG.error("Replication failed");
				} else {
					decision.setDirectoryLocation(directoryCluster,directoryName,address);
				}
			} catch (InterruptedException e) {
				LOG.error("Interrupted Exception during replication.", e);
			}
		}
	}

	private void mkdirs(String address, String remoteDirectory) throws IOException {
		Process process = new ProcessBuilder("ssh", address, "mkdir", "-p", remoteDirectory).redirectErrorStream(true)
				.start();
		dumpToStandardOut(process.getInputStream());
		try {
			process.waitFor();
		} catch (InterruptedException e) {
			LOG.error("Interrupted Exception", e);
			throw new RuntimeException(e);
		}
	}

	private void dumpToStandardOut(final InputStream inputStream) {
		new Thread(new Runnable() {
			@Override
			public void run() {
				byte[] buf = new byte[2048];
				int num;
				try {
					while ((num = inputStream.read(buf)) != -1) {
						System.out.write(buf, 0, num);
					}
					inputStream.close();
				} catch (IOException e) {
					e.printStackTrace();
					throw new RuntimeException(e);
				}
			}
		}).start();
	}

	private String getPath(String... path) {
		StringBuilder builder = new StringBuilder('/');
		for (String p : path) {
			builder.append(p).append('/');
		}
		return builder.toString().replace("//", "/");
	}

	@Override
	public void process(WatchedEvent event) {

	}

	@Override
	public List<String> listLocalDirectories(String directoryCluster) {
		List<String> result = new ArrayList<String>();
		File file = new File(rsyncBaseDir, directoryCluster);
		for (File f : file.listFiles()) {
			if (f.isDirectory()) {
				result.add(f.getName());
			}
		}
		return result;
	}

	private void rm(File path) {
		if (path.isDirectory()) {
			for (File f : path.listFiles()) {
				rm(f);
			}
		}
		path.delete();
	}
}
