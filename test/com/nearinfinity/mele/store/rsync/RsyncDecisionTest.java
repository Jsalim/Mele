package com.nearinfinity.mele.store.rsync;

import java.io.File;
import java.util.UUID;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import junit.framework.TestCase;

import com.nearinfinity.mele.store.MeleConfiguration;
import com.nearinfinity.mele.store.zookeeper.ZooKeeperFactory;

public class RsyncDecisionTest extends TestCase {
	
	@Override
	protected void setUp() throws Exception {
		ZooKeeperFactory.create(new MeleConfiguration(), new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				
			}
		});
	}

	public void testRsyncDecision() {
		RsyncDecision decision1 = getRsyncDecision("127.0.0.1","./tmp1");
		RsyncDecision decision2 = getRsyncDecision("127.0.0.2","./tmp2");
		RsyncDecision decision3 = getRsyncDecision("127.0.0.3","./tmp3");
		RsyncDecision decision4 = getRsyncDecision("127.0.0.4","./tmp4");
		RsyncDecision decision5 = getRsyncDecision("127.0.0.5","./tmp5");
		System.out.println(decision3.getRegisteredAddresses());
		System.out.println(decision3.getLiveAddresses());
		System.out.println(decision3.getRemoteDirectory("127.0.0.5"));
		System.out.println(decision3.getRemoteAddresses("test", "test"));
		System.out.println(decision3.getRemoteAddresses("test", "test"));
		System.out.println(decision3.getRemoteAddresses("test", "test"));
		System.out.println(decision3.getRemoteAddresses("test", "test"));
	}
	
	private RsyncDecision getRsyncDecision(String localhost,String dir) {
		MeleConfiguration conf = new MeleConfiguration();
		conf.setRsyncBaseDir(new File(dir).getAbsolutePath());
		conf.setRsyncLocalAddress(localhost);
		conf.setRsyncReplicationFactor(3);
		return new RsyncDecision(conf);
	}

}
