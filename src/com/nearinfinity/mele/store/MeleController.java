package com.nearinfinity.mele.store;

import java.io.IOException;

import com.nearinfinity.mele.Mele;
import com.nearinfinity.mele.store.cassandra.CassandraMele;
import com.nearinfinity.mele.store.hdfs.HdfsMele;
import com.nearinfinity.mele.store.rsync.RsyncMele;

public class MeleController {
	
	private static Mele mele;
	
	/**
	 * Single instance of Mele per jvm and has to be thread safe.
	 * @return the single Mele instance.
	 */
	public static synchronized Mele getInstance(MeleConfiguration configuration) {
		if (mele == null) {
			try {
				if (configuration.isUsingCassandra()) {
					mele = new CassandraMele(configuration);
				} else if (configuration.isUsingRync()) {
					mele = new RsyncMele(configuration);
				} else if (configuration.isUsingHdfs()) {
					mele = new HdfsMele(configuration);
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		return mele;
	}
}
