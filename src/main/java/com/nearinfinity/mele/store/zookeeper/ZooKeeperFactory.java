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

package com.nearinfinity.mele.store.zookeeper;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.nearinfinity.mele.MeleConfiguration;

/** @author Aaron McCurry (amccurry@nearinfinity.com) */
public class ZooKeeperFactory {

    private static final Log LOG = LogFactory.getLog(ZooKeeperFactory.class);
    private static ZooKeeper zk;

    public static ZooKeeper create(MeleConfiguration configuration, Watcher watcher) throws IOException {
        zk = new ZooKeeper(configuration.getZooKeeperConnectionString(), configuration.getZooKeeperSessionTimeout(),
                watcher);
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    System.out.println("Closing zookeeper client.");
                    zk.close();
                } catch (InterruptedException e) {
                    LOG.error("Unknown error while closing zookeeper client.", e);
                }
            }
        }));
        return zk;
    }

    public static synchronized void close() throws InterruptedException {
        zk.close();
    }

    public static synchronized ZooKeeper getZooKeeper() {
        return zk;
    }
}
