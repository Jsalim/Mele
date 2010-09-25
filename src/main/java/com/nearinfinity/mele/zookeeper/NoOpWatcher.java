package com.nearinfinity.mele.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class NoOpWatcher implements Watcher {

    @Override
    public void process(WatchedEvent event) {
    }

}
