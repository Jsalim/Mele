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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.zookeeper.WatchedEvent;

import com.nearinfinity.mele.MeleConfiguration;
import com.nearinfinity.mele.store.BaseMele;
import com.nearinfinity.mele.store.zookeeper.ZookeeperIndexDeletionPolicy;
import com.nearinfinity.mele.store.zookeeper.ZookeeperWrapperDirectory;

/** @author Aaron McCurry (amccurry@nearinfinity.com) */
public class HdfsMele extends BaseMele {

    public HdfsMele(MeleConfiguration configuration) throws IOException {
        super(configuration);
    }

    @Override
    public void process(WatchedEvent event) {

    }

    @Override
    public List<String> listLocalDirectories(String directoryCluster) {
        List<String> result = new ArrayList<String>();
        for (String localPath : pathList) {
            File file = new File(localPath, directoryCluster);
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

}
