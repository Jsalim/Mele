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

package com.nearinfinity.mele.store.util;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

/** @author Aaron McCurry (amccurry@nearinfinity.com) */
public class Bytes {

    private static final String UTF_8 = "UTF-8";
    public static final byte[] EMPTY_BYTE_ARRAY = new byte[]{ };

    public static byte[] toBytes(String s) {
        try {
            return s.getBytes(UTF_8);
        }
        catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public static String toString(byte[] bs) {
        try {
            return new String(bs, UTF_8);
        }
        catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] toBytes(long l) {
        return ByteBuffer.allocate(8).putLong(l).array();
    }

    public static long toLong(byte[] bs) {
        return ByteBuffer.wrap(bs).getLong();
    }
}
