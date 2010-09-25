package com.nearinfinity.mele.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class AddressUtil {
    public static InetAddress getMyAddress() throws UnknownHostException {
        return InetAddress.getLocalHost();
    }
    public static String getMyHostName() throws UnknownHostException {
        return getMyAddress().getHostName();
    }
}
