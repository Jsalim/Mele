package using;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class Inet {

	public static void main(String[] args) {
		System.out.println(getIpAddress("mele1"));
		System.out.println(getIpAddress("mele2"));
		System.out.println(getIpAddress("mele3"));
		System.out.println(getIpAddress("mele4"));
		System.out.println(getIpAddress("mele5"));
		
		System.out.println(getIpAddress("127.0.0.1"));
		System.out.println(getIpAddress("127.0.0.2"));
		System.out.println(getIpAddress("127.0.0.3"));
		System.out.println(getIpAddress("127.0.0.4"));
		System.out.println(getIpAddress("127.0.0.5"));
	}

	private static String getIpAddress(String host) {
		try {
			InetAddress address = InetAddress.getByName(host);
			return address.getHostAddress();
		} catch (UnknownHostException e) {
			throw new RuntimeException(e);
		}
	}

}
