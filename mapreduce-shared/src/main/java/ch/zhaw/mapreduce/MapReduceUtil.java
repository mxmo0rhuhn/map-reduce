package ch.zhaw.mapreduce;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

public final class MapReduceUtil {

	public static String getLocalIp() {
		try {
			Enumeration<NetworkInterface> ifs = NetworkInterface.getNetworkInterfaces();
			while (ifs.hasMoreElements()) {
				NetworkInterface iface = ifs.nextElement();
				if (iface.isUp()) {
					Enumeration<InetAddress> adds = iface.getInetAddresses();
					while (adds.hasMoreElements()) {
						InetAddress addr = adds.nextElement();
						if (!isIpV6(addr) && !addr.isAnyLocalAddress()) {
							return addr.getHostAddress();
						}
					}
				}
			}
		} catch (SocketException se) {
			se.printStackTrace();
		}
		return "NOIP";
	}
	
	private static boolean isIpV6(InetAddress addr) {
		return addr.getHostAddress().contains(":");
	}

}
