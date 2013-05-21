package ch.zhaw.mapreduce;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class MapReduceUtil {

	private static final Logger LOG = Logger.getLogger(MapReduceUtil.class.getName());

	public static String getLocalIp() {
		try {
			Enumeration<NetworkInterface> ifs = NetworkInterface.getNetworkInterfaces();
			while (ifs.hasMoreElements()) {
				NetworkInterface iface = ifs.nextElement();
				if (iface.isUp()) {
					Enumeration<InetAddress> adds = iface.getInetAddresses();
					while (adds.hasMoreElements()) {
						InetAddress addr = adds.nextElement();
						if (!isIpV6(addr) && !addr.isAnyLocalAddress() && !addr.isLoopbackAddress()) {
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

	public static Properties loadWithOptionalDefaults(String resource, String filesystem) throws IOException {
		Properties prop = new Properties();
		// defaults vom klassenpfad
		InputStream is = MapReduceUtil.class.getClassLoader().getResourceAsStream(resource);
		if (is == null) {
			throw new FileNotFoundException(resource);
		}
		prop.load(is);
		is.close();
		LOG.log(Level.INFO, "Loaded Default Settings from {0}", resource);
		
		try {
			// einstellungen vom dateisystem
			is = new FileInputStream(filesystem);
			prop.load(is);
			is.close();
			LOG.log(Level.INFO, "Loaded Custom Settings {0}", filesystem);
		} catch (FileNotFoundException e) {
			LOG.log(Level.FINE, "No custom config with filename {0} found", filesystem);
			// ok, config ist optional
		}
		return prop;
	}

}
