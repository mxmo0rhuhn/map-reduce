package ch.zhaw.mapreduce;

import static org.junit.Assert.*;

import org.junit.Test;

public class MapReduceUtilTest {

	@Test
	public void shouldReadSensibleIpAddress() {
		String ip = MapReduceUtil.getLocalIp();
		assertNotNull(ip);
		assertTrue(ip, ip.startsWith("192") || ip.startsWith("172.") || ip.startsWith("10.") || ip.startsWith("160."));
	}

}
