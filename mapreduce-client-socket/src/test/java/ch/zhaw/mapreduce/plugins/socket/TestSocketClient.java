package ch.zhaw.mapreduce.plugins.socket;


public class TestSocketClient {
	
	public static void main(String[] args) throws Exception {
		SocketClientStarter.main(new String[] {"localhost", "4753", "7"});
	}

}