package ch.zhaw.mapreduce.plugins.socket;


public interface SocketAgentFactory {
	
	SocketAgent createSocketAgent(String clientIp);

}
