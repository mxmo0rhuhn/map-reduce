package ch.zhaw.mapreduce.plugins.socket;

/**
 * 
 * @author Reto Hablützel (rethab)
 *
 */
public interface RegistrationServer {
	
	public void register(String ip, int port, ClientCallback clientCallback);

}