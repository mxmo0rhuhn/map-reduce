package ch.zhaw.mapreduce.plugins.socket;

import de.root1.simon.Lookup;
import de.root1.simon.Simon;

/**
 * 
 * @author Reto Hablützel (rethab)
 *
 */
public class SocketClientStarter {

	public static void main(String[] args) throws Exception {
		ClientCallback cb = new ClientCallback() {
			
			@Override
			public void acknowledge() {
				System.out.println("Acknowledged");
			}
		};
		
		Lookup nameLookup = Simon.createNameLookup(args[0], Integer.parseInt(args[1]));
		RegistrationServer reg = (RegistrationServer) nameLookup.lookup(args[2]); // master server registry name?
		reg.register("127.0.0.1", 666, cb);
		
		nameLookup.release(reg);
	}

}
