package ch.zhaw.mapreduce.android;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Map;

import javax.inject.Inject;

import ch.zhaw.mapreduce.Pool;
import ch.zhaw.mapreduce.mrcomm.CommException;
import ch.zhaw.mapreduce.mrcomm.RegisterComm;
import ch.zhaw.mapreduce.workers.Worker;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

public class AndroidRegistrationHandler implements HttpHandler {

	private final Pool pool;

	private final RegisterComm comm;
	
	private final AndroidWorkerFactory factory;

	@Inject
	public AndroidRegistrationHandler(Pool pool, RegisterComm comm, AndroidWorkerFactory factory) {
		this.pool = pool;
		this.comm = comm;
		this.factory = factory;
	}

	@Override
	public void handle(HttpExchange exchg) throws IOException {
		// TODO set content type
		if ("POST".equals(exchg.getRequestMethod())) {
			InputStream is = exchg.getRequestBody();
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			StringBuilder sb = new StringBuilder();
			String line;
			while ((line = br.readLine()) != null) {
				sb.append(line);
			}

			try {
				// TODO validation
				Map<String, String> params = comm.decodeClientRequest(sb.toString());
				String device = params.get("Device");
				String clientID = params.get("ClientID");
				AndroidWorker worker = factory.createWorker(clientID, device);
				pool.donateWorker(worker);
			} catch (CommException e) {
				// LOG
				try {
					String json = comm.encodeServerResponseFail("Invalid Request");
					BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(exchg.getResponseBody()));
					bw.append(json);
				} catch (CommException e1) {
					// LOG
				}
			}

		}

	}

}
