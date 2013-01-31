package ch.zhaw.mapreduce.plugins.android;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executor;
import java.util.logging.Logger;

import javax.inject.Inject;
import javax.inject.Named;

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public class AndroidAgentAdapter {
	
	@Inject
	private Logger logger;

	private final String androidRegisterCtx;

	private final int androidRegisterPort;

	private final Executor srvExecutor;

	private final HttpHandler handler;
	
	private HttpServer srv;

	@Inject
	public AndroidAgentAdapter(@Named("androidRegisterCtx") String registerCtx,
			@Named("androidRegisterPort") int registerPort, @AndroidRegisterExecutor Executor handlerExecutor,
			HttpHandler handler) {
		this.androidRegisterCtx = registerCtx;
		this.androidRegisterPort = registerPort;
		this.srvExecutor = handlerExecutor;
		this.handler = handler;
	}

	public void start() throws IOException {
		InetSocketAddress addr = new InetSocketAddress(androidRegisterPort);
		srv = HttpServer.create(addr, 0); // TODO what's that 0 ??
		srv.createContext(androidRegisterCtx, handler);
		srv.setExecutor(srvExecutor);
		srv.start();
		logger.info("Android Adapter started on Port " + androidRegisterPort + " with Context " + androidRegisterCtx);
	}

	public void stop() {
		// TODO srv.stop(arg0);
	}
}
