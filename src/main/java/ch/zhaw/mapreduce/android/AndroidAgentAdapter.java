package ch.zhaw.mapreduce.android;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executor;

import javax.inject.Inject;
import javax.inject.Named;

import ch.zhaw.mapreduce.AdapterException;
import ch.zhaw.mapreduce.AgentAdapter;
import ch.zhaw.mapreduce.registry.AndroidRegisterExecutor;

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public class AndroidAgentAdapter implements AgentAdapter {

	private final String androidRegisterCtx;

	private final int androidRegisterPort;

	private final Executor srvExecutor;
	
	private final HttpHandler handler;

	@Inject
	public AndroidAgentAdapter(@Named("androidRegisterCtx") String registerCtx,
			@Named("androidRegisterPort") int registerPort, @AndroidRegisterExecutor Executor handlerExecutor, HttpHandler handler) {
		this.androidRegisterCtx = registerCtx;
		this.androidRegisterPort = registerPort;
		this.srvExecutor = handlerExecutor;
		this.handler = handler;
	}

	@Override
	public void start() throws AdapterException {
		try {
			InetSocketAddress addr = new InetSocketAddress(androidRegisterPort);
			HttpServer srv = HttpServer.create(addr, 0); // TODO what's that 0 ??
			srv.createContext(androidRegisterCtx, handler);
			srv.setExecutor(srvExecutor);
			srv.start();
			// TODO log
		} catch (IOException io) {
			throw new AdapterException(io);
		}
	}


}
