package ch.zhaw.mapreduce.plugins.android;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import ch.zhaw.mapreduce.mrcomm.RegisterComm;
import ch.zhaw.mapreduce.mrcomm.json.JsonRegisterComm;
import ch.zhaw.mapreduce.plugins.AgentAdapter;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;
import com.sun.net.httpserver.HttpHandler;

public class AndroidConfig extends AbstractModule {

	@Override
	protected void configure() {
		bind(AgentAdapter.class).to(AndroidAgentAdapter.class);
		bind(HttpHandler.class).to(AndroidRegistrationHandler.class);
		bind(RegisterComm.class).to(JsonRegisterComm.class);

		install(new FactoryModuleBuilder().implement(AndroidWorker.class, AndroidWorker.class).build(
				AndroidWorkerFactory.class));

		bind(String.class).annotatedWith(Names.named("GCM_API_KEY")).toInstance(
				"AIzaSyD-5CCw5L7oMij3i2OGa2Ww5Tk_YksTDyA");
		bind(String.class).annotatedWith(Names.named("GCM_PROJECT_ID")).toInstance("367594230701");
		bind(Integer.class).annotatedWith(Names.named("GCM_TimeToLive")).toInstance(10);
		bind(Integer.class).annotatedWith(Names.named("GCM_Retries")).toInstance(2);

		bind(String.class).annotatedWith(Names.named("androidRegisterCtx")).toInstance("/registerAndroid");
		bind(Integer.class).annotatedWith(Names.named("androidRegisterPort")).toInstance(8081);

	}

	@Provides
	@AndroidRegisterExecutor
	public Executor createAndroidRegisterExec() {
		return Executors.newSingleThreadExecutor();
	}

}
