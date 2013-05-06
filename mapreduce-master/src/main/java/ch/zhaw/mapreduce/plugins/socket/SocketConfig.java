package ch.zhaw.mapreduce.plugins.socket;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import ch.zhaw.mapreduce.Persistence;
import ch.zhaw.mapreduce.Worker;
import ch.zhaw.mapreduce.impl.FilePersistence;

import com.google.inject.AbstractModule;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;

import de.root1.simon.Registry;
import de.root1.simon.Simon;

public class SocketConfig extends AbstractModule {

	@Override
	protected void configure() {
		bind(RegistrationServer.class).to(RegistrationServerImpl.class);
		bind(Integer.class).annotatedWith(Names.named("socket.masterpoolsize")).toInstance(1); // offizieller SIMON IANA
		bind(String.class).annotatedWith(Names.named("socket.mastername")).toInstance("MapReduceSocketMaster");
		bind(ExecutorService.class).annotatedWith(Names.named("socket.workerexecutorservice")).toInstance(
				Executors.newSingleThreadExecutor());

		bind(ServerPluginPartNameMeBetter.class);
		bind(Persistence.class).to(FilePersistence.class);

		bind(String.class).annotatedWith(Names.named("filepersistence.directory")).toInstance(System.getProperty("java.io.tmpdir") + "/mapred/filepers/");
		bind(String.class).annotatedWith(Names.named("filepersistence.ending")).toInstance(".ser");

		install(new FactoryModuleBuilder().implement(Worker.class, SocketWorker.class).build(SocketWorkerFactory.class));

		try {
			bind(Registry.class).toInstance(Simon.createRegistry(4753));
		} catch (Exception e) {
			addError(e);
		}
	}

}
