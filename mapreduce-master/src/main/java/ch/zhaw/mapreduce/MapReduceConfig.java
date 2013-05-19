package ch.zhaw.mapreduce;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;

import javax.inject.Named;

import ch.zhaw.mapreduce.impl.FilePersistence;
import ch.zhaw.mapreduce.impl.InMemoryShuffler;
import ch.zhaw.mapreduce.plugins.Loader;
import ch.zhaw.mapreduce.plugins.socket.impl.NamedThreadFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.matcher.Matchers;
import com.google.inject.name.Names;

/**
 * In dieser Klasse befinden sich die Bindings für Guice - also welche Implementationen für welche Interfaces verwendet
 * werden sollen.
 * 
 * @author Reto
 * 
 */
public class MapReduceConfig extends AbstractModule {
	
	private static final Logger LOG = Logger.getLogger(MapReduceConfig.class.getName());

	/**
	 * Binded verschiedene Interfaces zu den zugehörigen Implementationen.
	 */
	@Override
	protected void configure() {
		initNamedProperties();

		bind(Master.class);
		bind(Pool.class);
		bind(Loader.class);
		bind(Shuffler.class).to(InMemoryShuffler.class);
		bind(Persistence.class).to(FilePersistence.class);

		install(new FactoryModuleBuilder().build(WorkerTaskFactory.class));

		// see PostConstructFeature
		bindListener(Matchers.any(), new PostConstructFeature());
	}

	private void initNamedProperties() {
		Properties prop = new Properties();
		try {
			// defaults vom klassenpfad
			InputStream is = getClass().getClassLoader().getResourceAsStream("mapreduce-defaults.properties");
			if (is == null) {
				addError(new FileNotFoundException("mapreduce-defaults.properties"));
			}
			prop.load(is);
			is.close();
			LOG.fine("Loaded Default Settings");
		} catch (IOException e) {
			addError(e);
		}
		try {
			// einstellungen vom dateisystem
			InputStream is = new FileInputStream("mapreduce.properties");
			prop.load(is);
			is.close();
			LOG.info("Loaded Custom Settings");
		} catch (FileNotFoundException e) {
			LOG.fine("No custom config found");
			// ok, config ist optional
		} catch (IOException e) {
			addError(e);
		}
		Names.bindProperties(binder(), prop);
	}
	
	@Provides
	@Named("taskUuid")
	private String genTaskUuid() {
		return UUID.randomUUID().toString();
	}

	@Provides
	@Named("poolExecutor")
	private Executor createPoolExec() {
		return Executors.newSingleThreadExecutor(new NamedThreadFactory("PoolExecutor"));
	}

	@Provides
	@Named("supervisorScheduler")
	private ScheduledExecutorService poolSupervisor() {
		return Executors.newScheduledThreadPool(1, new NamedThreadFactory("PoolSupervisor"));
	}
}
