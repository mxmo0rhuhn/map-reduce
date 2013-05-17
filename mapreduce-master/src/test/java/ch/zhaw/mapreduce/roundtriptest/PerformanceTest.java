package ch.zhaw.mapreduce.roundtriptest;

import static org.junit.Assert.assertEquals;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.Test;

import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.MapEmitter;
import ch.zhaw.mapreduce.MapInstruction;
import ch.zhaw.mapreduce.MapReduceConfig;
import ch.zhaw.mapreduce.Master;
import ch.zhaw.mapreduce.ReduceEmitter;
import ch.zhaw.mapreduce.ReduceInstruction;
import ch.zhaw.mapreduce.plugins.AgentPlugin;
import ch.zhaw.mapreduce.plugins.Loader;

import com.google.inject.Guice;
import com.google.inject.Injector;

public class PerformanceTest {

	@Test
	public void countRandomCharacters() throws Exception {
		System.setProperty("mrplugins", "Thread");
		Injector injector = Guice.createInjector(new MapReduceConfig());
		Master master = injector.getInstance(Master.class);
		Loader pluginLoader = injector.getInstance(Loader.class);
		for (AgentPlugin plugin : pluginLoader.loadPlugins()) {
			plugin.start(injector);
		}


		// wie viele Strings sollen erstellt werden
		int inputs = (int) Math.pow(10, 2);
		// wie lang soll jeder einzelne sein?
		int chunkSize = (int) Math.pow(10, 2);

		long start = System.currentTimeMillis();
		Map<String, List<String>> results = master.runComputation(mapInstr, null, redInstr, null, generateIterator(inputs, chunkSize));
		long time = System.currentTimeMillis() - start;
		// System.out.println("Time: " + time /*+ ", Results: " + results*/);
		int sum = 0;
		for (Map.Entry<String, List<String>> entry : results.entrySet()) {
			for (String val : entry.getValue()) {
				sum += Integer.parseInt(val);
			}
		}
		assertEquals(inputs * chunkSize, sum);
	}

	private Iterator<String> generateIterator(final int length, final int chunkSize) {
		// alles zwischen 33 und 126 sind mehr oder weniger druckbare zeichen
		return new Iterator<String>() {

			private final Random gen = new Random();

			private int served = 0;

			@Override
			public boolean hasNext() {
				return served < length;
			}

			@Override
			public String next() {
				served++;
				StringBuilder sb = new StringBuilder(chunkSize);
				for (int i = 0; i < chunkSize; i++) {
					sb.append((char) (gen.nextInt(93) + 33));
				}
				return sb.toString();
			}

			@Override
			public void remove() {
				throw new IllegalStateException("should never happen");
			}

		};
	}

	private MapInstruction mapInstr = new MapInstruction() {

		@Override
		public void map(MapEmitter emitter, String input) {
			// Anfangsbuchstaben ermitteln
			for (int i = 1; i <= input.length(); i++) {
				emitter.emitIntermediateMapResult(input.substring(i - 1, i), "1");
			}
		}
	};
	
	private ReduceInstruction redInstr = new ReduceInstruction() {

		@Override
		public void reduce(ReduceEmitter emitter, String key, Iterator<KeyValuePair> values) {
			long sum = 0;
			while (values.hasNext()) {
				sum += Long.parseLong((String) values.next().getValue());
			}
			emitter.emit(Long.toString(sum));
		}
	};

}
