package ch.zhaw.mapreduce.roundtriptest;

import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import junit.framework.Assert;

import org.junit.Test;

import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.MapEmitter;
import ch.zhaw.mapreduce.MapInstruction;
import ch.zhaw.mapreduce.MapReduceTask;
import ch.zhaw.mapreduce.Pool;
import ch.zhaw.mapreduce.ReduceEmitter;
import ch.zhaw.mapreduce.ReduceInstruction;
import ch.zhaw.mapreduce.Worker;
import ch.zhaw.mapreduce.registry.Registry;

public class PerformanceTest {
	
	@Test
	public void countRandomCharacters() throws InterruptedException {
		Pool pool = Registry.getComponent(Pool.class);
		pool.donateWorker(Registry.getComponent(Worker.class));
		pool.donateWorker(Registry.getComponent(Worker.class));
		pool.donateWorker(Registry.getComponent(Worker.class));
//		pool.donateWorker(Registry.getComponent(Worker.class));
//		pool.donateWorker(Registry.getComponent(Worker.class));
		
		MapReduceTask mapReduce = new MapReduceTask(new MapInstruction() {
			
			@Override
			public void map(MapEmitter emitter, String input) {
				// Anfangsbuchstaben ermitteln
				for (int i = 1; i <= input.length(); i++) {
					emitter.emitIntermediateMapResult(input.substring(i-1, i), "1");
				}
			}
		}, new ReduceInstruction() {
			
			@Override
			public void reduce(ReduceEmitter emitter, String key, Iterator<KeyValuePair> values) {
				long sum = 0;
				while (values.hasNext()) {
					sum += Long.parseLong(values.next().getValue());
				}
				emitter.emit(Long.toString(sum));
			}
		});
		
		// wie viele Strings sollen erstellt werden
		int inputs = (int) Math.pow(10, 2);
		// wie lang soll jeder einzelne sein?
		int chunkSize = (int) Math.pow(10, 2);
		
		
		long start = System.currentTimeMillis();
		Map<String, String> results = mapReduce.compute(generateIterator(inputs, chunkSize));
		long time = System.currentTimeMillis() - start;
		//System.out.println("Time: " + time /*+ ", Results: " + results*/);
		int sum = 0;
		for (Map.Entry<String, String> entry : results.entrySet()) {
			sum += Integer.parseInt(entry.getValue());
		}
		Assert.assertEquals(inputs * chunkSize, sum);
	}
	
	private Iterator<String> generateIterator (final int length, final int chunkSize) {
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
					sb.append((char)(gen.nextInt(93) + 33));
				}
				return sb.toString();
			}

			@Override
			public void remove() {
				throw new IllegalStateException("should never happen");
			}
			
		};
	}

}
