package ch.zhaw.mapreduce.plugins.socket;

import ch.zhaw.mapreduce.MapEmitter;
import ch.zhaw.mapreduce.MapInstruction;

public class TestMapInstruction implements MapInstruction {

	@Override
	public void map(MapEmitter emitter, String input) {
		System.out.println("sleeping");
		// Thread.sleep(500);
		System.out.println("done sleeping");
		emitter.emitIntermediateMapResult("key", input);
	}

}