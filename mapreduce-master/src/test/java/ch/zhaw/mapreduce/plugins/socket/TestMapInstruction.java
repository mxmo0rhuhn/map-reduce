package ch.zhaw.mapreduce.plugins.socket;

import ch.zhaw.mapreduce.MapEmitter;
import ch.zhaw.mapreduce.MapInstruction;

public class TestMapInstruction implements MapInstruction {

	private static final long serialVersionUID = -5951237460480827687L;

	@Override
	public void map(MapEmitter emitter, String input) {
		emitter.emitIntermediateMapResult("key", input);
	}

}