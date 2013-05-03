package ch.zhaw.mapreduce.roundtriptest;

import ch.zhaw.mapreduce.MapEmitter;
import ch.zhaw.mapreduce.MapInstruction;

/**
 * Splittet jedes Wort und Emit'ed es an MapWorkerTask
 * 
 * @author des
 * 
 */
public class WordFrequencyMapInstruction implements MapInstruction {
	
	private static final String ONE = "1";

	/** {@inheritDoc} */
	@Override
	public void map(MapEmitter emitter, String input) {
		for (String s : input.trim().split(" ")) {
			emitter.emitIntermediateMapResult(s, ONE);
		}
	}

}
