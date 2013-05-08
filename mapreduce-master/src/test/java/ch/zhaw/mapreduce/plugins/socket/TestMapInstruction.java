package ch.zhaw.mapreduce.plugins.socket;

import ch.zhaw.mapreduce.MapEmitter;
import ch.zhaw.mapreduce.MapInstruction;

public class TestMapInstruction implements MapInstruction {

	@Override
	public void map(MapEmitter emitter, String input) {
		try {
			System.out.println("sleeping");
			Thread.sleep(500);
			System.out.println("done sleeping");
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		emitter.emitIntermediateMapResult("key", input);
	}

}