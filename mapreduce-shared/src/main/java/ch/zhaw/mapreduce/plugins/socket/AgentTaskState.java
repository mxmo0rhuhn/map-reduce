package ch.zhaw.mapreduce.plugins.socket;

import java.io.Serializable;

/**
 * Wenn der SocketWorker dem SocketAgent einen Task zu Ausführung gibt, kriegt er einen Status zurück, ob der Task
 * akzeptiert wurde etc.
 * 
 * @author Reto Hablützel (rethab)
 * 
 */
public final class AgentTaskState implements Serializable {

	private static final long serialVersionUID = 741887843664884234L;

	public enum State {
		/**
		 * Task wurde zur Ausführung aufgegeben
		 */
		ACCEPTED,

		/**
		 * Task konnte nicht zur Ausführung aufgegeben werden
		 */
		REJECTED;
	}

	private final State state;

	private final String msg;

	public AgentTaskState(State state, String msg) {
		this.state = state;
		this.msg = msg;
	}
	
	public AgentTaskState(State state) {
		this(state, "");
	}

	public State state() {
		return this.state;
	}

	public String msg() {
		return this.msg;
	}
	
	@Override
	public String toString() {
		return getClass().getSimpleName() + "[State="+this.state+",Msg="+this.msg+"]";
	}

}
