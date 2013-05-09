package ch.zhaw.mapreduce.plugins.socket;

/**
 * Wenn der SocketWorker dem SocketAgent einen Task zu Ausführung gibt, kriegt er einen Status zurück, ob der Task
 * akzeptiert wurde etc.
 * 
 * @author Reto Hablützel (rethab)
 * 
 */
public final class AgentTaskState {

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
