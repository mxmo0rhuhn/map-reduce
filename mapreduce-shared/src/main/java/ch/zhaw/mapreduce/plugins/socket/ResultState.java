package ch.zhaw.mapreduce.plugins.socket;

import ch.zhaw.mapreduce.plugins.socket.SocketResultObserver;

/**
 * Diese Klasse repräsentiert der Status, in dem das Resultat eines SocketAgent im SocketResultCollector ist. Da sich
 * ein SocketWorker (SocketResultObserver) beim SocketResultCollector für bestimmte Resultate meldet (registerObserver)
 * kann das Resultat im Status REQUESTED sein. Wenn das Resultat vom Agent an den SocketResultCollector gesandt wurde,
 * ist das Resultat im Status AVAILABLE und muss nur noch vom SocketWorker akzeptiert werden. Dann wird es aus dessen
 * Verwalt gezogen und ist üer die Persistence verfügbar.
 * 
 * Die StateMachine hat also zwei Kanten von einem initialen Zustand:
 * <ul>
 * <li>Resultat wird von SocketAgent gepusht. Zustand: AVAILABLE. Sobald sich ein Observer registriert, wird er
 * notifiziert. END</li>
 * <li>Resultat wird vom SocketWorker (Observer) angefragt. Zustand: REQUESTED. Sobald das Resultat gepusht wird, wird
 * der Observer notifiziert. END</li>
 * </ul>
 * 
 * @author Reto Hablützel (rethab)
 * 
 */
public final class ResultState {

	/**
	 * Entweder ist das Resultat verfügbar oder es wurde nach ihm gefragt. Wenn nach ihm gefragt wurde, ist der observer
	 * in dieser Klasse auch gesetzt.
	 */
	private enum State {
		AVAILABLE, REQUESTED
	}

	/**
	 * Status vom Resultat
	 */
	private final State state;

	/**
	 * Wer das Resultat Requested hat. Ist nur gesetzt, wenn der Status REQUESTED ist.
	 */
	private final SocketResultObserver requestedBy;

	/**
	 * Das Resultat der Berechnung. Nur gesetzt im Status AVAILABLE.
	 */
	private final SocketAgentResult result;
	
	/**
	 * Der timestamp, wann der status erstellt wurde
	 */
	private final long created;

	/**
	 * Konstruktor ist private um ungültige Konstellationen zu vermeiden.
	 */
	private ResultState(State state, SocketResultObserver requestedBy, SocketAgentResult result) {
		this.state = state;
		this.requestedBy = requestedBy;
		this.result = result;
		this.created = System.currentTimeMillis();
	}

	/**
	 * Erstellt neues ResultState Objekt mit dem übergebenen Observer und dem Status REQUESTED.
	 * 
	 * @throws IllegalArgumentException
	 *             wenn der observer null ist
	 */
	public static ResultState requestedBy(SocketResultObserver requestedBy) {
		if (requestedBy == null) {
			throw new IllegalArgumentException("Requestor must not be null");
		}
		return new ResultState(State.REQUESTED, requestedBy, null);
	}

	/**
	 * Erstellt neues ResultState mit dem Status AVAILABLE und ohne Observer
	 */
	public static ResultState resultAvailable(SocketAgentResult res) {
		return new ResultState(State.AVAILABLE, null, res);
	}

	/**
	 * Gibt den Observer zurück. Dieser ist aber nur im Status REQUESTED gesetzt!
	 */
	public SocketResultObserver requestedBy() {
		return this.requestedBy;
	}

	/**
	 * Gibt das Resultat der Berechnung zurück. Wenn der Status REQUESTED ist, gibt diese Methode null zurück.
	 */
	public SocketAgentResult result(){
		return this.result;
	}

	/**
	 * Ob der Status AVAILABLE ist
	 */
	public boolean available() {
		return this.state == State.AVAILABLE;
	}

	/**
	 * Ob der Status REQUESTED ist
	 */
	public boolean requested() {
		return this.state == State.REQUESTED;
	}
	
	/**
	 * Wann der status erstellt wurde
	 */
	public long created() {
		return this.created;
	}

	@Override
	public String toString() {
		if (this.state == State.REQUESTED) {
			return getClass().getSimpleName() + " [State=" + this.state + ",RequestedBy=" + this.requestedBy + "]";
		} else if (this.state == State.AVAILABLE) {
			return getClass().getSimpleName() + " [State=" + this.state + ",Result=" + this.result + "]";
		} else {
			throw new IllegalStateException("Unhandled State: " + this.state);
		}
	}

}
