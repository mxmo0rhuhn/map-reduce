package ch.zhaw.mapreduce.impl;

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
 *  <li>Resultat wird von SocketAgent gepusht. Zustand: AVAILABLE. Sobald sich ein Observer registriert, wird er notifiziert. END</li>
 *  <li>Resultat wird vom SocketWorker (Observer) angefragt. Zustand: REQUESTED. Sobald das Resultat gepusht wird, wird der Observer notifiziert. END</li>
 * </ul>
 * 
 * @author Reto Hablützel (rethab)
 * 
 */
final class ResultState {

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
	 * Konstruktor ist private um ungültige Konstellationen zu vermeiden.
	 */
	private ResultState(State state, SocketResultObserver requestedBy) {
		this.state = state;
		this.requestedBy = requestedBy;
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
		return new ResultState(State.REQUESTED, requestedBy);
	}

	/**
	 * Erstellt neues ResultState mit dem Status AVAILABLE und ohne Observer
	 */
	public static ResultState resultAvailable() {
		return new ResultState(State.AVAILABLE, null);
	}

	/**
	 * Gibt den Observer zurück. Dieser ist aber nur im Status REQUESTED gesetzt!
	 */
	public SocketResultObserver requestedBy() {
		return this.requestedBy;
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

}
