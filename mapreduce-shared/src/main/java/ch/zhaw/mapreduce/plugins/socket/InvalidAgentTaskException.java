package ch.zhaw.mapreduce.plugins.socket;

public class InvalidAgentTaskException extends Exception {
	
	private static final long serialVersionUID = -895729702141702753L;

	public InvalidAgentTaskException(Throwable cause) {
		super(cause);
	}
	
	public InvalidAgentTaskException(String msg) {
		super(msg);
	}

}
