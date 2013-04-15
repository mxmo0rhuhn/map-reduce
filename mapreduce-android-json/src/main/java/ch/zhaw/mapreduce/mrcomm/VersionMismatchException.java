package ch.zhaw.mapreduce.mrcomm;


public class VersionMismatchException extends CommException {
	
	private static final long serialVersionUID = 1L;

	private final String expectedVersion;
	
	private final String actualVersion;

	public VersionMismatchException(String expectedVersion, String actualVersion) {
		this.expectedVersion = expectedVersion;
		this.actualVersion = actualVersion;
	}

	public String getExpectedVersion() {
		return expectedVersion;
	}

	public String getActualVersion() {
		return actualVersion;
	}
	
	@Override
	public String getMessage() {
		return "Expected Version " + this.expectedVersion + ", but got: " + this.actualVersion;
	}

}
