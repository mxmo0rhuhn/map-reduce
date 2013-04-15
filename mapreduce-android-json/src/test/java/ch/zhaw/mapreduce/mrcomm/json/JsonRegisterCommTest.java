package ch.zhaw.mapreduce.mrcomm.json;

import static ch.zhaw.mapreduce.mrcomm.json.JsonRegisterComm.VERSION_ID;
import static ch.zhaw.mapreduce.mrcomm.json.JsonRegisterComm.VERSION_NUMBER;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Map;

import org.junit.Test;

import ch.zhaw.mapreduce.mrcomm.CommException;
import ch.zhaw.mapreduce.mrcomm.VersionMismatchException;

public class JsonRegisterCommTest {

	private JsonRegisterComm json = new JsonRegisterComm();

	@Test(expected = CommException.class)
	public void shouldNotAcceptGarbage() throws CommException {
		json.decodeClientRequest("foo");
	}

	@Test(expected = VersionMismatchException.class)
	public void shouldNotAcceptOtherVersion() throws CommException {
		json.decodeClientRequest("{\"Request\":[{\"" + VERSION_ID
				+ "\":\"42.75\"},{\"Device\":\"Galaxy Nexus\"},{\"ClientID\":\"1234ABC\"}]}");
	}

	@Test
	public void testDecondingSuccess() throws CommException {
		assertThat(json.encodeServerResponseSuccess("127.0.0.1"), is(equalTo("{\"Response\":[{\"" + VERSION_ID
				+ "\":\"" + VERSION_NUMBER + "\"},{\"State\":\"Success\"},{\"Msg\":\"Added Worker: 127.0.0.1\"}]}")));
	}

	@Test
	public void testEncodingFailure() throws CommException {
		assertThat(json.encodeServerResponseFail("Reason"), is(equalTo("{\"Response\":[{\"" + VERSION_ID + "\":\""
				+ VERSION_NUMBER + "\"},{\"State\":\"Failure\"},{\"Msg\":\"Reason\"}]}")));
	}

	@Test
	public void testDecodingClientRequest() throws CommException {
		Map<String, String> result = json.decodeClientRequest("{\"Request\":[{\"" + VERSION_ID + "\":\""
				+ VERSION_NUMBER + "\"},{\"Device\":\"Galaxy Nexus\"},{\"ClientID\":\"1234ABC\"}]}");
		assertThat("1234ABC", is(equalTo(result.get("ClientID"))));
		assertThat("Galaxy Nexus", is(equalTo(result.get("Device"))));
	}

	@Test
	public void testDecodingServerResponseSuccess() throws CommException {
		Map<String, String> result = json.decodeServerResponse("{\"Response\":[{\"" + VERSION_ID + "\":\""
				+ VERSION_NUMBER + "\"},{\"State\":\"Success\"},{\"Msg\":\"Added Worker: 127.0.0.1\"}]}");
		assertThat("Added Worker: 127.0.0.1", is(equalTo(result.get("Msg"))));
		assertThat("Success", is(equalTo(result.get("State"))));
	}

	@Test
	public void testDecodingServerResponseFailure() throws CommException {
		Map<String, String> result = json.decodeServerResponse("{\"Response\":[{\"" + VERSION_ID + "\":\""
				+ VERSION_NUMBER + "\"},{\"State\":\"Failure\"},{\"Msg\":\"No IP Submitted\"}]}");
		assertThat("No IP Submitted", is(equalTo(result.get("Msg"))));
		assertThat("Failure", is(equalTo(result.get("State"))));
	}

}