package ch.zhaw.mapreduce.mrcomm.json;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;

import ch.zhaw.mapreduce.mrcomm.CommException;
import ch.zhaw.mapreduce.mrcomm.RegisterComm;
import ch.zhaw.mapreduce.mrcomm.VersionMismatchException;

public class JsonRegisterComm implements RegisterComm {

	public static final String VERSION_NUMBER = "1.0";
	
	public static final String VERSION_ID = "Version";

	private static final JSONObject JSON_VERSION;
	
	static {
		try {
			JSON_VERSION = new JSONObject().put(VERSION_ID, VERSION_NUMBER);
		} catch (JSONException e) {
			throw new IllegalStateException("Cannot happen!");
		}
	}

	@Override
	public String encodeServerResponseSuccess(String clientID) throws CommException {
		try {
			JSONArray ary = new JSONArray(
					Arrays.asList(new JSONObject[] { JSON_VERSION, new JSONObject().put("State", "Success"),
							new JSONObject().put("Msg", "Added Worker: " + clientID) }));
			return new JSONStringer().object().key("Response").value(ary).endObject().toString();
		} catch (JSONException e) {
			throw new CommException(e);
		}
	}

	@Override
	public String encodeServerResponseFail(String errMsg) throws CommException {
		try {
			JSONArray ary = new JSONArray(Arrays.asList(new JSONObject[] { JSON_VERSION,
					new JSONObject().put("State", "Failure"), new JSONObject().put("Msg", errMsg) }));
			return new JSONStringer().object().key("Response").value(ary).endObject().toString();
		} catch (JSONException e) {
			throw new CommException(e);
		}
	}

	@Override
	public String encodeClientRequest(String clientID, String device) throws CommException {
		try {
			JSONArray ary = new JSONArray(Arrays.asList(new JSONObject[] { JSON_VERSION,
					new JSONObject().put("Device", device), new JSONObject().put("ClientID", clientID) }));
			return new JSONStringer().object().key("Request").value(ary).endObject().toString();
		} catch (JSONException e) {
			throw new CommException(e);
		}
	}

	@Override
	public Map<String, String> decodeServerResponse(String json) throws CommException {
		try {
			Map<String, String> result = new HashMap<String, String>();
			JSONArray resp = new JSONObject(json).getJSONArray("Response");
			checkVersion(resp);
			result.put("State", resp.getJSONObject(1).getString("State"));
			result.put("Msg", resp.getJSONObject(2).getString("Msg"));
			return result;
		} catch (JSONException e) {
			throw new CommException(e);
		}
	}

	@Override
	public Map<String, String> decodeClientRequest(String json) throws CommException {
		try {
			Map<String, String> result = new HashMap<String, String>();
			JSONArray req = new JSONObject(json).getJSONArray("Request");
			checkVersion(req);
			result.put("Device", req.getJSONObject(1).getString("Device"));
			result.put("ClientID", req.getJSONObject(2).getString("ClientID"));
			return result;
		} catch (JSONException e) {
			throw new CommException(e);
		}
	}

	private void checkVersion(JSONArray ary) throws VersionMismatchException, JSONException {
		String version = ary.getJSONObject(0).getString(VERSION_ID);
		if (!VERSION_NUMBER.equals(version)) {
			throw new VersionMismatchException(VERSION_NUMBER, version);
		}
	}

}