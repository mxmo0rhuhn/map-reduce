package ch.zhaw.mapreduce.mrcomm;

import java.util.Map;


public interface RegisterComm {

	String encodeServerResponseSuccess(String clientID) throws CommException;

	String encodeServerResponseFail(String errMsg) throws CommException;
	
	String encodeClientRequest(String clientID, String device) throws CommException;
	
	Map<String, String> decodeServerResponse(String json) throws CommException;
	
	Map<String, String> decodeClientRequest(String json) throws CommException;
	
}
