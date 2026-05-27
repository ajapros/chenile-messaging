package org.chenile.cloudedgeswitch.interceptor.errorcodes;

public enum ErrorCodes {
	
	PROCESSED_LOCALLY("8000"), ERROR_DETAIL("8001"), CANNOT_INVOKE_CLOUD("8002"),
	LOCAL_SERVICE_FAILED("8003")
	;
	final String subError;
	private ErrorCodes(String subError) {
		this.subError = subError;
	}
	
	public String getSubError() {
		return this.subError;
	}
}
