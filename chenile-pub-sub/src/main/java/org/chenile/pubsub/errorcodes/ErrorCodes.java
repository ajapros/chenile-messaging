package org.chenile.pubsub.errorcodes;

/**
 * Chenile MQTT error codes.
 */
public enum ErrorCodes {

	MISCONFIGURATION("900"), UNSUPPORTED_TOPIC_FORMAT_FOR_OPERATION("901"), UNSUPPORTED_TOPIC_FORMAT_FOR_SERVICE("902"),
	MISSING_SERVICE("903"), MISSING_SERVICE_OPERATION("904"), CANNOT_FIND_TOPIC("905");

	final String subError;
	private ErrorCodes(String subError) {
		this.subError = subError;
	}
	
	public String getSubError() {
		return this.subError;
	}
}
