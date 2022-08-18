package jarvey.streams.export;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class Globals {
	private Globals() {
		throw new AssertionError("Should not be called here: class=" + getClass());
	}
	
	private static final Logger s_logger = LoggerFactory.getLogger(Globals.class.getPackage().getName());
	public static final Logger getLogger() {
		return s_logger;
	}
}
