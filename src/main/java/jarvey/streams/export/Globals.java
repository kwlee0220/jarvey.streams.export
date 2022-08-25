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
	
	public static final Logger LOGGER = LoggerFactory.getLogger(Globals.class.getPackage().getName());
	public static final Logger LOGGER_ROTATION = LoggerFactory.getLogger(LOGGER.getName() + ".rotation");
}
