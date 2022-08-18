package jarvey.streams.export;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vlkan.rfos.Clock;
import com.vlkan.rfos.policy.TimeBasedRotationPolicy;

import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class HourBasedRotationPolicy extends TimeBasedRotationPolicy {
    private static final Logger LOGGER = LoggerFactory.getLogger(HourBasedRotationPolicy.class);
    
    private final ZoneId m_zoneId = ZoneId.systemDefault();
    private final int m_nhours;
    
    public HourBasedRotationPolicy(int nhours) {
    	Utilities.checkArgument(nhours > 0 && nhours < 24, String.format("invalid hours: %d", nhours));
    	m_nhours = nhours;
    }

	@Override
	public Instant getTriggerInstant(Clock clock) {
        LocalDateTime now = LocalDateTime.ofInstant(clock.now(), m_zoneId);
        int nextHour = ((now.getHour() / m_nhours) + 1) * m_nhours;
        
        LocalDateTime next = LocalDate.from(now).atStartOfDay().plusHours(nextHour);
        if ( now.getDayOfMonth() != next.getDayOfMonth() ) {
        	next = LocalDate.from(next).atStartOfDay();
        }
        
        return next.atZone(m_zoneId).toInstant();
	}

	@Override
	protected Logger getLogger() {
		return LOGGER;
	}
}
