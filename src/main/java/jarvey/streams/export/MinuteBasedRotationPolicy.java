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
public class MinuteBasedRotationPolicy extends TimeBasedRotationPolicy {
    private static final Logger LOGGER = LoggerFactory.getLogger(MinuteBasedRotationPolicy.class);
    
    private final ZoneId m_zoneId = ZoneId.systemDefault();
    private final int m_nminutes;
    
    public MinuteBasedRotationPolicy(int nminutes) {
    	Utilities.checkArgument(nminutes > 0 && nminutes < 60, String.format("invalid minutes: %d", nminutes));
    	m_nminutes = nminutes;
    }

	@Override
	public Instant getTriggerInstant(Clock clock) {
        LocalDateTime now = LocalDateTime.ofInstant(clock.now(), m_zoneId);
        int curHour = now.getHour();
        int nextMinute = ((now.getMinute() / m_nminutes) + 1) * m_nminutes;
        
        LocalDateTime next = LocalDate.from(now)
        								.atStartOfDay()
        								.plusHours(curHour)
        								.plusMinutes(nextMinute);
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
