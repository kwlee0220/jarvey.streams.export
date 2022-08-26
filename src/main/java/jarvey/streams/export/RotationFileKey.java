package jarvey.streams.export;

import java.util.Objects;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
final class RotationFileKey {
	final String m_topic;
	final int m_partition;
	
	RotationFileKey(String topic, int partition) {
		m_topic = topic;
		m_partition = partition;
	}
	
	public String getTopic() {
		return m_topic;
	}
	
	public int getPartition() {
		return m_partition;
	}
	
	@Override
	public String toString() {
		return String.format("%s(%d)", m_topic, m_partition);
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || obj.getClass() != getClass() ) {
			return false;
		}
		
		RotationFileKey other = (RotationFileKey)obj;
		return Objects.equals(m_topic, other.m_topic)
				&& Objects.equals(m_partition, other.m_partition);
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(m_topic, m_partition);
	}
}