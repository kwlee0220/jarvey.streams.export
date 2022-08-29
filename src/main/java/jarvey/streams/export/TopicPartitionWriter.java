package jarvey.streams.export;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.utils.Bytes;

import com.vlkan.rfos.RotatingFileOutputStream;

import utils.func.Try;
import utils.func.Unchecked;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class TopicPartitionWriter {
	private static final byte[] NEWLINE = "\n".getBytes(StandardCharsets.UTF_8);
	
	final ByteArrayOutputStream m_buffer;
	final RotatingFileOutputStream m_rfos;
	final int m_maxBufSize;
	volatile long m_lastOffset = -1;

	TopicPartitionWriter(RotatingFileOutputStream rfos, int bufSize) {
		m_rfos = rfos;
		m_buffer = new ByteArrayOutputStream(bufSize);
		m_maxBufSize = bufSize;
	}
	
	Try<Integer> write(List<ConsumerRecord<Bytes,Bytes>> records) {
		try {
			int totalSize = 0;
			for ( ConsumerRecord<Bytes,Bytes> record: records ) {
				m_buffer.write(record.value().get());
				m_buffer.write(NEWLINE);
				
				if ( m_buffer.size() >= m_maxBufSize ) {
					m_rfos.write(m_buffer.toByteArray());
					m_rfos.flush();
					
					totalSize += m_buffer.size();
					m_buffer.reset();
				}
				m_lastOffset = record.offset();
			}
			
			m_rfos.write(m_buffer.toByteArray());
			m_rfos.flush();
			
			totalSize += m_buffer.size();
			m_buffer.reset();
			
			return Try.success(totalSize);
		}
		catch ( IOException e ) {
			return Try.failure(e);
		}
	}
	
	public void close() {
		Globals.LOGGER_ROTATION.info("closing the rotating-file: {}", m_rfos.getFile());
		
		Unchecked.runOrIgnore(m_rfos::close);
		Unchecked.runOrIgnore(m_buffer::close);
	}
	
	public OffsetAndMetadata getOffsetAndMeta() {
		return new OffsetAndMetadata(m_lastOffset+1);
	}
}
