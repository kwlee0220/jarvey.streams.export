package jarvey.streams.export;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.utils.Bytes;
import org.slf4j.Logger;

import com.vlkan.rfos.RotatingFileOutputStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class TopicWriter implements Runnable {
	private static final Logger s_logger = Globals.LOGGER;

	private static final Duration POLL_TIMEOUT = Duration.ofSeconds(3);
	private static final byte[] NEWLINE = "\n".getBytes(StandardCharsets.UTF_8);
	
	private final RotationFileKey m_key;
	private final KafkaConsumer<Bytes,Bytes> m_consumer;
	private final RotatingFileOutputStream m_rfos;
	
	TopicWriter(RotationFileKey key, KafkaConsumer<Bytes,Bytes> consumer, RotatingFileOutputStream rfos) {
		m_key = key;
		m_consumer = consumer;
		m_rfos = rfos;
	}
	
	public void run() {
		boolean dirty = false;
		while ( true ) {
			ConsumerRecords<Bytes,Bytes> records = m_consumer.poll(POLL_TIMEOUT);
			if ( records.count() == 0 ) {
				if ( dirty ) {
					try {
						s_logger.debug("flushing tail file: {}", m_rfos.getFile().getAbsolutePath());
						m_rfos.flush();
						dirty = false;
					}
					catch ( IOException e ) {
						String details = "fails to flush tail files";
						s_logger.error(details, e);
					}
				}
			}
			else {
				if ( s_logger.isDebugEnabled() ) {
					s_logger.debug("fetch {} records", records.count());
				}
				for ( ConsumerRecord<Bytes,Bytes> record: records ) {
					try {
						byte[] vbytes = record.value().get();
						byte[] lineBytes = com.google.common.primitives.Bytes.concat(vbytes, NEWLINE);
						m_rfos.write(lineBytes);
					}
					catch ( IOException e ) {
						String details = String.format("fails to export record: topic=%s, cause=%s",
														m_key, e.getMessage());
						s_logger.error(details, e);
					}
				}
				dirty = true;
				
				try {
					m_consumer.commitSync();
				}
				catch ( CommitFailedException e ) {
					String details = String.format("fails to commit offset, cause=%s", e.getMessage());
					s_logger.error(details, e);
				}
			}
		}
	}
}
