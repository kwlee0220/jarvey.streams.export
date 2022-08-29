package jarvey.streams.export;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.utils.Bytes;
import org.slf4j.Logger;

import com.google.common.collect.Maps;
import com.vlkan.rfos.RotatingFileOutputStream;

import utils.func.Unchecked;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TopicExporterNew implements Runnable {
	private static final Logger s_logger = Globals.LOGGER;

	private static final Duration POLL_TIMEOUT = Duration.ofSeconds(3);
	
	private final Collection<String> m_topics;
	private final ExporterConfig m_exporterConfig;
	private final KafkaConsumer<Bytes,Bytes> m_consumer;
	private final Map<TopicPartition,BufferedWriter> m_writers = Maps.newHashMap();
	
	public TopicExporterNew(Collection<String> topics, ExporterConfig exporterConfig) {
		m_topics = topics;
		m_exporterConfig = exporterConfig;
		m_consumer = m_exporterConfig.newKafkaConsumer();
	}
	
	private final ConsumerRebalanceListener m_rebalanceListener = new ConsumerRebalanceListener() {
		@Override
		public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
			m_consumer.commitSync(getOffsets());
			FStream.from(partitions).forEach(m_writers::remove);
		}

		@Override
		public void onPartitionsAssigned(Collection<TopicPartition> partitions) { }
	};
	
	public void run() {
		try {
			m_consumer.subscribe(m_topics, m_rebalanceListener);
			
			while ( true ) {
				ConsumerRecords<Bytes,Bytes> records = m_consumer.poll(POLL_TIMEOUT);
				if ( records.count() > 0 ) {
					if ( s_logger.isDebugEnabled() ) {
						s_logger.debug("fetch {} records", records.count());
					}
					for ( ConsumerRecord<Bytes,Bytes> record: records ) {
						TopicPartition tpart = new TopicPartition(record.topic(), record.partition());
						BufferedWriter writer = m_writers.get(tpart);
						if ( writer == null ) {
							RotatingFileOutputStream rfos = m_exporterConfig.getOutputStream(tpart);
							writer = new BufferedWriter(rfos, 4 * 1024);
							m_writers.put(tpart, writer);
						}
						writer.write(record);
					}
					
					FStream.from(m_writers.values()).forEachOrIgnore(BufferedWriter::flush);
					m_consumer.commitAsync(getOffsets(), null);
				}
			}
		}
		catch ( WakeupException ignored ) { }
		catch ( Exception e ) {
			s_logger.error("Unexpected error", e);
		}
		finally {
			Unchecked.runOrIgnore(() -> m_consumer.commitSync(getOffsets()));
		}
	}
	
	private Map<TopicPartition,OffsetAndMetadata> getOffsets() {
		Map<TopicPartition,OffsetAndMetadata> offsets = Maps.newHashMap();
		for ( Map.Entry<TopicPartition, BufferedWriter> ent: m_writers.entrySet() ) {
			offsets.put(ent.getKey(), ent.getValue().getOffset());
		}
		return offsets;
	}
}
