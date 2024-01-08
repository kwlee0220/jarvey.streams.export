package jarvey.streams.export;

import java.util.Collection;
import java.util.List;
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

import utils.UnitUtils;
import utils.func.Try;
import utils.func.Unchecked;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TopicExporter {
	private static final Logger s_logger = Globals.LOGGER;
	
	private final Collection<String> m_topics;
	private final KafkaConsumer<Bytes,Bytes> m_consumer;
	private final ExportConfig m_exporterConfig;
	private final Map<TopicPartition,TopicPartitionWriter> m_writers = Maps.newHashMap();
	
	public TopicExporter(KafkaConsumer<Bytes,Bytes> consumer, Collection<String> topics,
							ExportConfig exporterConfig) {
		m_consumer = consumer;
		m_topics = topics;
		m_exporterConfig = exporterConfig;
	}
	
	private final ConsumerRebalanceListener m_rebalanceListener = new ConsumerRebalanceListener() {
		@Override
		public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
			s_logger.info("provoking topic-partitions: {}", partitions);
			
			m_consumer.commitSync(getOffsets());
			for ( TopicPartition tpart: partitions ) {
				TopicPartitionWriter writer = m_writers.remove(tpart);
				if ( writer != null ) {
					writer.close();
				}
			}
		}

		@Override
		public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
			s_logger.info("assigning new topic-partitions: {}", partitions);
		}
	};
	
	public void run() {
		try {
			m_consumer.subscribe(m_topics, m_rebalanceListener);
			
			while ( true ) {
				ConsumerRecords<Bytes,Bytes> records = m_consumer.poll(m_exporterConfig.getPollTimeout());
				if ( records.count() > 0 ) {
					if ( s_logger.isDebugEnabled() ) {
						s_logger.debug("fetch {} records", records.count());
					}
					
					for ( TopicPartition tpart: records.partitions() ) {
						List<ConsumerRecord<Bytes,Bytes>> work = records.records(tpart);
						TopicPartitionWriter writer = getWriter(tpart);
						Try<Integer> nbytes = writer.write(work);
						nbytes.ifSuccessful(n -> {
							if ( s_logger.isInfoEnabled() ) {
								s_logger.info("topic={}({}), nrecords={}, size={}, offset={}",
												tpart.topic(), tpart.partition(),
												work.size(), UnitUtils.toByteSizeString(n),
												writer.getOffsetAndMeta().offset());
							}
						});
					}
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
			Unchecked.runOrIgnore(m_consumer::close);
		}
	}
	
	private TopicPartitionWriter getWriter(TopicPartition tpart) {
		return m_writers.computeIfAbsent(tpart, k -> {
			RotatingFileOutputStream rfos = m_exporterConfig.getOutputStream(k);
			return new TopicPartitionWriter(rfos, m_exporterConfig.getBufferSize());
		});
	}
	
	private Map<TopicPartition,OffsetAndMetadata> getOffsets() {
		return FStream.from(m_writers)
						.mapValue((k,v) -> v.getOffsetAndMeta())
						.toMap();
	}
}
