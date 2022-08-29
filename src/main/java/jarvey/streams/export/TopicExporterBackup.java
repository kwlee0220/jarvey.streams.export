package jarvey.streams.export;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.utils.Bytes;
import org.slf4j.Logger;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.vlkan.rfos.RotatingFileOutputStream;
import com.vlkan.rfos.RotatingFilePattern;
import com.vlkan.rfos.RotationCallback;
import com.vlkan.rfos.RotationConfig;
import com.vlkan.rfos.policy.RotationPolicy;
import com.vlkan.rfos.policy.TimeBasedRotationPolicy;

import utils.UnitUtils;
import utils.io.FileProxy;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TopicExporterBackup implements Runnable {
	private static final Logger s_logger = Globals.LOGGER;
	
	private static final Duration POLL_TIMEOUT = Duration.ofSeconds(5);
	private static final long FETCH_MIN_BYTE = UnitUtils.parseByteSize("16kb");
	private static final long FETCH_MAX_WAIT_MS = POLL_TIMEOUT.toMillis();
	private static final long MAX_FILE_BUFFER_BYTE = Math.round(FETCH_MIN_BYTE * 1.1);
	private static final long POLL_INTERVAL_MILLIS
									= Math.round((POLL_TIMEOUT.toMillis() + FETCH_MAX_WAIT_MS) * 1.1);
	
	private final String m_kafkaServers;
	private final String m_groupId;
	private final Set<String> m_topics;

	private final FileProxy m_exportTailDir;
	private final FileProxy m_exportArchiveDir;
	private final String m_suffix;
	private final RotationPolicy m_policy;
	private final Map<TopicPartition,RotatingFileOutputStream> m_rfosMap = Maps.newHashMap();
	
	public TopicExporterBackup(String kafkaServers, String groupId, Collection<String> topics,
						FileProxy tailDir, FileProxy archiveDir, String suffix, RotationPolicy policy) {
		m_kafkaServers = kafkaServers;
		m_groupId = groupId;
		m_topics = Sets.newHashSet(topics);
		
		m_exportTailDir = tailDir;
		if ( !tailDir.exists() ) {
			s_logger.info("creating an export tail directory: {}", tailDir.getAbsolutePath());
			if ( !tailDir.mkdirs() ) {
				throw new UncheckedIOException(new IOException("fails to create a directory: " + tailDir));
			}
		}
		
		m_exportArchiveDir = archiveDir;
		if ( !archiveDir.exists() ) {
			s_logger.info("creating an export archive directory: {}", archiveDir);
			if ( !archiveDir.mkdirs() ) {
				throw new UncheckedIOException(new IOException("fails to create a directory: " + archiveDir));
			}
		}
		
		m_suffix = suffix;
		m_policy = policy;
	}

	@Override
	public void run() {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, m_kafkaServers);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, m_groupId);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, FETCH_MIN_BYTE);
		props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, FETCH_MAX_WAIT_MS);
		props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, POLL_INTERVAL_MILLIS);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());
		
		try ( KafkaConsumer<Bytes,Bytes> consumer = new KafkaConsumer<>(props) ) {
			consumer.subscribe(m_topics);
			
			boolean dirty = false;
			while ( true ) {
				ConsumerRecords<Bytes,Bytes> records = consumer.poll(POLL_TIMEOUT);
				if ( records.count() == 0 ) {
					if ( dirty ) {
						try { 
							FStream.from(m_rfosMap.values())
									.forEachOrThrow(rfos -> {
										s_logger.debug("flushing tail file: {}", rfos.getFile().getAbsolutePath());
										rfos.flush();
									});
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
							export(record);
						}
						catch ( IOException e ) {
							String details = String.format("fails to export record: topic=%s, cause=%s",
															record.topic(), e.getMessage());
							s_logger.error(details, e);
						}
					}
					dirty = true;
					
					try {
						consumer.commitSync();
					}
					catch ( CommitFailedException e ) {
						String details = String.format("fails to commit offset, cause=%s", e.getMessage());
						s_logger.error(details, e);
					}
				}
			}
		}
	}
	
	private static final byte[] NEWLINE = "\n".getBytes(StandardCharsets.UTF_8);
	private void export(ConsumerRecord<Bytes,Bytes> record) throws IOException {
		TopicPartition key = new TopicPartition(record.topic(), record.partition());
		RotatingFileOutputStream rfos = getOutputStream(key);
		if ( rfos != null ) {
			byte[] vbytes = record.value().get();
			byte[] lineBytes = com.google.common.primitives.Bytes.concat(vbytes, NEWLINE);
			rfos.write(lineBytes);
		}
	}
	
	private RotatingFileOutputStream getOutputStream(TopicPartition key) {
		RotatingFileOutputStream rfos = m_rfosMap.get(key);
		if ( rfos != null ) {
			return rfos;
		}
		
		if ( !m_topics.contains(key.topic()) ) {
			return null;
		}

		String topicTailFileName = String.format("%s_%d%s", key.topic(), key.partition(), m_suffix);
		FileProxy topicTailFile = m_exportTailDir.getChild(topicTailFileName);

		FileProxy topicArchiveDir = m_exportArchiveDir.getChild(key.topic());
		String patFileName = String.format("%s_%d-%s%s", key.topic(), key.partition(),
											"%d{yyyyMMdd-HH}", m_suffix);
		String filePattern = topicArchiveDir.getAbsolutePath()
												+ File.separator + "%d{yyyy}"
												+ File.separator + "%d{MM}"
												+ File.separator + "%d{dd}"
												+ File.separator + patFileName;
		RotatingFilePattern pat = RotatingFilePattern.builder().pattern(filePattern).build();
		
		ExportFileMerger merger = new ExportFileMerger();
		RotationConfig rconfig = RotationConfig.builder()
												.append(true)
												.filePattern(pat)
												.compress(true)
												.policy(m_policy)
												.callback(merger)
												.build();
		rfos = new RotatingFileOutputStream(topicTailFile, rconfig);
		rfos.setLogger(Globals.LOGGER_ROTATION);
		m_rfosMap.put(key, rfos);
		
		return rfos;
	}
	
	static class ExportFileMerger implements RotationCallback {
		@Override
		public void onTrigger(RotationPolicy policy, Instant instant) { }

		@Override
		public void onOpen(RotationPolicy policy, Instant instant, OutputStream stream) { }

		@Override
		public void onClose(RotationPolicy policy, Instant instant, OutputStream stream) { }

		@Override
		public void onSuccess(RotationPolicy policy, Instant instant, FileProxy file) {
			TimeBasedRotationPolicy tbPolicy = (TimeBasedRotationPolicy)policy;
			tbPolicy.getLogger().info("created a rolling file: {}", file);
		}

		@Override
		public void onFailure(RotationPolicy policy, Instant instant, FileProxy file, Exception error) {
			TimeBasedRotationPolicy tbPolicy = (TimeBasedRotationPolicy)policy;
			
			String msg = String.format("faild to create a rolling file: file=%s, cause=%s", file, error);
			tbPolicy.getLogger().error(msg, error);
		}
	}
}
