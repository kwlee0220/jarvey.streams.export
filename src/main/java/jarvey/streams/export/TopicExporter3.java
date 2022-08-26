package jarvey.streams.export;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.utils.Bytes;
import org.slf4j.Logger;

import com.google.common.collect.Maps;
import com.vlkan.rfos.RotatingFileOutputStream;
import com.vlkan.rfos.RotatingFilePattern;
import com.vlkan.rfos.RotationCallback;
import com.vlkan.rfos.RotationConfig;
import com.vlkan.rfos.policy.RotationPolicy;

import jarvey.streams.export.TopicExporter.ExportFileMerger;
import utils.io.FileProxy;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TopicExporter3 implements Runnable {
	private static final Logger s_logger = Globals.LOGGER;

	private static final Duration POLL_TIMEOUT = Duration.ofSeconds(3);
	private static final byte[] NEWLINE = "\n".getBytes(StandardCharsets.UTF_8);
	
	private final KafkaConsumer<Bytes,Bytes> m_consumer;
	private final FileProxy m_exportTailDir;
	private final FileProxy m_exportArchiveDir;
	private final String m_suffix;
	private final RotationPolicy m_policy;
	private final Map<RotationFileKey,RotatingFileOutputStream> m_rfosMap = Maps.newHashMap();
	
	public TopicExporter3(KafkaConsumer<Bytes,Bytes> consumer, FileProxy tailDir, FileProxy archiveDir,
							String suffix, RotationPolicy policy) {
		m_consumer = consumer;
		
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
	
	public void run() {
		boolean dirty = false;
		while ( true ) {
			ConsumerRecords<Bytes,Bytes> records = m_consumer.poll(POLL_TIMEOUT);
			if ( records.count() == 0 ) {
				if ( dirty ) {
					for ( RotatingFileOutputStream rfos: m_rfosMap.values() ) {
						try {
							s_logger.debug("flushing tail file: {}", rfos.getFile().getAbsolutePath());
							rfos.flush();
							dirty = false;
						}
						catch ( IOException e ) {
							String details = "fails to flush tail files";
							s_logger.error(details, e);
						}
					}
				}
			}
			else {
				if ( s_logger.isDebugEnabled() ) {
					s_logger.debug("fetch {} records", records.count());
				}
				for ( ConsumerRecord<Bytes,Bytes> record: records ) {
					RotationFileKey key = new RotationFileKey(record.topic(), record.partition());
					try {
						RotatingFileOutputStream rfos = getOutputStream(key);
						
						byte[] vbytes = record.value().get();
						byte[] lineBytes = com.google.common.primitives.Bytes.concat(vbytes, NEWLINE);
						rfos.write(lineBytes);
					}
					catch ( IOException e ) {
						String details = String.format("fails to export record: topic=%s, cause=%s",
														key, e.getMessage());
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
	
	private RotatingFileOutputStream getOutputStream(RotationFileKey key) {
		RotatingFileOutputStream rfos = m_rfosMap.get(key);
		if ( rfos != null ) {
			return rfos;
		}

		String topicTailFileName = String.format("%s_%d%s", key.m_topic, key.m_partition, m_suffix);
		FileProxy topicTailFile = m_exportTailDir.getChild(topicTailFileName);

		FileProxy topicArchiveDir = m_exportArchiveDir.getChild(key.m_topic);
		String patFileName = String.format("%s_%d-%s%s", key.m_topic, key.m_partition,
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
	
	static class LoggingCallback implements RotationCallback {
		@Override
		public void onTrigger(RotationPolicy policy, Instant instant) { }

		@Override
		public void onOpen(RotationPolicy policy, Instant instant, OutputStream stream) { }

		@Override
		public void onClose(RotationPolicy policy, Instant instant, OutputStream stream) { }

		@Override
		public void onSuccess(RotationPolicy policy, Instant instant, FileProxy file) {
			s_logger.info("created a rolling file: {}", file);
		}

		@Override
		public void onFailure(RotationPolicy policy, Instant instant, FileProxy file, Exception error) {
			String msg = String.format("faild to create a rolling file: file=%s, cause=%s", file, error);
			s_logger.error(msg, error);
		}
	}
}
