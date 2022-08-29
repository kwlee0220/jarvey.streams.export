package jarvey.streams.export;

import java.io.File;
import java.io.OutputStream;
import java.time.Instant;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;

import com.vlkan.rfos.RotatingFileOutputStream;
import com.vlkan.rfos.RotatingFilePattern;
import com.vlkan.rfos.RotationCallback;
import com.vlkan.rfos.RotationConfig;
import com.vlkan.rfos.policy.RotationPolicy;

import utils.io.FileProxy;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class ExporterConfig {
	private final Properties m_props;
	private final FileProxy m_tailDir;
	private final FileProxy m_archiveDir;
	private final String m_suffix;
	private final RotationPolicy m_policy;
	
	ExporterConfig(Properties props, FileProxy tailDir, FileProxy archiveDir, String suffix,
					RotationPolicy policy) {
		m_props = props;
		m_tailDir = tailDir;
		m_archiveDir = archiveDir;
		m_suffix = suffix;
		m_policy = policy;
	}
	
	public KafkaConsumer<Bytes,Bytes> newKafkaConsumer() {
		return new KafkaConsumer<>(m_props);
	}
	
	RotatingFileOutputStream getOutputStream(TopicPartition key) {
		String topicTailFileName = String.format("%s_%d%s", key.topic(), key.partition(), m_suffix);
		FileProxy topicTailFile = m_tailDir.getChild(topicTailFileName);

		FileProxy topicArchiveDir = m_archiveDir.getChild(key.topic());
		String patFileName = String.format("%s_%d-%s%s", key.topic(), key.partition(),
											"%d{yyyyMMdd-HH}", m_suffix);
		String filePattern = topicArchiveDir.getAbsolutePath()
												+ File.separator + "%d{yyyy}"
												+ File.separator + "%d{MM}"
												+ File.separator + "%d{dd}"
												+ File.separator + patFileName;
		RotatingFilePattern pat = RotatingFilePattern.builder().pattern(filePattern).build();
		
		RotationConfig rconfig = RotationConfig.builder()
												.append(true)
												.filePattern(pat)
												.compress(true)
												.policy(m_policy)
												.callback(s_callback)
												.build();
		RotatingFileOutputStream rfos = new RotatingFileOutputStream(topicTailFile, rconfig);
		rfos.setLogger(Globals.LOGGER_ROTATION);
		
		return rfos;
	}
	
	private static final LoggingCallback s_callback = new LoggingCallback();
	static class LoggingCallback implements RotationCallback {
		@Override
		public void onTrigger(RotationPolicy policy, Instant instant) { }

		@Override
		public void onOpen(RotationPolicy policy, Instant instant, OutputStream stream) { }

		@Override
		public void onClose(RotationPolicy policy, Instant instant, OutputStream stream) { }

		@Override
		public void onSuccess(RotationPolicy policy, Instant instant, FileProxy file) {
			Globals.LOGGER_ROTATION.info("created a rolling file: {}", file);
		}

		@Override
		public void onFailure(RotationPolicy policy, Instant instant, FileProxy file, Exception error) {
			String msg = String.format("faild to create a rolling file: file=%s, cause=%s", file, error);
			Globals.LOGGER_ROTATION.error(msg, error);
		}
	}
}