/*
 * Copyright 2018-2022 Volkan Yazıcı
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permits and
 * limitations under the License.
 */

package rhdfos;

import java.io.IOException;
import java.time.Instant;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vlkan.rfos.Rotatable;
import com.vlkan.rfos.RotatingFileOutputStream;
import com.vlkan.rfos.RotationConfig;
import com.vlkan.rfos.policy.RotationPolicy;

import jarvey.HdfsPath;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RotatingFSDataOutputStream extends RotatingFileOutputStream implements Rotatable {
    private static final Logger LOGGER = LoggerFactory.getLogger(RotatingFSDataOutputStream.class);

    /**
     * Constructs an instance using the given configuration
     *
     * @param config a configuration instance
     */
    public RotatingFSDataOutputStream(HdfsPath file, RotationConfig config) {
    	super(file, config);
    }

    @Override
    protected ByteCountingHdfsOutputStream open(RotationPolicy policy, Instant instant) {
		HdfsPath hdfsFile = (HdfsPath)m_file;
		
		try {
			FSDataOutputStream fsdos = m_config.isAppend() ? hdfsFile.append() : hdfsFile.create(true);
			invokeCallbacks(callback -> callback.onOpen(policy, instant, fsdos));
			long size = m_config.isAppend() ? m_file.getLength() : 0;
			return new ByteCountingHdfsOutputStream(fsdos, size);
		}
		catch ( IOException error ) {
			String message = String.format("file open failure {file=%s}", m_file);
			throw new RuntimeException(message, error);
		}
    }
}
