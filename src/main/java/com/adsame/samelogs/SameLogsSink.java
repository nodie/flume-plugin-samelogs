/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.adsame.samelogs;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.handlers.hdfs.DFSEventSink;
import com.cloudera.flume.handlers.text.TailSource;
import com.cloudera.util.Pair;
import com.google.common.base.Preconditions;

/**
 * Simple Sink that writes to a "helloworld.txt" file.
 */
public class SameLogsSink extends EventSink.Base {
	static final Logger LOG = LoggerFactory.getLogger(SameLogsSink.class);
	private PrintWriter pw;
	DFSEventSink dfsEventSink;
//	EscapedCustomDfsSink escapedCustomDfsSink; 

	@Override
	public void open() throws IOException {
		// Initialized the sink
		pw = new PrintWriter(new FileWriter("SameLogs.txt"));
		
		dfsEventSink = new DFSEventSink("hdfs://nodie-Ubuntu4:9000/user/nodie/input/dfs");
		dfsEventSink.open();

//		escapedCustomDfsSink = new EscapedCustomDfsSink("hdfs://nodie-Ubuntu4:9000/user/nodie/input/dfs"
//				, "hello");
//		escapedCustomDfsSink.open();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void append(Event e) throws IOException {
		// append the event to the output
		byte[] fn = e.get(TailSource.A_TAILSRCFILE);
		byte[] bd = e.getBody();
		
		Map<String, byte[]> maps = e.getAttrs();
		
        Iterator iter = maps.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry) iter.next();
            String key = (String) entry.getKey();
            System.out.println("key: " + key);
        }
        
		System.out.println("##" + new String(fn) + "##" + new String(bd));

		// here we are assuming the body is a string
		pw.println(new String(e.getBody()));
		pw.flush(); // so we can see it in the file right away
		
		
		try {
			dfsEventSink.append(e);
//			escapedCustomDfsSink.append(e);
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	@Override
	public void close() throws IOException {
		// Cleanup
		pw.flush();
		pw.close();
		
		dfsEventSink.close();
//		escapedCustomDfsSink.close();
	}

	public static SinkBuilder builder() {
		return new SinkBuilder() {
			// construct a new parameterized sink
			@Override
			public EventSink build(Context context, String... argv) {
				Preconditions.checkArgument(argv.length == 0,
						"usage: SameLogsSink");

				return new SameLogsSink();
			}
		};
	}

	/**
	 * This is a special function used by the SourceFactory to pull in this
	 * class as a plugin sink.
	 */
	public static List<Pair<String, SinkBuilder>> getSinkBuilders() {
		List<Pair<String, SinkBuilder>> builders = new ArrayList<Pair<String, SinkBuilder>>();
		builders.add(new Pair<String, SinkBuilder>("SameLogsSink", builder()));
		return builders;
	}
}
