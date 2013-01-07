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

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SourceFactory.SourceBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.handlers.text.TailSource;
import com.cloudera.util.Pair;
import com.google.common.base.Preconditions;

/**
 * Simple Source that generates a "hello world!" event every 3 seconds.
 */

public class SameLogsSource extends EventSource.Base {
	static final Logger LOG = LoggerFactory.getLogger(SameLogsSource.class);
	
	private String helloWorld;
	TailSource tailSource;
	
	public SameLogsSource() {
	    File f = new File("/home/samelog/logs/rtb_test.2011-11-15.18.Standard.192.168.32.134");
		tailSource = new TailSource(f, 1024, 100, true);
		try {
			tailSource.open();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void open() throws IOException {
		// Initialized the source
		helloWorld = "Hello World!!";
	}
	
	
	@Override
	public Event next() throws IOException {
		// Next returns the next event, blocking if none available.
		Event eventImpl = null;
		try {
			Date currentTime = new Date();
			SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String dateString = formatter.format(currentTime);
			helloWorld = dateString;

			eventImpl = new EventImpl();
			eventImpl = tailSource.next();
			System.out.println("#####" + eventImpl);
			
			//helloWorld = tailSource.
			
			System.out.println(helloWorld);
			//eventImpl = new EventImpl(helloWorld.getBytes());
			
			Thread.sleep(300);
		} catch (Exception e) {
			e.printStackTrace();
		}
		//return new EventImpl(helloWorld.getBytes());
		return eventImpl;
	}

	@Override
	public void close() throws IOException {
		// Cleanup
		helloWorld = null;
		tailSource.close();
	}

	public static SourceBuilder builder() {
		// construct a new parameterized source
		return new SourceBuilder() {
			@Override
			public EventSource build(Context ctx, String... argv) {
				Preconditions.checkArgument(argv.length == 0,
						"usage: SameLogsSource");

				return new SameLogsSource();
			}
		};
	}

	/**
	 * This is a special function used by the SourceFactory to pull in this
	 * class as a plugin source.
	 */
	public static List<Pair<String, SourceBuilder>> getSourceBuilders() {
		List<Pair<String, SourceBuilder>> builders = new ArrayList<Pair<String, SourceBuilder>>();
		builders.add(new Pair<String, SourceBuilder>("SameLogsSource",
				builder()));
		return builders;
	}
}
