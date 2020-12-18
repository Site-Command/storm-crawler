/**
 * Licensed to DigitalPebble Ltd under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * DigitalPebble licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.digitalpebble.stormcrawler.urlfrontier;

import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.persistence.AbstractStatusUpdaterBolt;
import com.digitalpebble.stormcrawler.persistence.Status;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import com.digitalpebble.stormcrawler.util.URLPartitioner;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import crawlercommons.urlfrontier.URLFrontierGrpc;
import crawlercommons.urlfrontier.URLFrontierGrpc.URLFrontierStub;
import crawlercommons.urlfrontier.Urlfrontier.StringList;
import crawlercommons.urlfrontier.Urlfrontier.StringList.Builder;
import crawlercommons.urlfrontier.Urlfrontier.Timestamp;
import crawlercommons.urlfrontier.Urlfrontier.URLItem;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

@SuppressWarnings("serial")
public class StatusUpdaterBolt extends AbstractStatusUpdaterBolt
		implements RemovalListener<String, List<Tuple>>, StreamObserver<crawlercommons.urlfrontier.Urlfrontier.String> {

	public static final Logger LOG = LoggerFactory.getLogger(StatusUpdaterBolt.class);
	private URLFrontierStub frontier;
	private ManagedChannel channel;
	private URLPartitioner partitioner;
	private StreamObserver<URLItem> requestObserver;
	private Cache<String, List<Tuple>> waitAck;

	public StatusUpdaterBolt() {
		waitAck = CacheBuilder.newBuilder().expireAfterWrite(60, TimeUnit.SECONDS).removalListener(this).build();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		String host = ConfUtils.getString(stormConf, "urlfrontier.host", "localhost");
		int port = ConfUtils.getInt(stormConf, "urlfrontier.port", 7071);

		LOG.info("Initialisation of connection to URLFrontier service on {}:{}", host, port);

		channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
		frontier = URLFrontierGrpc.newStub(channel);

		partitioner = new URLPartitioner();
		partitioner.configure(stormConf);

		requestObserver = frontier.putURLs(this);
	}

	@Override
	public void onNext(final crawlercommons.urlfrontier.Urlfrontier.String value) {
		String url = value.getValue();
		List<Tuple> xx = waitAck.getIfPresent(url);
		if (xx != null) {
			LOG.debug("Acked {} tuple(s) for ID {}", xx.size(), url);
			for (Tuple x : xx) {
				// String url = x.getStringByField("url");
				// ack and put in cache
				LOG.debug("Acked {} ", url);
				super.ack(x, url);
			}
			waitAck.invalidate(url);
		} else {
			LOG.warn("Could not find unacked tuple for {}", url);
		}
	}

	@Override
	public void onError(Throwable t) {
		LOG.info("Error received", t);
	}

	@Override
	public void onCompleted() {
		// end of stream - nothing special to do?
	}

	@Override
	public synchronized void store(String url, Status status, Metadata metadata, Date nextFetch, Tuple t)
			throws Exception {

		// need to synchronize: otherwise it might get added to the cache
		// without having been sent
		synchronized (waitAck) {
			// check that the same URL is not being sent to ES
			List<Tuple> alreadySent = waitAck.getIfPresent(url);
			if (alreadySent != null && status.equals(Status.DISCOVERED)) {
				// if this object is discovered - adding another version of it
				// won't make any difference
				LOG.debug("Already being sent to urlfrontier {} with status {} and ID {}", url, status, url);
				// ack straight away!
				super.ack(t, url);
				return;
			}
		}

		Timestamp ts = Timestamp.newBuilder().setSeconds(nextFetch.toInstant().getEpochSecond()).build();

		String partitionKey = partitioner.getPartition(url, metadata);
		if (partitionKey == null) {
			partitionKey = "_DEFAULT_";
		}

		crawlercommons.urlfrontier.Urlfrontier.URLItem.Status stat = crawlercommons.urlfrontier.Urlfrontier.URLItem.Status
				.valueOf(status.name());
		
		final Map<String, StringList> mdCopy = new HashMap<>(metadata.size());
		for (String k : metadata.keySet()) {
			String[] vals = metadata.getValues(k);
			Builder builder = StringList.newBuilder();
			for (String v : vals)
				builder.addString(v);
			mdCopy.put(k, builder.build());
		}

		URLItem item = URLItem.newBuilder().setKey(partitionKey).setUrl(url).setStatus(stat).setNextFetchDate(ts)
				.putAllMetadata(mdCopy).build();

		synchronized (waitAck) {
			List<Tuple> tt = waitAck.getIfPresent(url);
			if (tt == null) {
				tt = new LinkedList<>();
				waitAck.put(url, tt);
			}
			tt.add(t);
			LOG.debug("Added to waitAck {} with ID {} total {}", url, url, tt.size());
		}

		requestObserver.onNext(item);
	}

	public void onRemoval(RemovalNotification<String, List<Tuple>> removal) {
		if (!removal.wasEvicted())
			return;
		LOG.error("Purged from waitAck {} with {} values", removal.getKey(), removal.getValue().size());
		for (Tuple t : removal.getValue()) {
			_collector.fail(t);
		}
	}

	@Override
	public void cleanup() {
		requestObserver.onCompleted();
		channel.shutdownNow();
	}

}