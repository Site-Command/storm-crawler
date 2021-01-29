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

package com.digitalpebble.stormcrawler.vespa.persistence;

import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.storm.metric.api.MultiReducedMetric;
import org.apache.storm.metric.api.ReducedMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.persistence.AbstractStatusUpdaterBolt;
import com.digitalpebble.stormcrawler.persistence.Status;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import com.digitalpebble.stormcrawler.util.PerSecondReducer;
import com.digitalpebble.stormcrawler.util.URLPartitioner;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.yahoo.vespa.http.client.FeedClient;
import com.yahoo.vespa.http.client.FeedClientFactory;
import com.yahoo.vespa.http.client.Result;
import com.yahoo.vespa.http.client.Result.Detail;
import com.yahoo.vespa.http.client.config.Cluster;
import com.yahoo.vespa.http.client.config.ConnectionParams;
import com.yahoo.vespa.http.client.config.Endpoint;
import com.yahoo.vespa.http.client.config.FeedParams;
import com.yahoo.vespa.http.client.config.SessionParams;

@SuppressWarnings("serial")
public class StatusUpdaterBolt extends AbstractStatusUpdaterBolt
		implements FeedClient.ResultCallback, RemovalListener<String, List<Tuple>> {

	private static final Logger LOG = LoggerFactory.getLogger(StatusUpdaterBolt.class);

	private FeedClient feedClient;

	private static ObjectMapper mapper = new ObjectMapper(); // create once, reuse

	private URLPartitioner partitioner;

	private Cache<String, List<Tuple>> waitAck;

	private ReducedMetric perSecMetrics;

	public StatusUpdaterBolt() {
		super();
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);

		boolean useCompression = ConfUtils.getBoolean(stormConf, "vespa.compression", false);
		String hostname = ConfUtils.getString(stormConf, "vespa.host", "localhost");
		int port = ConfUtils.getInt(stormConf, "vespa.port", 4080);

		Endpoint endpoint = Endpoint.create(hostname, port, false);
		SessionParams sessionParams = new SessionParams.Builder()
				.addCluster(new Cluster.Builder().addEndpoint(endpoint).build())
				.setConnectionParams(new ConnectionParams.Builder().setUseCompression(useCompression).build())
				.setFeedParams(new FeedParams.Builder().setDataFormat(FeedParams.DataFormat.JSON_UTF8).build()).build();
		this.feedClient = FeedClientFactory.create(sessionParams, this);

		partitioner = new URLPartitioner();
		partitioner.configure(stormConf);

		waitAck = CacheBuilder.newBuilder().expireAfterWrite(60, TimeUnit.SECONDS).removalListener(this).build();

		this.perSecMetrics = context.registerMetric("sent_average_persec", new ReducedMetric(new PerSecondReducer()),
				60);
	}

	@Override
	public void cleanup() {
		feedClient.close();
	}

	@Override
	protected void store(String url, Status status, Metadata metadata, Date nextFetch, Tuple t) throws Exception {

		String docId = org.apache.commons.codec.digest.DigestUtils.sha256Hex(url);

		// need to synchronize: otherwise it might get added to the cache
		// without having been sent to the backend
		synchronized (waitAck) {
			// check that the same URL is not being sent to ES
			List<Tuple> alreadySent = waitAck.getIfPresent(docId);
			if (alreadySent != null && status.equals(Status.DISCOVERED)) {
				// if this object is discovered - adding another version of it
				// won't make any difference
				LOG.debug("Already being sent to ES {} with status {} and ID {}", url, status, docId);
				// ack straight away!
				super.ack(t, url);
				return;
			}
		}

		String partitionKey = partitioner.getPartition(url, metadata);
		if (partitionKey == null) {
			partitionKey = "_DEFAULT_";
		}

		String ts = Long.toString(Instant.now().getEpochSecond());

		LinkedHashMap<String, Object> map = new LinkedHashMap<>();

		Map<String, Object> fields = new HashMap<>();

		// need to use an update for discovered
		// at least until https://github.com/vespa-engine/vespa/issues/16209
		// is resolved
		if (status.equals(Status.DISCOVERED)) {
			// It should be possible to do what you're looking for by using updates with
			// create: true set in conjunction with a condition that always fails if the
			// document does exist (e.g. just "false"). The condition is explicitly ignored
			// iff the document does not exist and the create-flag is set.
			map.put("update", "id:url:url::" + docId);
			map.put("condition", "url.url=='IMPOSSIBLEVALUE'");
			map.put("create", true);
			// need assigns
			HashMap a1 = new HashMap(1);
			a1.put("assign", url);
			fields.put("url", a1);
			HashMap a2 = new HashMap(1);
			a2.put("assign", status.toString());
			fields.put("status", a2);
			HashMap a3 = new HashMap(1);
			a3.put("assign", partitionKey);
			fields.put("key", a3);
			HashMap a4 = new HashMap(1);
			a4.put("assign", ts);
			fields.put("next_fetch_date", a4);
			HashMap a5 = new HashMap(1);
			a5.put("assign", metadata.asMap());
			fields.put("metadata", a5);
		}
		// just override any existing value
		else {
			map.put("put", "id:url:url::" + docId);
			fields.put("url", url);
			fields.put("status", status.toString());
			fields.put("key", partitionKey);
			fields.put("next_fetch_date", ts);
			fields.put("metadata", metadata.asMap());
		}

		map.put("fields", fields);

		String jsonResult = mapper.writeValueAsString(map);

		synchronized (waitAck) {
			List<Tuple> tt = waitAck.getIfPresent(docId);
			if (tt == null) {
				tt = new LinkedList<>();
				waitAck.put(docId, tt);
			}
			tt.add(t);
			LOG.debug("Added to waitAck {} with ID {} total {}", url, docId, tt.size());
		}

		feedClient.stream(docId, jsonResult);
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
	public void onCompletion(String id, Result documentResult) {
		this.perSecMetrics.update(1);
		boolean success = documentResult.isSuccess();
		if (!success) {
			for (Detail d : documentResult.getDetails()) {
				if (d.getResultType().name().equals("CONDITION_NOT_MET")) {
					success = true;
				}
				break;
			}
		}
		synchronized (waitAck) {
			List<Tuple> xx = waitAck.getIfPresent(id);
			if (xx != null) {
				LOG.debug("Acked {} tuple(s) for ID {}", xx.size(), id);
				for (Tuple x : xx) {
					if (success) {
						String url = x.getStringByField("url");
						// ack and put in cache
						LOG.debug("Acked {} with ID {}", url, id);
						super.ack(x, url);
					} else {
						LOG.error("Failure reported by Vespa: {}", documentResult);
						_collector.fail(x);
					}
				}
				waitAck.invalidate(id);
			} else {
				LOG.warn("Could not find unacked tuple for {}", id);
			}
		}
	}
}
