package com.gogeo.real_time.insert;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import com.gogeo.real_time.objects.TimeSeriesData;
import com.vividsolutions.jts.geom.Geometry;

public class TimeSeriesDataWriter {

	private static final int SHARDS = 5;
	private static final String ATTR = "attr";
	private static Client client;

	@SuppressWarnings("resource")
	public TimeSeriesDataWriter(String seed, String cluster) {
		Settings settings = Settings.builder().put("cluster.name", cluster)
				.build();
		client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new InetSocketTransportAddress(
						new InetSocketAddress(seed, 9300)));
	}

	public void createDataLayer(String layer, int shardsReplicasNum,
			int numOfShards) {
		ClusterHealthResponse actionGet = client.admin().cluster()
				.health(new ClusterHealthRequest()).actionGet();
		if (actionGet.getNumberOfNodes() < shardsReplicasNum)
			throw new RuntimeException("Number of replicas "
					+ shardsReplicasNum
					+ " is smaller than the number of nodes "
					+ actionGet.getNumberOfNodes());

		if (numOfShards == 0)
			numOfShards = SHARDS;

		CreateIndexRequestBuilder createIndexBuilder = client.admin().indices()
				.prepareCreate(layer);
		createIndexBuilder.execute().actionGet();

		Map<String, Object> mapping = new HashMap<String, Object>();
		mapping.put("xPixel", "string");
		mapping.put("yPixel", "string");
		mapping.put("pixel_value", "double");
		mapping.put("date", "date");

		client.admin().indices().preparePutMapping(layer).setType(ATTR)
				.setSource(mapping).execute().actionGet();
	}

	public void insertTimeSeriesData(String layer, List<TimeSeriesData> datasToInsert) throws IOException {
		BulkRequestBuilder bulkRequest = client.prepareBulk();
        
		for (TimeSeriesData data: datasToInsert) {
			XContentBuilder jsonToInsert = constructJsonToInsert(data);
			bulkRequest.add(client.prepareIndex(layer, ATTR).setSource(jsonToInsert));
		}
		bulkRequest.get();
	}

	private XContentBuilder constructJsonToInsert(TimeSeriesData data)
			throws IOException {

		XContentBuilder jsonToInsert = XContentFactory.jsonBuilder()
				.startObject();

		Geometry geom = data.getGeom();
		DataWriterHelper.buildGeoShapeField(jsonToInsert, "pixel_bound", geom);
		DataWriterHelper.buildGeoPointField(jsonToInsert, "pixel_centroid",
				geom.getCentroid());

		jsonToInsert.field("xPixel", data.getxPixel());
		jsonToInsert.field("yPixel", data.getyPixel());
		jsonToInsert.field("pixel_value", data.getPixelValue());
		jsonToInsert.field("date", data.getDate());

		jsonToInsert.endObject();

		return jsonToInsert;
	}

}
