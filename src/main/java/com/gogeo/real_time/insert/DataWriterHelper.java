package com.gogeo.real_time.insert;

import java.io.IOException;

import org.elasticsearch.common.xcontent.XContentBuilder;

import com.gogeo.utils.GeometryUtils;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;

public class DataWriterHelper {
	
	public static void buildGeoPointField(XContentBuilder builder,
            String fieldName, Point point) throws IOException {
        builder.field(fieldName,
                new double[] { point.getX(), point.getY() });
    }

    public static void buildGeoShapeField(XContentBuilder builder,
            String fieldName,
            Geometry geom)
            throws IOException {
        String geojson = GeometryUtils.toJson(geom);
        builder.field(fieldName, geojson.toLowerCase().getBytes());
    }
}
