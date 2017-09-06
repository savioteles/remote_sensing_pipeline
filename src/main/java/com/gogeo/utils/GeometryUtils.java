package com.gogeo.utils;

import java.io.IOException;
import java.io.StringWriter;

import org.geotools.geojson.geom.GeometryJSON;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;

public class GeometryUtils {
	private static PrecisionModel PRECISION_MODEL = new PrecisionModel(
            PrecisionModel.FLOATING);
	public static int WGS84_srid = 32722;
	private static GeometryFactory gf = new GeometryFactory(PRECISION_MODEL,
            WGS84_srid);
	
	public static String toJson(Geometry geom) throws IOException {
        GeometryJSON gjson = new GeometryJSON(
                PRECISION_MODEL.getMaximumSignificantDigits());
        StringWriter writer = new StringWriter();
        gjson.write(geom, writer);
        return writer.toString();
    }
	
	public static Geometry toGeometry(Envelope env) {
        return gf.toGeometry(env);
    }
	
	public static Envelope getPixelBound(double[] adfGeoTransform,
            double x, double y) {
		
		double xPixelSize = x + 1;
        double yPixelSize = y + 1;
        
        // upper left
        double x_ul = adfGeoTransform[0] + adfGeoTransform[1] * x
                + adfGeoTransform[2] * y;
        double y_ul = adfGeoTransform[3] + adfGeoTransform[4] * x
                + adfGeoTransform[5] * y;

        // lower left
        double x_ll = adfGeoTransform[0] + adfGeoTransform[1] * x
                + adfGeoTransform[2] * yPixelSize;
        double y_ll = adfGeoTransform[3] + adfGeoTransform[4] * x
                + adfGeoTransform[5] * yPixelSize;

        // upper right
        double x_ur = adfGeoTransform[0] + adfGeoTransform[1] * xPixelSize
                + adfGeoTransform[2] * y;
        double y_ur = adfGeoTransform[3] + adfGeoTransform[4] * xPixelSize
                + adfGeoTransform[5] * y;

        // lower right
        double x_lr = adfGeoTransform[0] + adfGeoTransform[1] * xPixelSize
                + adfGeoTransform[2] * yPixelSize;
        double y_lr = adfGeoTransform[3] + adfGeoTransform[4] * xPixelSize
                + adfGeoTransform[5] * yPixelSize;

        // calculate x and y bounds
        double xmin = x_ll < x_ul ? x_ll : x_ul;
        double xmax = x_ur > x_lr ? x_ur : x_lr;
        double ymin = y_ll < y_lr ? y_ll : y_lr;
        double ymax = y_ul > y_ur ? y_ul : y_ur;

        return new Envelope(xmin, xmax, ymin, ymax);
    }
}
