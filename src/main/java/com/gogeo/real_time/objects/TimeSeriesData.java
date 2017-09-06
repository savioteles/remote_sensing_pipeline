package com.gogeo.real_time.objects;

import java.io.Serializable;
import java.util.Date;

import com.vividsolutions.jts.geom.Geometry;

public class TimeSeriesData implements Serializable{

	private static final long serialVersionUID = 1L;
	private int xPixel;
	private int yPixel;
	private Geometry geom;
	private double pixelValue;
	private Date date;

	public TimeSeriesData(int xPixel, int yPixel, Geometry geom,
			double pixelValue, Date date) {
		this.xPixel = xPixel;
		this.yPixel = yPixel;
		this.geom = geom;
		this.pixelValue = pixelValue;
		this.date = date;
	}

	public int getxPixel() {
		return xPixel;
	}

	public int getyPixel() {
		return yPixel;
	}

	public Geometry getGeom() {
		return geom;
	}

	public double getPixelValue() {
		return pixelValue;
	}

	public Date getDate() {
		return date;
	}
}
