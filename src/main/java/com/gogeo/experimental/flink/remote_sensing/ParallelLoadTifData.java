package com.gogeo.experimental.flink.remote_sensing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.script.ScriptException;

import org.gdal.gdal.Band;
import org.gdal.gdal.Dataset;
import org.gdal.gdal.gdal;
import org.gdal.gdalconst.gdalconstConstants;

public class ParallelLoadTifData {
	private static final int MAX_COLUMNS_TO_READ_INDIVIDUALLY = 2000;
	private static final int NUM_THREADS = 8;
	
	public static void main(String[] args) throws IOException,
			InterruptedException, ScriptException {
		gdal.AllRegister();
		Dataset hDataset = gdal.Open("/media/savio/67E3BA5B4FBF63B1/resampled_mask_past_v07_modis_5m.tif", gdalconstConstants.GA_ReadOnly);
		int x_size = hDataset.getRasterXSize();
		int y_size = hDataset.getRasterYSize();
		
		int[] band_list = {1};
		byte[] regularArrayOut = new byte[x_size * y_size];
		//read mask file
		hDataset.ReadRaster(0, 0, 21592, 18660, 21592, 18660, gdalconstConstants.GDT_Byte, regularArrayOut, band_list);

		Map<Integer, List<Integer>> imgIndices = new HashMap<Integer, List<Integer>>();
	    
		long time = System.currentTimeMillis();
		for (int i = 0; i < regularArrayOut.length; i++) {
			if (regularArrayOut[i] != 0) {
				int x = i % 21592;
				int y = i / 21592;
				
				List<Integer> list = imgIndices.get(y);
				if(list == null) {
					list = new ArrayList<Integer>();
					imgIndices.put(y, list);
				}
				
				list.add(x);
			}
		}
		System.out.println("Time to construct aux indice: " +(System.currentTimeMillis() - time) );
		
		time = System.currentTimeMillis();
		Dataset modisDataset = gdal.Open("/media/savio/67E3BA5B4FBF63B1/pa_br_mod13q1_ndvi_250_2000_2016.tif", gdalconstConstants.GA_ReadOnly);
		
		int numBands = modisDataset.getRasterCount();
		int modisBandList[] = new int[numBands];
		for (int i = 0; i < numBands; i++) {
			modisBandList[i] = i + 1;
		}
		
		ThreadPoolExecutor pool = new ThreadPoolExecutor(NUM_THREADS, NUM_THREADS, 0, TimeUnit.DAYS, new LinkedBlockingQueue<Runnable>());
		
		for (int i = 0; i < NUM_THREADS; i++) {
			pool.execute(new LoadThread(i, NUM_THREADS, numBands, x_size, 1000, modisBandList, imgIndices, modisDataset));
		}
		
		pool.shutdown();
		pool.awaitTermination(10, TimeUnit.DAYS);
		
		System.out.println("Time to read modis values: " +(System.currentTimeMillis() - time));
		
	}
	
	private static class LoadThread implements Runnable {
		private int initLine;
		private int jump;
		
		private int numBands;
		private int x_size;
		private int y_size;
		
		private int modisBandList[];
		
		private Map<Integer, List<Integer>> imgIndices;
		
		private Dataset modisDataset;
		
		public LoadThread(int initLine, int jump,
				int numBands, int x_size, int y_size, int[] modisBandList,
				Map<Integer, List<Integer>> imgIndices, Dataset modisDataset) {
			super();
			this.initLine = initLine;
			this.jump = jump;
			this.numBands = numBands;
			this.x_size = x_size;
			this.y_size = y_size;
			this.modisBandList = modisBandList;
			this.imgIndices = imgIndices;
			this.modisDataset = modisDataset;
		}

		@Override
		public void run() {
			int pixels = 1;
		    Band pBand = modisDataset.GetRasterBand(1);
		
		    // setting buffer properties
		    int bufferType = pBand.getDataType();
		    
		    int y = initLine;
		    
		    long time = System.currentTimeMillis();
			while (y <= y_size) {
				List<Integer> columns = imgIndices.get(y);
				
				//If the line y must not be processed, go to the next line
				if (columns == null) {
					y += jump;
					continue;
				}
				
				int readRaster = 0;
				int num_reads_count = 0;
				
				long timeToRead = 0;
				long t = System.currentTimeMillis();
				if (columns.size() <= MAX_COLUMNS_TO_READ_INDIVIDUALLY) {
					int local_x_size = 1;
					int x_off = columns.get(0);
					int last_x = columns.get(0);
					
					
					//Try to read sequential blocks of pixels
					for (int i = 1; i < columns.size(); i++) {
						int x = columns.get(i);
						
						//If x the next integer from the last column pixel value read
						if (x == last_x + 1) {
							local_x_size++;
							last_x = x;
							continue;
						}
						
						float[] bandsValues = new float[numBands * local_x_size];
						synchronized (ParallelLoadTifData.class) {
							long local_t = System.currentTimeMillis();
							readRaster = modisDataset.ReadRaster(x_off, y, local_x_size, 1, local_x_size, 1, bufferType, bandsValues, modisBandList);
							timeToRead += System.currentTimeMillis() - local_t;
						}
						num_reads_count++;
						
						x_off = x;
						last_x = x;
						local_x_size = 1;
					}
					
					//Read the last pixels values not read
					float[] bandsValues = new float[numBands * local_x_size];
					synchronized (ParallelLoadTifData.class) {
						long local_t = System.currentTimeMillis();
						readRaster = modisDataset.ReadRaster(x_off, y, local_x_size, 1, local_x_size, 1, bufferType, bandsValues, modisBandList);
						timeToRead += System.currentTimeMillis() - local_t;
					}
					num_reads_count++;
				} else {
					float[] bandsValues = new float[numBands * x_size];
					synchronized (ParallelLoadTifData.class) {
						long local_t = System.currentTimeMillis();
						readRaster = modisDataset.ReadRaster(0, y, x_size, pixels, x_size, pixels, bufferType, bandsValues, modisBandList);
						timeToRead += System.currentTimeMillis() - local_t;
					}
					num_reads_count++;
				}
				
				if (readRaster != 0)
					System.err.println("ERRO NA LINHA: " +y);
				
				System.out.println("Line: " +y +"\t" +initLine +"\t" +columns.size() +"\t" +num_reads_count +"\t" +timeToRead +"\t" +(System.currentTimeMillis() - t));
				y += jump;
			}
			System.out.println("Time to read modis values: " +(System.currentTimeMillis() - time) +" on Thread " +initLine);
		}
	}
}
