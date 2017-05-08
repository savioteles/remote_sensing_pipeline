package com.gogeo.experimental.flink.remote_sensing;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.script.ScriptException;

import org.gdal.gdal.Band;
import org.gdal.gdal.Dataset;
import org.gdal.gdal.gdal;
import org.gdal.gdalconst.gdalconstConstants;

public class ConvertTifToCsvData {
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

		Map<Integer, List<Integer>> imgIndices = new TreeMap<Integer, List<Integer>>();
	    
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

	    Band pBand = modisDataset.GetRasterBand(1);
	
	    // setting buffer properties
	    int bufferType = pBand.getDataType();
	    
	    String outputFolder = "/media/savio/67E3BA5B4FBF63B1/modis_ndvi";
	    File file = new File(outputFolder);
	    if (!file.exists())
	    	file.mkdir();
	    
		for (int y = 0; y < y_size; y++) {
			List<Integer> columns = imgIndices.get(y);
			
			//If the line y must not be processed, go to the next line
			if (columns == null || columns.size() < 3)
				continue;
			
			String outputFile = outputFolder +"/" +y +".txt";
			if (new File(outputFile).exists())
				continue;
			String outputStr = "";
			int readRaster = 0;
			int num_reads_count = 0;
			
			long t = System.currentTimeMillis();
			int local_x_size = 1;
			int x_off = columns.get(0);
			int last_x = columns.get(0);
			
			List<Integer> columnsToStore = new ArrayList<Integer>();
			columnsToStore.add(last_x);
			
			//Try to read sequential blocks of pixels
			for (int i = 1; i < columns.size(); i++) {
				int x = columns.get(i);
				
				//If x the next integer from the last column pixel value read
				if (x == last_x + 1) {
					local_x_size++;
					last_x = x;
					columnsToStore.add(last_x);
					continue;
				}
				
				float[] bandsValues = new float[numBands * local_x_size];
				readRaster = modisDataset.ReadRaster(x_off, y, local_x_size, 1, local_x_size, 1, bufferType, bandsValues, modisBandList);
				if (readRaster != 0) {
					System.err.println("ERRO NA LINHA: " +y);
					continue;
				}
				num_reads_count++;
				
				outputStr = writeNdviValues(x_size, columnsToStore, y, local_x_size, numBands, bandsValues, outputStr);
				
				x_off = x;
				last_x = x;
				local_x_size = 1;
				
				columnsToStore.clear();
				columnsToStore.add(last_x);
			}
			
			//Read the last pixels values not read
			float[] bandsValues = new float[numBands * local_x_size];
			readRaster = modisDataset.ReadRaster(x_off, y, local_x_size, 1, local_x_size, 1, bufferType, bandsValues, modisBandList);
			if (readRaster != 0) {
				System.err.println("ERRO NA LINHA: " +y);
				continue;
			}
			
			outputStr = writeNdviValues(x_size, columnsToStore, y, local_x_size, numBands, bandsValues, outputStr);
			
			num_reads_count++;
			
			BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile));
			bw.write(outputStr);
			bw.close();
			
			System.out.println("Line: " +y +"\t" +columns.size() +"\t" +num_reads_count +"\t" +(System.currentTimeMillis() - t));
		}
		System.out.println("Time to read modis values: " +(System.currentTimeMillis() - time));
		
	}
	
	private static String writeNdviValues(int x_size, List<Integer> columnsToStore, int y, int local_x_size, int numBands, float[] bandsValues, String outputStr) throws IOException {
		int line_indice = y * x_size;
		int j = 0;
		for (int x_off: columnsToStore) {
			int indice = line_indice + x_off;
			String out = indice +",";
			int array_offset = j++ * numBands;
			for (int a = array_offset; a < array_offset + numBands; a++) 
				out += bandsValues[a] +",";
			out = out.substring(0, out.length() - 1) +"\n";
			outputStr += out;
		}
		
		return outputStr;
	}
}
