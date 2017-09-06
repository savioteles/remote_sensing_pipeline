package com.gogeo.experimental.flink.remote_sensing.utils;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

public class MergeCsvPixelFiles {
	public static void main(String[] args) throws FileNotFoundException, IOException {

		CSVParser reader = new CSVParser(new FileReader("/home/savio/Shapes/LAPIG/Bfast/DADOS/masterscsv/MASTER_ALL_GROUPS_pix_cent.csv"), CSVFormat.DEFAULT);
		CSVPrinter writer = new CSVPrinter(new FileWriter("/home/savio/Shapes/LAPIG/Bfast/DADOS/pixels_central.csv"), CSVFormat.DEFAULT);
		
		boolean skipHeader = true;
		for (CSVRecord record: reader) {
			
			if (skipHeader) {
				skipHeader = false;
				continue;
			}
			
			if (record.size() == 397) {
				String id = record.get(395).replaceAll(".csv", "") +"_" +record.get(0);
				
				int index = 0;
				String[] ndviValues = new String[393];
				ndviValues[index++] = id;
				
				for (int i = 3; i < 395; i++) {
					ndviValues[index++] = record.get(i);
				}
				writer.printRecord(ndviValues);
			} else{
				String id = record.get(0).replaceAll(".csv", "");
				
				int index = 0;
				String[] ndviValues = new String[393];
				ndviValues[index++] = id;
				
				for (int i = 1; i < 393; i++) {
					ndviValues[index++] = record.get(i);
				}
				writer.printRecord(ndviValues);
			}
		}
		reader.close();
		writer.close();
	}

}
