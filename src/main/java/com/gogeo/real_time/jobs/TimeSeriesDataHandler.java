package com.gogeo.real_time.jobs;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

import javax.script.ScriptException;

import org.apache.flink.api.common.functions.RichMapFunction;

public class TimeSeriesDataHandler extends RichMapFunction<byte[], String> {
	private static final long serialVersionUID = 1L;

	@Override
	public String map(byte[] value) throws Exception {
		String tempFilePath = "/tmp/" + UUID.randomUUID().toString() + ".csv";
		Path path = Paths.get(tempFilePath);
		Files.write(path, value);
		String output = extractLines(tempFilePath);
		Files.deleteIfExists(path);
		return output;
	}

	public String extractLines(String inputFile) throws IOException,
			InterruptedException, ScriptException {
		BufferedReader br = new BufferedReader(new FileReader(inputFile));
		String output = "";
		String line;
		while ((line = br.readLine()) != null) {
			output += line + "\n";
		}
		br.close();
		return output.substring(0, output.length() - 1);
	}
}
