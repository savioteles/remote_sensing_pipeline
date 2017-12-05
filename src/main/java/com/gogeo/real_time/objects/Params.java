package com.gogeo.real_time.objects;

import java.io.Serializable;

public class Params implements Serializable {

	private static final long serialVersionUID = 1L;
	private int initIndex;
	private int endIndex;
	private String inputFile;
	private String outputDir;

	public Params(int initIndex, int endIndex, String inputFile, String outputDir) {
		super();
		this.initIndex = initIndex;
		this.endIndex = endIndex;
		this.inputFile = inputFile;
		this.outputDir = outputDir;
	}

	public int getInitIndex() {
		return initIndex;
	}

	public int getEndIndex() {
		return endIndex;
	}

	public String getInputFile() {
		return inputFile;
	}

	public String getOutputDir() {
		return outputDir;
	}
	
	@Override
	public String toString() {
		return initIndex +" " +endIndex +" " +inputFile +" " +outputDir;
	}
}
