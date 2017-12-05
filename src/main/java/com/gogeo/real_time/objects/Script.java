package com.gogeo.real_time.objects;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.util.UUID;

public class Script implements Serializable {

	private static final long serialVersionUID = 1L;
	private byte[] scriptFile;
	private Params scriptParams;
	private byte[] dataFile;
	
	private transient File localScriptFile;
	private transient File localDataFile;

	public Script(File scriptFile, Params scriptParams) throws IOException {
		this(scriptFile, scriptParams, null);
	}

	public Script(File scriptFile, Params scriptParams, File dataFile) throws IOException {
		super();
		this.scriptParams = scriptParams;
		this.scriptFile = Files.readAllBytes(scriptFile.toPath());
		
		if (dataFile != null)
			this.dataFile = Files.readAllBytes(dataFile.toPath());
	}

	public File getScriptFile() throws IOException {
		localScriptFile = new File("/tmp/" +UUID.randomUUID().toString());
		Files.write(localScriptFile.toPath(), scriptFile);
		return localScriptFile;
	}

	public Params getScriptParams() {
		return scriptParams;
	}

	public File getDataFile() throws IOException {
		localDataFile = new File("/tmp/" +UUID.randomUUID().toString());
		Files.write(localDataFile.toPath(), dataFile);
		return localDataFile;
	}
	
	public void cleanFiles() throws IOException {
		if (localDataFile != null)
			Files.delete(localDataFile.toPath());
		Files.delete(localScriptFile.toPath());
	}
}