package com.gogeo.experimental.flink.remote_sensing.jobs;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;

import javax.script.ScriptException;

import org.apache.commons.io.IOUtils;
import org.apache.flink.api.common.functions.RichMapFunction;

import com.gogeo.real_time.objects.Params;
import com.gogeo.real_time.objects.Script;

import serialization.IObjectSerializer;
import serialization.SerializeServiceFactory;

public class Rscript extends RichMapFunction<byte[], String> {
    private static final long serialVersionUID = 1L;
    

    @Override
    public String map(byte[] value) throws Exception {
    	IObjectSerializer serializer = SerializeServiceFactory.getObjectSerializer();
    	
    	Script script = serializer.unserialize(value, Script.class);
    	File scriptFile = script.getScriptFile();
    	Params params = script.getScriptParams();
    	
    	//Create output dir
    	File outputDir = new File(params.getOutputDir());
		Files.createDirectories(outputDir.toPath());
    	
    	String output = runScript(scriptFile, params); 
    	script.cleanFiles();
    	
		return output;
    }

    public String runScript(File script, Params params) throws IOException,
            InterruptedException, ScriptException {

        String command = "sudo Rscript " +script.getAbsolutePath();
        if (params != null)
        	command += " " +params;
        Process child = Runtime.getRuntime().exec(command
                );

        int code = child.waitFor();
        
        String output = "";

        switch (code) {
        case 0:
            BufferedReader br = new BufferedReader(
                    new InputStreamReader(
                            child.getInputStream()));
            String line;
            while ((line = br.readLine()) != null) {
                output += line +"\n";
            }
            System.out.println(output);
            return output.substring(0, output.length() - 1);
        default:
            // Read the error stream then
            String message = IOUtils.toString(child.getErrorStream(), Charset.defaultCharset());
            System.err.println("ERRO: " + message);
            System.err.println(command);
            return "";
        }
    }
}
