package com.stratio.ingestion.model.source;
/**
 * Created by miguelsegura on 18/09/15.
 */

import java.util.List;

import org.apache.flume.interceptor.Interceptor;
import org.codehaus.jackson.JsonNode;

import com.stratio.ingestion.model.AgentComponent;
import com.stratio.ingestion.model.source.ExecSource;

import flumeClasses.SpoolSource;

public class Source extends AgentComponent {



    private List<Interceptor> interceptors;
    private List<JsonNode> settings;
    private String channels;

    public void fillSource(String sourceType){
        if(sourceType.equals("spoolSource")){
            SpoolSource spoolSource = new SpoolSource();
            spoolSource.setSettings(this.settings);
        }
        if(sourceType.equals("execSource")){
            ExecSource execSource = new ExecSource();
//            execSource.setSettings(this.settings);
        }

    }

    public List<Interceptor> getInterceptors() {
        return interceptors;
    }

    public void setInterceptors(List<Interceptor> interceptors) {

        this.interceptors = interceptors;
    }

    public List<JsonNode> getSettings() {
        return settings;
    }

    public void setSettings(List<JsonNode> settings) {
        this.settings = settings;
    }

    public String getChannels() {
        return channels;
    }

    public void setChannels(String channels) {
        this.channels = channels;
    }
}
