package com.stratio.ingestion.model;

import flumeClasses.Channel;
import flumeClasses.Sink;
import flumeClasses.Source;

/**
 * Created by miguelsegura on 18/09/15.
 */
public class AgentComponent {

    private String id;
    private String type;
    private String name;
    private String description;
    private Source source;
    private Sink sink;
    private Channel channel;

    public AgentComponent(){}

    public AgentComponent(String idComponent, String typeComponent, String nameComponent, String descripComponent){
        this.id = idComponent;
        this.type = typeComponent;
        this.name = nameComponent;
        this.description = descripComponent;
    }

    public String formatString(String inputString){

        inputString = inputString.replace("\"", "");
        inputString = inputString.replace("[", "");
        inputString = inputString.replace("]", "");
        //        inputString = inputString.replace(",", " ");

        return inputString;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Source getSource() {
        return source;
    }

    public void setSource(Source source) {
        this.source = source;
    }

    public Sink getSink() {
        return sink;
    }

    public void setSink(Sink sink) {
        this.sink = sink;
    }

    public Channel getChannel() {
        return channel;
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
    }
}
