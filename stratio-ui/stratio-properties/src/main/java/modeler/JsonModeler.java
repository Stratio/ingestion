package modeler;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.ingestion.model.AgentComponent;
import com.stratio.ingestion.model.source.Source;

/**
 * Created by miguelsegura on 18/09/15.
 */
public class JsonModeler {

    private static final Logger log = LoggerFactory.getLogger(JsonModeler.class);

    private String element;
    private AgentComponent agent = new AgentComponent();
    private List<Source> listSources = new ArrayList<>();
//    private Source source = new Source();
//    private Sink sink = new Sink();
//    private Channel channel = new Channel();


    public JsonModeler(String jsonFile)
            throws IOException {

        try {

            ObjectMapper mapper = new ObjectMapper();

            BufferedReader fileReader = new BufferedReader(
                    new FileReader(jsonFile));

            JsonNode rootNode = mapper.readValue(fileReader, JsonNode.class);




            /*** Create Sources ***/
            element = "sources";
            getComponents(rootNode, element, agent);

            /*** Create Sinks ***/
            element = "sinks";

            getComponents(rootNode, element, agent);

            /*** Create Channels ***/
            element = "channels";

            getComponents(rootNode, element, agent);

            /*** Join Elements ***/

//            element = "sources";
//            getComponents(rootNode, element);
//
//            element = "sinks";
//            getComponents(rootNode, element);




            return;
        } catch (Exception e) {
            throw new IOException("Cannot serialize JSON", e);
        }
    }


//    private String formatString(String inputString){
//
//        inputString = inputString.replace("\"", "");
//        inputString = inputString.replace("[", "");
//        inputString = inputString.replace("]", "");
//        //        inputString = inputString.replace(",", " ");
//
//        return inputString;
//    }



    private void getComponents(JsonNode rootNode, String elements, AgentComponent component) throws IOException {


        JsonNode sourcesNode = rootNode.path(elements);

        JsonNode componentsNode = sourcesNode.path("components");
        Iterator<JsonNode> compIte = componentsNode.getElements();


//        while (compIte.hasNext()) {
//
//        }


        List<JsonNode> settings = new ArrayList<>();
        compIte = componentsNode.getElements();
        while (compIte.hasNext()) {
            JsonNode compNext = compIte.next();

            JsonNode id = compNext.path("id");
            JsonNode type = compNext.path("type");
            JsonNode name = compNext.path("name");
            JsonNode description = compNext.path("description");
            String idString = component.formatString(id.asText());
            String typeString = component.formatString(type.asText());
            String nameString = component.formatString(name.asText());
            String descripString = component.formatString(description.asText());

            List<JsonNode> listSettings = compNext.findValues("settings");
            JsonNode settingsValues = listSettings.get(0);
            Iterator<JsonNode> settingsNode2 = settingsValues.getElements();
            JsonNode settingsNode3 = settingsNode2.next();
            Iterator<JsonNode> listValue = settingsNode3.getElements();
            while(listValue.hasNext()) {
                JsonNode node = listValue.next();
                settings.add(node);
            }

//            JsonNode settingsNode = compNext.path("settings");
//            for(JsonNode node : list){
//                Iterator<JsonNode> nodes = node.getElements();
//                while (nodes.hasNext()) {
//                    JsonNode js = nodes.next();
//                    Iterator<JsonNode> js2 = js.getElements();
//                    while (js2.hasNext()) {
//                        JsonNode js3 = js2.next();
//                        System.out.println(js3);
//                        System.out.println();
//                    }
//                    System.out.println(js);
//                    System.out.println();
//                }
//                System.out.println(nodes);
//                System.out.println();
//                System.out.println();
//            }

//            Iterator<JsonNode> settingsIte = settingsNode.getElements();
//            JsonNode settingsList = settingsIte.next();
//
//            List<String> fieldList = new ArrayList<>();
//            Iterator<String> fieldNames = settingsList.getFieldNames();
//            while (fieldNames.hasNext()) {
//                String field = fieldNames.next();
//                String fieldName = field.toString();
//                fieldList.add(fieldName);
//            }
//
//            List<String> valueList = new ArrayList<>();
//            Iterator<JsonNode> sets = settingsList.getElements();
//            while (sets.hasNext()) {
//                JsonNode set = sets.next();
//                Iterator<JsonNode> valuesIte = set.getElements();
//                JsonNode values = valuesIte.next();
//                JsonNode typeField = values.path("type");
//                JsonNode valueField = values.path("value");
//                String typeString = typeField.toString();
//                typeString = formatString(typeString);
////                checkTypes(typeString, valueField);
//                String value = valueField.toString();
//                value = formatString(value);
//                valueList.add(value);
//            }
//
//            HashMap<String, String> mappedFiles = new HashMap<>();
//
//            for (int i = 0; i < valueList.size(); i++) {
//                if (!(valueList.get(i).equals("null")) && !(valueList.get(i).equals(""))) {
//                    mappedFiles.put(fieldList.get(i), valueList.get(i));
//                }
//            }

            if(elements.equals("sources")){

                Source source = new Source();
                source.setId(idString);
                source.setType(typeString);
                source.setName(nameString);
                source.setDescription(descripString);
                source.setSettings(settings);
                component.addSource(source);
//                source.fillSource(component.getId());
            }
//            if(elements.equals("sinks")){
//                Sink sink = new Sink();
//                sink.setSettings(settings);
//                component.setSink(sink);
//            }
//            if(elements.equals("channels")){
//                Channel channel = new Channel();
//                channel.setSettings(settings);
//                component.setChannel(channel);
//            }
            //            List<String> valueList = new ArrayList<>();
            //            Iterator<JsonNode> settings = settingsList.getElements();
            //            while (settings.hasNext()) {
            //                JsonNode set = settings.next();
            //                Iterator<JsonNode> valuesIte = set.getElements();
            //                JsonNode values = valuesIte.next();
            //                JsonNode valueField = values.path("value");
            //                String value = valueField.toString();
            //                value = formatString(value);
            //                valueList.add(value);
            //            }
            //
            //            HashMap<String, String> mappedFiles = new HashMap<>();
            //
            //            for (int i = 0; i < valueList.size(); i++) {
            //                if (!(valueList.get(i).equals("null")) && !(valueList.get(i).equals(""))) {
            //                    mappedFiles.put(fieldList.get(i), valueList.get(i));
            //                }
            //            }



        }


    }

    private void joinElements(JsonNode rootNode, String elements) throws IOException{
        JsonNode sourcesNode = rootNode.path(elements);

        JsonNode componentsNode = sourcesNode.path("components");
        Iterator<JsonNode> compIte = componentsNode.getElements();

        while (compIte.hasNext()) {
            JsonNode component = compIte.next();
            JsonNode id = component.path("id");
            JsonNode channels = component.path("channels");

        }
    }
}


