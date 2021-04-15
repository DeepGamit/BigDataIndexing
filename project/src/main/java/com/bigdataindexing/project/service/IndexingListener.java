package com.bigdataindexing.project.service;

import org.apache.http.HttpHost;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.*;
import java.util.List;

@Component
public class IndexingListener {

    private static RestHighLevelClient client = new RestHighLevelClient(
            RestClient.builder(new HttpHost("localhost", 9200, "http")));
    private static final String IndexName="planindex";


    public void receiveMessage(Map<String, String> message) throws IOException {
        System.out.println("Message received: " + message);

            String operation = message.get("operation");
            String body = message.get("body");
            JSONObject jsonBody = new JSONObject(body);

            switch (operation) {
                case "SAVE": {
                    postDocument(jsonBody);
                    break;
                }
                case "DELETE": {
                    deleteDocument(jsonBody.get("objectId").toString());
                    break;
                }
            }
    }

    private static void postDocument(JSONObject plan) throws IOException {
        if(!indexExists()) {
            createElasticIndex();
        }
        IndexRequest request = new IndexRequest(IndexName);
        convertMapToDocumentIndex(plan,"", request, "plan" );
    }

    private static void deleteDocument(String documentId) throws IOException {
        DeleteRequest request = new DeleteRequest(IndexName, documentId);
        DeleteResponse deleteResponse = client.delete(
                request, RequestOptions.DEFAULT);
        if (deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
            System.out.println("Document " +documentId+" Not Found!!");
        }
    }

    private static Map<String, Map<String, Object>> convertMapToDocumentIndex (JSONObject jsonObject,
                                                                 String parentId, IndexRequest request, String objectName ) throws IOException {

        Map<String, Map<String, Object>> map = new HashMap<>();
        Map<String, Object> valueMap = new HashMap<>();
        Iterator<String> iterator = jsonObject.keys();

        while (iterator.hasNext()){

            String key = iterator.next();
            String redisKey = jsonObject.get("objectType") + ":" + parentId;
            Object value = jsonObject.get(key);

            if (value instanceof JSONObject) {

                convertMapToDocumentIndex((JSONObject) value, jsonObject.get("objectId").toString(), request, key.toString());

            } else if (value instanceof JSONArray) {

                convertToList((JSONArray) value, jsonObject.get("objectId").toString(), request, key.toString());

            } else {
                valueMap.put(key, value);
                map.put(redisKey, valueMap);
            }
        }

        Map<String, Object> temp = new HashMap<>();
        if(objectName == "plan"){
            valueMap.put("plan_join", objectName);
        } else {
            temp.put("name", objectName);
            temp.put("parent", parentId);
            valueMap.put("plan_join", temp);
        }
        request.id(jsonObject.get("objectId").toString());
        request.source(valueMap);
        request.routing(parentId);
        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
        System.out.println("response id: " + indexResponse.getId() +" parent id: " + parentId);

        return map;
    }

    private static List<Object> convertToList(JSONArray array, String parentId, IndexRequest request, String objectName) throws IOException {
        List<Object> list = new ArrayList<>();
        for (int i = 0; i < array.length(); i++) {
            Object value = array.get(i);
            if (value instanceof JSONArray) {
                value = convertToList((JSONArray) value, parentId, request, objectName);
            } else if (value instanceof JSONObject) {
                value = convertMapToDocumentIndex((JSONObject) value, parentId, request, objectName);
            }
            list.add(value);
        }
        return list;
    }

    private static boolean indexExists() throws IOException {
        GetIndexRequest request = new GetIndexRequest(IndexName);
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        return exists;
    }

    private static void createElasticIndex() throws IOException {
        CreateIndexRequest request = new CreateIndexRequest(IndexName);
        request.settings(Settings.builder().put("index.number_of_shards", 3).put("index.number_of_replicas", 2));
        XContentBuilder mapping = getMapping();
        request.mapping(mapping);
        CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);

        boolean acknowledged = createIndexResponse.isAcknowledged();
        System.out.println("Index Creation:" + acknowledged);

    }

    private static XContentBuilder getMapping() throws IOException {

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("properties");
            {
                builder.startObject("plan");
                {
                    builder.startObject("properties");
                    {
                        builder.startObject("_org");
                        {
                            builder.field("type", "text");
                        }
                        builder.endObject();
                        builder.startObject("objectId");
                        {
                            builder.field("type", "keyword");
                        }
                        builder.endObject();
                        builder.startObject("objectType");
                        {
                            builder.field("type", "text");
                        }
                        builder.endObject();
                        builder.startObject("planType");
                        {
                            builder.field("type", "text");
                        }
                        builder.endObject();
                        builder.startObject("creationDate");
                        {
                            builder.field("type", "date");
                            builder.field("format", "MM-dd-yyyy");
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();
                builder.startObject("planCostShares");
                {
                    builder.startObject("properties");
                    {
                        builder.startObject("copay");
                        {
                            builder.field("type", "long");
                        }
                        builder.endObject();
                        builder.startObject("deductible");
                        {
                            builder.field("type", "long");
                        }
                        builder.endObject();
                        builder.startObject("_org");
                        {
                            builder.field("type", "text");
                        }
                        builder.endObject();
                        builder.startObject("objectId");
                        {
                            builder.field("type", "keyword");
                        }
                        builder.endObject();
                        builder.startObject("objectType");
                        {
                            builder.field("type", "text");
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();
                builder.startObject("linkedPlanServices");
                {
                    builder.startObject("properties");
                    {
                        builder.startObject("_org");
                        {
                            builder.field("type", "text");
                        }
                        builder.endObject();
                        builder.startObject("objectId");
                        {
                            builder.field("type", "keyword");
                        }
                        builder.endObject();
                        builder.startObject("objectType");
                        {
                            builder.field("type", "text");
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();
                builder.startObject("linkedService");
                {
                    builder.startObject("properties");
                    {
                        builder.startObject("name");
                        {
                            builder.field("type", "text");
                        }
                        builder.endObject();
                        builder.startObject("_org");
                        {
                            builder.field("type", "text");
                        }
                        builder.endObject();
                        builder.startObject("objectId");
                        {
                            builder.field("type", "keyword");
                        }
                        builder.endObject();
                        builder.startObject("objectType");
                        {
                            builder.field("type", "text");
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();
                builder.startObject("planserviceCostShares");
                {
                    builder.startObject("properties");
                    {
                        builder.startObject("copay");
                        {
                            builder.field("type", "long");
                        }
                        builder.endObject();
                        builder.startObject("deductible");
                        {
                            builder.field("type", "long");
                        }
                        builder.endObject();
                        builder.startObject("_org");
                        {
                            builder.field("type", "text");
                        }
                        builder.endObject();
                        builder.startObject("objectId");
                        {
                            builder.field("type", "keyword");
                        }
                        builder.endObject();
                        builder.startObject("objectType");
                        {
                            builder.field("type", "text");
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();
                builder.startObject("plan_join");
                {
                    builder.field("type", "join");
                    builder.startObject("relations");
                    {
                        builder.array("plan", "planCostShares", "linkedPlanServices");
                        builder.array("linkedPlanServices", "linkedService", "planserviceCostShares");
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();

        return builder;

    }
}
