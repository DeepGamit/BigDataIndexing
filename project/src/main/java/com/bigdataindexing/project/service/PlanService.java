package com.bigdataindexing.project.service;

import com.bigdataindexing.project.controller.PlanController;
import com.bigdataindexing.project.exception.InvalidInputException;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class PlanService {

    private JedisPool jedisPool;

    private JedisPool getJedisPool() {
        if (this.jedisPool == null) {
            this.jedisPool = new JedisPool();
        }
        return this.jedisPool;
    }

    // Validate Plan against the Plan Schema
    public void validatePlan(JSONObject plan){

        JSONObject jsonSchema = new JSONObject(
                new JSONTokener(PlanController.class.getResourceAsStream("/planSchema.json")));

        Schema planSchema = SchemaLoader.load(jsonSchema);

        try {
            planSchema.validate(plan);
        } catch (ValidationException e){
            e.getCausingExceptions().stream().map(ValidationException::getMessage).forEach(System.out::println);
            throw new InvalidInputException("Invalid Input! Error: " + e.getMessage());

        }
    }

    // Generate E-Tag
    public String generateETag(JSONObject object){
        String tag = String.valueOf(object.hashCode());
        System.out.println(tag);
        return tag;
    }


    public String savePlan(JSONObject jsonObject, String objectType){

        // temp array of keys to remove from JSON Object
        ArrayList<String> objectKeysToDelete = new ArrayList<String>();

        Iterator<String> keys = jsonObject.keys();
        String objectID = (String) jsonObject.get("objectId");

        while (keys.hasNext()){
            String key = keys.next();
            Object current = jsonObject.get(key);

            if(current instanceof  JSONObject){

                JSONObject currentObject = (JSONObject) current;
                String objectKey = this.savePlan(currentObject, key);
                // remove this value from JSON Object, as it will be stored separately
                objectKeysToDelete.add(key);

                Jedis jedis = this.jedisPool.getResource();
                String relKey = objectType + "_" + jsonObject.get("objectId") + "_" + key;
                jedis.set(relKey, objectKey);
                jedis.close();

            } else if (current instanceof JSONArray){
                JSONArray currentArray = (JSONArray) current;
                String[] tempArrayObject = new String[currentArray.length()];

                for (int i = 0; i < currentArray.length(); i++) {
                    Object currentArrayObject = currentArray.get(i);
                    if (currentArrayObject instanceof JSONObject) {
                        JSONObject arrayObject = (JSONObject)currentArrayObject;
                        String arrayObjectKey = this.savePlan(arrayObject, (String)arrayObject.get("objectType"));

                        tempArrayObject[i] = arrayObjectKey;
                    }
                }

                // remove this value from JSON Object, as it will be stored separately
                objectKeysToDelete.add(key);

                Jedis jedis = this.getJedisPool().getResource();
                String relKey = objectType + "_" + jsonObject.get("objectId") + "_" + key;
                jedis.set(relKey, Arrays.toString(tempArrayObject));
                jedis.close();
            }
        }

        // Remove objects from json that are stored separately
        for (String key : objectKeysToDelete) {
            jsonObject.remove(key);
        }

        // Save the Object in Redis
        String objectKey = objectType + "_" + objectID;
        System.out.println(objectKey);
        Jedis jedis = this.getJedisPool().getResource();
        jedis.set(objectKey, jsonObject.toString());
        jedis.close();

        return objectKey;

    }

    public JSONObject getPlan(String objectKey){

        JedisPool jedisPool = new JedisPool();
        Jedis jedis;
        JSONObject jsonObject;

        jedis = jedisPool.getResource();
        String jsonString = jedis.get(objectKey);
        jedis.close();
        if (jsonString == null || jsonString.isEmpty()) {
            return null;
        }
        jsonObject = new JSONObject(jsonString);

        return  jsonObject;

    }
}
