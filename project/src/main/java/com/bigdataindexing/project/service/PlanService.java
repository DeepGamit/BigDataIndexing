package com.bigdataindexing.project.service;

import com.bigdataindexing.project.controller.PlanController;
import com.bigdataindexing.project.exception.InvalidInputException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.*;

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

    public boolean checkIfKeyExists(String objectKey){

        Jedis jedis = this.getJedisPool().getResource();
        String jsonString = jedis.get(objectKey);
        jedis.close();
        if (jsonString == null || jsonString.isEmpty()) {
            return false;
        } else {
            return true;
        }

    }

    public String savePlan(JSONObject jsonObject, String objectType){

        // temp array of keys to remove from JSON Object
        ArrayList<String> objectKeysToDelete = new ArrayList<String>();

        Iterator<String> iterator = jsonObject.keys();
        String objectID = (String) jsonObject.get("objectId");

        while (iterator.hasNext()){
            String key = iterator.next();
            Object current = jsonObject.get(key);

            if(current instanceof  JSONObject){

                JSONObject currentObject = (JSONObject) current;
                String objectKey = this.savePlan(currentObject, (String)currentObject.get("objectType"));
                // remove this value from JSON Object, as it will be stored separately
                objectKeysToDelete.add(key);

                Jedis jedis = this.jedisPool.getResource();
                String relKey = jsonObject.get("objectId") + "_" + key;
//                String relKey = objectType + "_" + jsonObject.get("objectId") + "_" + key;
                System.out.println(relKey + " :---: " + objectKey);
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
                String relKey = jsonObject.get("objectId") + "_" + key;
//                String relKey = objectType + "_" +jsonObject.get("objectId") + "_" + key;
                System.out.println(relKey + " :---: " + Arrays.toString(tempArrayObject));
                jedis.set(relKey, Arrays.toString(tempArrayObject));
                jedis.close();
            }
        }

        // Remove objects from json that are stored separately
        for (String key : objectKeysToDelete) {
            jsonObject.remove(key);
        }

        // Save the Object in Redis
        String objectKey = objectID;
//        String objectKey = objectType + "_" + objectID;
        System.out.println(objectKey + " :---: " + jsonObject.toString());
        Jedis jedis = this.getJedisPool().getResource();
        jedis.set(objectKey, jsonObject.toString());
        jedis.close();

        return objectKey;

    }

    public JSONObject getPlan(String objectKey) {

        JedisPool jedisPool = new JedisPool();
        Jedis jedis;
        JSONObject jsonObject;

        if (isStringArray(objectKey)) {
            ArrayList<JSONObject> arrayValue = getFromArrayString(objectKey);
            jsonObject = new JSONObject(arrayValue);
        } else {

            jedis = jedisPool.getResource();
            String jsonString = jedis.get(objectKey);
            jedis.close();
            if (jsonString == null || jsonString.isEmpty()) {
                return null;
            }
            jsonObject = new JSONObject(jsonString);
        }
        jedis = jedisPool.getResource();
        Set<String> relatedKeys = jedis.keys(objectKey + "_*");
        jedis.close();

       for(String relatedKey: relatedKeys){

           String partialObjectKey = relatedKey.substring(relatedKey.lastIndexOf('_')+1);

           jedis = jedisPool.getResource();
           String partialObjectDBKey = jedis.get(relatedKey);
           jedis.close();

           if (partialObjectDBKey == null || partialObjectDBKey.isEmpty()) {
               continue;
           }

           if(isStringArray(partialObjectDBKey)) {
               ArrayList<JSONObject> arrayValue = getFromArrayString(partialObjectDBKey);
               jsonObject.put(partialObjectKey, arrayValue);
           } else {
               JSONObject partObj = this.getPlan(partialObjectDBKey);
               jsonObject.put(partialObjectKey, partObj);
           }

       }
        return  jsonObject;
    }

    public boolean deletePlan(String objectKey){

        JedisPool jedisPool = new JedisPool();
        Jedis jedis;

        if(isStringArray(objectKey)) {
            // delete all keys in the array
            String[] arrayKeys = objectKey.substring(objectKey.indexOf("[")+1, objectKey.lastIndexOf("]")).split(", ");
            for (String key : arrayKeys) {
                if(!this.deletePlan(key)) {
                    return false;
                }
            }
        } else{
            jedis = jedisPool.getResource();
            if(jedis.del(objectKey) < 1) {
                // deletion failed
                jedis.close();
                return false;
            }
            jedis.close();
        }

        // fetch additional relations for the object, if present
        jedis = jedisPool.getResource();
        Set<String> relatedKeys = jedis.keys(objectKey + "_*");
        System.out.println(relatedKeys);
        jedis.close();
        for(String relatedKey: relatedKeys) {

            String partialObjectKey = relatedKey.substring(relatedKey.lastIndexOf('_')+1);

            // fetch the id stored at partObjKey
            jedis = jedisPool.getResource();
            String partialObjectDBKey = jedis.get(partialObjectKey);
            if(jedis.del(relatedKey) < 1) {
                //deletion failed
                return false;
            }
            jedis.close();
            if (partialObjectDBKey == null || partialObjectDBKey.isEmpty()) {
                continue;
            }

            if(isStringArray(partialObjectDBKey)) {
                // delete all keys in the array
                String[] arrayKeys = partialObjectDBKey.substring(partialObjectDBKey.indexOf("[")+1, partialObjectDBKey.lastIndexOf("]")).split(", ");
                for (String key : arrayKeys) {
                    if(!this.deletePlan(key)) {
                        //deletion failed
                        return false;
                    }
                }
            } else {
                if(!this.deletePlan(partialObjectDBKey)){
                    //deletion failed
                    return false;
                }
            }
        }

        return true;
    }

    private boolean isStringArray(String str) {
        if (str.indexOf('[') < str.indexOf(']')) {
            if (str.substring((str.indexOf('[') + 1), str.indexOf(']')).split(", ").length > 0)
                return true;
            else
                return false;
        } else {
            return false;
        }
    }

    private ArrayList<JSONObject> getFromArrayString(String keyArray) {
        ArrayList<JSONObject> jsonArray = new ArrayList<>();
        String[] array = keyArray.substring((keyArray.indexOf('[') + 1), keyArray.indexOf(']')).split(", ");

        for (String key : array) {
            JSONObject partialObject = this.getPlan(key);
            jsonArray.add(partialObject);
        }

        return jsonArray;
    }
}
