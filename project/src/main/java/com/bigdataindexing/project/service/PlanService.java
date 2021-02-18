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
    public void validatePlan(JSONObject plan) {

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

    public boolean checkIfKeyExists(String objectKey) {

        Jedis jedis = this.getJedisPool().getResource();
        String jsonString = jedis.get(objectKey);
        jedis.close();
        if (jsonString == null || jsonString.isEmpty()) {
            return false;
        } else {
            return true;
        }

    }

    public String savePlan(JSONObject jsonObject) {

        // Save the Object in Redis
        String objectKey = (String) jsonObject.get("objectId");
        Jedis jedis = this.getJedisPool().getResource();
        jedis.set(objectKey, jsonObject.toString());
        jedis.close();

        return objectKey;

    }

    public JSONObject getPlan(String objectKey) {

        Jedis jedis = this.getJedisPool().getResource();

        String jsonString = jedis.get(objectKey);
        jedis.close();

        if (jsonString == null || jsonString.isEmpty()) {
            return null;
        }

        JSONObject jsonObject = new JSONObject(jsonString);

        return  jsonObject;
    }

    public void deletePlan(String objectKey) {

        Jedis jedis = this.getJedisPool().getResource();
        jedis.del(objectKey);
        jedis.close();

    }

}
