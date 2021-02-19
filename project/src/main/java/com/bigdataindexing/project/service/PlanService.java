package com.bigdataindexing.project.service;

import org.json.JSONObject;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class PlanService {

    private JedisPool jedisPool;

    private JedisPool getJedisPool() {
        if (this.jedisPool == null) {
            this.jedisPool = new JedisPool();
        }
        return this.jedisPool;
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
