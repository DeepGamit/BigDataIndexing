package com.bigdataindexing.project.service;

import org.json.JSONObject;
import org.json.JSONArray;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.*;

public class PlanService {

    private JedisPool jedisPool;
    ETagManager eTagManager = new ETagManager();

    private JedisPool getJedisPool() {
        if (this.jedisPool == null) {
            this.jedisPool = new JedisPool();
            System.out.println(this.jedisPool);
        }
        return this.jedisPool;
    }

    public boolean checkIfKeyExists(String objectKey) {

        Jedis jedis = this.getJedisPool().getResource();
        jedis.close();
        return jedis.exists(objectKey);

    }

    public String getEtag(String key) {
        Jedis jedis = this.getJedisPool().getResource();
        return jedis.hget(key, "eTag");
    }

    public String setEtag(String key, JSONObject jsonObject){
        String eTag = eTagManager.getETag(jsonObject);
        Jedis jedis = this.getJedisPool().getResource();
        jedis.hset(key, "eTag", eTag);
        return eTag;
    }

    public String savePlan(JSONObject planObject, String key){

        convertToMap(planObject);
        return this.setEtag(key, planObject);
    }

    public Map<String, Object> getPlan(String key){
        Map<String, Object> outputMap = new HashMap<String, Object>();
        getOrDeleteData(key, outputMap, false);
        return outputMap;
    }

    public void deletePlan(String objectKey) {
        getOrDeleteData(objectKey, null, true);
    }

    public Map<String, Map<String, Object>> convertToMap(JSONObject jsonObject) {

        Map<String, Map<String, Object>> map = new HashMap<>();
        Map<String, Object> valueMap = new HashMap<>();
        Iterator<String> iterator = jsonObject.keys();

        while (iterator.hasNext()){

            System.out.println(jsonObject.get("objectType") + ":" + jsonObject );
            String redisKey = jsonObject.get("objectType") + ":" + jsonObject.get("objectId");
            System.out.println(redisKey);
            String key = iterator.next();
            Object value = jsonObject.get(key);

            if (value instanceof JSONObject) {

                value = convertToMap((JSONObject) value);
                HashMap<String, Map<String, Object>> val = (HashMap<String, Map<String, Object>>) value;
                Jedis jedis = this.getJedisPool().getResource();
                jedis.sadd(redisKey + ":" + key, val.entrySet().iterator().next().getKey());
                jedis.close();

            } else if (value instanceof JSONArray) {
                value = convertToList((JSONArray) value);
                for (HashMap<String, HashMap<String, Object>> entry : (List<HashMap<String, HashMap<String, Object>>>) value) {
                    for (String listKey : entry.keySet()) {
                        Jedis jedis = this.getJedisPool().getResource();
                        jedis.sadd(redisKey + ":" + key, listKey);
                        jedis.close();
                        System.out.println(redisKey + ":" + key + " : " + listKey);
                    }
                }
            } else {
                Jedis jedis = this.getJedisPool().getResource();
                jedis.hset(redisKey, key, value.toString());
                jedis.close();
                valueMap.put(key, value);
                map.put(redisKey, valueMap);
            }
        }

        System.out.println("MAP: " + map.toString());
        return map;

    }

    private List<Object> convertToList(JSONArray array) {
        List<Object> list = new ArrayList<>();
        for (int i = 0; i < array.length(); i++) {
            Object value = array.get(i);
            if (value instanceof JSONArray) {
                value = convertToList((JSONArray) value);
            } else if (value instanceof JSONObject) {
                value = convertToMap((JSONObject) value);
            }
            list.add(value);
        }
        return list;
    }

    private boolean isStringDouble(String s) {
        try {
            Double.parseDouble(s);
            return true;
        } catch (NumberFormatException ex) {
            return false;
        }
    }


    private Map<String, Object> getOrDeleteData(String redisKey, Map<String, Object> outputMap, boolean isDelete) {
        Jedis jedis = this.getJedisPool().getResource();
        Set<String> keys = jedis.keys(redisKey + "*");
        for (String key : keys) {
            if (key.equals(redisKey)) {
                if (isDelete) {
                    jedis.del(new String[] {key});
                } else {
                    Map<String, String> val =  jedis.hgetAll(key);
                    for (String name : val.keySet()) {
                        if (!name.equalsIgnoreCase("eTag")) {
                            outputMap.put(name,
                                    isStringDouble(val.get(name)) ? Double.parseDouble(val.get(name)) : val.get(name));
                        }
                    }
                }

            } else {
                String newStr = key.substring((redisKey + ":").length());
                System.out.println("Key to be serched :" +key+"--------------"+newStr);
                Set<String> members = jedis.smembers(key);
                System.out.println(members);
                if (members.size() > 1) {
                    List<Object> listObj = new ArrayList<Object>();
                    for (String member : members) {
                        if (isDelete) {
                            getOrDeleteData(member, null, true);
                        } else {
                            Map<String, Object> listMap = new HashMap<String, Object>();
                            listObj.add(getOrDeleteData(member, listMap, false));

                        }
                    }
                    if (isDelete) {
                        jedis.del(new String[] {key});
                    } else {
                        outputMap.put(newStr, listObj);
                    }

                } else {
                    if (isDelete) {
                        jedis.del(new String[]{members.iterator().next(), key});
                    } else {
                        Map<String, String> val = jedis.hgetAll(members.iterator().next());
                        Map<String, Object> newMap = new HashMap<String, Object>();
                        for (String name : val.keySet()) {
                            newMap.put(name,
                                    isStringDouble(val.get(name)) ? Double.parseDouble(val.get(name)) : val.get(name));
                        }
                        outputMap.put(newStr, newMap);
                    }
                }
            }
        }
        return outputMap;
    }

}
