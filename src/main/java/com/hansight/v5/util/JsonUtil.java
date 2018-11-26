package com.hansight.v5.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class JsonUtil {

    private static final Logger logger = LoggerFactory.getLogger(JsonUtil.class);

    static {
        JSON.DEFAULT_PARSER_FEATURE &= ~Feature.UseBigDecimal.getMask();
    }

    public static String toJsonStr(Object obj) {
        try {
            return JSONObject.toJSONString(obj);
        } catch (Exception e) {
            logger.error("Error parsing object to json string ! bean:[{}], exception:{}", obj, e);
        }
        return null;
    }

    public static <T> T parseObject(String jsonStr, Class<T> classType) {
        try {
            return JSONObject.parseObject(jsonStr, classType);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String parseToPrettyJson(Object o) {
        try {
            return JSON.toJSONString(o, SerializerFeature.PrettyFormat, SerializerFeature.DisableCircularReferenceDetect);
        } catch (Exception e) {
            logger.error("Error parsing object to json string ! bean:[{}], exception:{}", o, e);
        }
        return null;
    }

    public static <K, V> Map<K, V> parseMap(String jsonStr, Class<K> keyType, Class<V> valueType) {
        try {
            Map<K, V> result = new HashMap<>();
            JSONObject obj = JSONObject.parseObject(jsonStr);
            Set<String> keys = obj.keySet();
            for (String key : keys) {
                K enKey = JSONObject.parseObject(key, keyType);
                V enValue = JSONObject.parseObject(obj.get(key).toString(), valueType);
                result.put(enKey, enValue);
            }
            return result;
        } catch (Exception e) {
            logger.error("Error parsing Map from json string ! bean:<{},{}>, exception:{}", keyType, valueType, e);
        }
        return null;
    }

    public static <K> Set<K> parseSet(String jsonStr, Class<K> keyType) {
        try {
            Set<K> result = new HashSet<>();
            JSONArray obj = JSONArray.parseArray(jsonStr);
            for (Object key : obj) {
                K enKey = JSONObject.parseObject(key.toString(), keyType);
                result.add(enKey);
            }
            return result;
        } catch (Exception e) {
            logger.error("Error parsing Set from json string ! bean:<{}>, exception:{}", keyType, e);
        }
        return null;
    }
}
