package com.github.knightliao.apollo.utils.data;

import java.io.IOException;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.github.knightliao.apollo.utils.time.DateUtils;

/**
 * JSON工具类
 *
 * @author liaoqiqi
 * @version 2014-8-20
 */
public final class JsonUtils {

    final static ObjectMapper objectMapper;
    private static final Logger log = LoggerFactory.getLogger(JsonUtils.class);

    static {
        objectMapper = new ObjectMapper();
    }

    private JsonUtils() {

    }

    public static ObjectMapper getObjectMapper() {
        return objectMapper;
    }

    /**
     * JSON串转换为Java泛型对象，可以是各种类型，此方法最为强大。用法看测试用例。
     *
     * @param <T>
     * @param jsonString JSON字符串
     * @param tr         TypeReference,例如: new TypeReference< List<FamousUser> >(){}
     *
     * @return List对象列表
     */
    @SuppressWarnings("unchecked")
    public static <T> T json2GenericObject(String jsonString,
                                           TypeReference<T> tr) {

        if (jsonString == null || "".equals(jsonString)) {
            return null;
        } else {
            try {
                return (T) objectMapper.readValue(jsonString, tr);
            } catch (Exception e) {
                log.warn("json error:" + e.getMessage());
            }
        }
        return null;
    }

    /**
     * Java对象转Json字符串
     */
    public static String toJson(Object object) {
        String jsonString = "";
        try {
            jsonString = objectMapper.writeValueAsString(object);
        } catch (Exception e) {
            log.warn("json error:" + e.getMessage());
        }
        return jsonString;

    }

    /**
     * Json字符串转Java对象
     */
    public static Object json2Object(String jsonString, Class<?> c) {

        if (jsonString == null || "".equals(jsonString)) {
            return "";
        } else {
            try {
                return objectMapper.readValue(jsonString, c);
            } catch (Exception e) {
                log.warn("json error:" + e.getMessage());
            }

        }
        return "";
    }

    public static final class IosDateTimeJsonSerializer extends
            JsonSerializer<Date> {

        @Override
        public void serialize(Date value, JsonGenerator paramJsonGenerator,
                              SerializerProvider provider) throws IOException,
                JsonProcessingException {
            if (value != null) {
                paramJsonGenerator.writeString(DateUtils.ISO_DATETIME_FORMAT
                        .format(value));
            }
        }

    }

}
