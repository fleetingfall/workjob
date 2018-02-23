package util;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.DateDeserializers.DateDeserializer;
import com.fasterxml.jackson.databind.deser.std.DateDeserializers.SqlDateDeserializer;
import com.fasterxml.jackson.databind.deser.std.DateDeserializers.TimestampDeserializer;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

class LZDateSerializers {
    static final LZDateDeserializer dateDeserializer = new LZDateDeserializer();
    static final LZTimestampDeserializer timestampDeserializer = new LZTimestampDeserializer();
    static final LZSqlDateDeserializer sqlDateDeserializer = new LZSqlDateDeserializer();
    
    private static class LZDateDeserializer extends DateDeserializer {
        private static final long serialVersionUID = -8049402816880830231L;
        
        @Override
        protected java.util.Date _parseDate(JsonParser jp, DeserializationContext ctxt)
                throws IOException, JsonProcessingException {
            JsonToken t = jp.getCurrentToken();
            try {
                if (t == JsonToken.VALUE_NUMBER_INT) {
                    return new java.util.Date(jp.getLongValue());
                }
                if (t == JsonToken.VALUE_STRING) {
                    /*
					 * As per [JACKSON-203], take empty Strings to mean null
					 */
                    String str = jp.getText().trim();
                    if (str.length() == 0) {
                        return null;
                    }
                    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                    return formatter.parse(str);
                }
                throw ctxt.mappingException(_valueClass);
            } catch (ParseException e) {
                throw ctxt.mappingException(_valueClass);
            } catch (IllegalArgumentException iae) {
                throw ctxt.weirdStringException(null, _valueClass,
                        "not a valid representation (error: " + iae.getMessage()
                                + ")");
            }
        }
    }
    
    private static class LZTimestampDeserializer extends TimestampDeserializer {
        private static final long serialVersionUID = -8049402816880830231L;
        
        @Override
        protected java.util.Date _parseDate(JsonParser jp, DeserializationContext ctxt)
                throws IOException, JsonProcessingException {
            JsonToken t = jp.getCurrentToken();
            try {
                if (t == JsonToken.VALUE_NUMBER_INT) {
                    return new java.util.Date(jp.getLongValue());
                }
                if (t == JsonToken.VALUE_STRING) {
					/*
					 * As per [JACKSON-203], take empty Strings to mean null
					 */
                    String str = jp.getText().trim();
                    if (str.length() == 0) {
                        return null;
                    }
                    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    return formatter.parse(str);
                }
                throw ctxt.mappingException(_valueClass);
            } catch (ParseException e) {
                throw ctxt.mappingException(_valueClass);
            } catch (IllegalArgumentException iae) {
                throw ctxt.weirdStringException(null, _valueClass,
                        "not a valid representation (error: " + iae.getMessage()
                                + ")");
            }
        }
    }
    
    private static class LZSqlDateDeserializer extends SqlDateDeserializer {
        private static final long serialVersionUID = -8049402816880830231L;
        
        @Override
        protected java.sql.Date _parseDate(JsonParser jp, DeserializationContext ctxt)
                throws IOException, JsonProcessingException {
            JsonToken t = jp.getCurrentToken();
            try {
                if (t == JsonToken.VALUE_NUMBER_INT) {
                    return new java.sql.Date(jp.getLongValue());
                }
                if (t == JsonToken.VALUE_STRING) {
					/*
					 * As per [JACKSON-203], take empty Strings to mean null
					 */
                    String str = jp.getText().trim();
                    if (str.length() == 0) {
                        return null;
                    }
                    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                    return new java.sql.Date(formatter.parse(str).getTime());
                }
                throw ctxt.mappingException(_valueClass);
            } catch (ParseException e) {
                throw ctxt.mappingException(_valueClass);
            } catch (IllegalArgumentException iae) {
                throw ctxt.weirdStringException(null, _valueClass,
                        "not a valid representation (error: " + iae.getMessage()
                                + ")");
            }
        }
    }
}
