package util;

import com.fasterxml.jackson.databind.PropertyNamingStrategy.PropertyNamingStrategyBase;
import com.fasterxml.jackson.databind.module.SimpleModule;

import java.sql.Timestamp;
import java.util.Date;

public class LZJsonModule extends SimpleModule {
	private static final long serialVersionUID = 4093223126017721944L;
	public static LZJsonModule LZ_MODULE = new LZJsonModule();
	private LZJsonModule(){
		addDeserializer(Timestamp.class, LZDateSerializers.timestampDeserializer);
		addDeserializer(Date.class, LZDateSerializers.dateDeserializer);
		addDeserializer(java.sql.Date.class, LZDateSerializers.sqlDateDeserializer);
		addSerializer(Timestamp.class, new LZDateSerializer());
		addSerializer(Date.class, new LZDateSerializer());
		addSerializer(java.sql.Date.class, new LZDateSerializer());
	}
	public static class UpperCasePropertyNamingStrategy extends PropertyNamingStrategyBase {
		
		private static final long serialVersionUID = -6510034237130928673L;
		@Override
		/**
		 * this method call back by PropertyNamingStrategy
		 */
		public String translate(String propertyName) {
			if ("objectName".equals(propertyName)) {
				return propertyName;
			}
			String name = propertyName.replaceAll("^\\w", propertyName.toUpperCase().substring(0, 1));
			
			return name;
		}

	}
}
