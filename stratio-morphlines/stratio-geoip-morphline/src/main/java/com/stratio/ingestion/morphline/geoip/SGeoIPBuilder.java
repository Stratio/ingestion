package com.stratio.ingestion.morphline.geoip;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Collection;
import java.util.Collections;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;
import org.kitesdk.morphline.base.Notifications;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.net.InetAddresses;
import com.maxmind.db.Reader;
import com.typesafe.config.Config;

public class SGeoIPBuilder implements CommandBuilder {

	private final static String COMMAND_NAME = "sgeoIP";

	@Override
	public Collection<String> getNames() {
		return Collections.singletonList(COMMAND_NAME);
	}

	@Override
	public Command build(Config config, Command parent, Command child,
			MorphlineContext context) {
		return new SGeoIP(this, config, parent, child, context);
	}

	private static final class SGeoIP extends AbstractCommand {

		private final static String INPUT_FIELD = "input";
		private final static String LONGITUDE_LATITUDE_OUTPUT_FIELD = "longitudeLatituteOutput";
		private final static String ISO_CODE_OUTPUT_FIELD = "isoCodeOutput";
		private final static String DATABASE = "database";

		private final String inputFieldName;
		private final File databaseFile;
		private final Reader databaseReader;
		private String longitudeLatituteFieldName;
		private String isoCodeFieldName;

		public SGeoIP(CommandBuilder builder, Config config, Command parent, Command child,
				final MorphlineContext context) {

			super(builder, config, parent, child, context);
			this.inputFieldName = getConfigs().getString(config, INPUT_FIELD);
			this.databaseFile = new File(getConfigs().getString(config,
					DATABASE, "GeoLite2-City.mmdb"));
			this.longitudeLatituteFieldName = getConfigs().getString(config,
					LONGITUDE_LATITUDE_OUTPUT_FIELD);
			this.isoCodeFieldName = getConfigs().getString(config,
					ISO_CODE_OUTPUT_FIELD);
			try {
				this.databaseReader = new Reader(databaseFile);
			} catch (IOException e) {
				throw new MorphlineCompilationException(
						"Cannot read Maxmind database: " + databaseFile,
						config, e);
			}
			validateArguments();
		}

		@Override
		protected boolean doProcess(Record record) {
			Object value = record.get(inputFieldName).get(0);
			InetAddress addr;
			if (value instanceof InetAddress) {
				addr = (InetAddress) value;
			} else {
				try {
					addr = InetAddresses.forString(value.toString());
				} catch (IllegalArgumentException e) {
					LOG.debug("Invalid IP string literal: {}", value);
					return super.doProcess(record);
				}
			}

			JsonNode json;
			try {
				json = databaseReader.get(addr);
				if (json == null)
					return super.doProcess(record);
			} catch (IOException e) {
				return super.doProcess(record);
			}

			if (longitudeLatituteFieldName != null) {
				ObjectNode location = (ObjectNode) json.get("location");
				if (location != null) {
					JsonNode jlatitude = location.get("latitude");
					JsonNode jlongitude = location.get("longitude");
					if (jlatitude != null && jlongitude != null) {
						String latitude = jlatitude.toString();
						String longitude = jlongitude.toString();
						location.put("latitude_longitude", latitude + ","
								+ longitude);
						location.put("longitude_latitude", longitude + ","
								+ latitude);
						record.put(longitudeLatituteFieldName, longitude + ","
								+ latitude);
					}
				}
			}

			if (isoCodeFieldName != null) {
				ObjectNode country = (ObjectNode) json.get("country");
				if (country != null) {
					JsonNode jISOCode = country.get("iso_code");
					if (jISOCode != null) {
						String isoCode = jISOCode.toString().replaceAll("\"",
								"");
						record.put(isoCodeFieldName, isoCode);
					}
				}
			}

			// pass record to next command in chain:
			return super.doProcess(record);
		}

		@Override
		protected void doNotify(Record notification) {
			for (Object event : Notifications.getLifecycleEvents(notification)) {
				if (event == Notifications.LifecycleEvent.SHUTDOWN) {
					try {
						databaseReader.close();
					} catch (IOException e) {
						LOG.warn("Cannot close Maxmind database: "
								+ databaseFile, e);
					}
				}
			}
			super.doNotify(notification);
		}
	}

}
