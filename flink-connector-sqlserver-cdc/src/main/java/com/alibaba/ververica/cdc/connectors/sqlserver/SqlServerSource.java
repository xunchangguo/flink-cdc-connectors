/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.ververica.cdc.connectors.sqlserver;

import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import io.debezium.connector.sqlserver.SqlServerConnector;

import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A builder to build a SourceFunction which can read snapshot and continue to consume binlog.
 */
public class SqlServerSource {

	public static <T> Builder<T> builder() {
		return new Builder<>();
	}

	/**
	 * Builder class of {@link SqlServerSource}.
	 */
	public static class Builder<T> {

		private int port = 1433; // default 1433 port
		private String hostname;
		private String database;
		private String username;
		private String password;
		private String serverTimeZone;
		private String[] tableList;
		private Properties dbzProperties;
		private DebeziumDeserializationSchema<T> deserializer;

		public Builder<T> hostname(String hostname) {
			this.hostname = hostname;
			return this;
		}

		/**
		 * Integer port number of the MySQL database server.
		 */
		public Builder<T> port(int port) {
			this.port = port;
			return this;
		}

		public Builder<T> database(String database) {
			this.database = database;
			return this;
		}

		/**
		 * An optional list of regular expressions that match fully-qualified table identifiers
		 * for tables to be monitored; any table not included in the list will be excluded from
		 * monitoring. Each identifier is of the form databaseName.tableName.
		 * By default the connector will monitor every non-system table in each monitored database.
		 */
		public Builder<T> tableList(String... tableList) {
			this.tableList = tableList;
			return this;
		}

		/**
		 * Name of the MySQL database to use when connecting to the MySQL database server.
		 */
		public Builder<T> username(String username) {
			this.username = username;
			return this;
		}

		/**
		 * Password to use when connecting to the MySQL database server.
		 */
		public Builder<T> password(String password) {
			this.password = password;
			return this;
		}

		/**
		 * The session time zone in database server, e.g. "America/Los_Angeles".
		 * It controls how the TIMESTAMP type in MYSQL converted to STRING.
		 * See more https://debezium.io/documentation/reference/1.2/connectors/mysql.html#_temporal_values
		 */
		public Builder<T> serverTimeZone(String timeZone) {
			this.serverTimeZone = timeZone;
			return this;
		}

		/**
		 * The Debezium MySQL connector properties. For example, "snapshot.mode".
		 */
		public Builder<T> debeziumProperties(Properties properties) {
			this.dbzProperties = properties;
			return this;
		}

		/**
		 * The deserializer used to convert from consumed {@link org.apache.kafka.connect.source.SourceRecord}.
		 */
		public Builder<T> deserializer(DebeziumDeserializationSchema<T> deserializer) {
			this.deserializer = deserializer;
			return this;
		}

		public DebeziumSourceFunction<T> build() {
			Properties props = new Properties();
			props.setProperty("connector.class", SqlServerConnector.class.getCanonicalName());
			/**
			 * Logical name that identifies and provides a namespace for the particular SQL Server database server being monitored.
			 * The logical name should be unique across all other connectors, since it is used as a prefix for all Kafka topic
			 * names emanating from this connector. Only alphanumeric characters and underscores should be used.
			 */
			props.setProperty("database.server.name", "sqlserver_cdc_source");
			props.setProperty("database.hostname", checkNotNull(hostname));
			props.setProperty("database.user", checkNotNull(username));
			props.setProperty("database.password", checkNotNull(password));
			props.setProperty("database.port", String.valueOf(port));
			props.setProperty("database.dbname", checkNotNull(database));

			if (tableList != null) {
				props.setProperty("table.include.list", String.join(",", tableList));
			}
			if (serverTimeZone != null) {
				props.setProperty("database.server.timezone", serverTimeZone);
			}

			if (dbzProperties != null) {
				dbzProperties.forEach(props::put);
			}

			return new DebeziumSourceFunction<>(
				deserializer,
				props);
		}
	}
}
