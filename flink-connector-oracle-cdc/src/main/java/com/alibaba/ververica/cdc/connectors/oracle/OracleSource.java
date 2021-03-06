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

package com.alibaba.ververica.cdc.connectors.oracle;

import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import io.debezium.connector.oracle.OracleConnector;

import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A builder to build a SourceFunction which can read snapshot and continue to consume binlog.
 */
public class OracleSource {

	public static <T> Builder<T> builder() {
		return new Builder<>();
	}

	/**
	 * Builder class of {@link OracleSource}.
	 */
	public static class Builder<T> {

		private int port = 1521; // default 1521 port
		private String hostname;
		private String database;
		private String username;
		private String password;
		/** Name of the PDB to connect to, when working with the CDB + PDB model. */
		private String pdbName;
		/** Name of the XStream outbound server configured in the database. */
		private String outServerName;
		/** xstream/logminer. The adapter implementation to use. xstream uses the Oracle XStreams API. logminer uses the native Oracle LogMiner API. */
		private String connectionAdapter;
		private String serverTimeZone;
		private String[] tableList;
		private Properties dbzProperties;
		private DebeziumDeserializationSchema<T> deserializer;

		public Builder<T> hostname(String hostname) {
			this.hostname = hostname;
			return this;
		}

		/**
		 * Integer port number of the Oracle database server.
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
		 * Name of the Oracle database to use when connecting to the Oracle database server.
		 */
		public Builder<T> username(String username) {
			this.username = username;
			return this;
		}

		/**
		 * Password to use when connecting to the Oracle database server.
		 */
		public Builder<T> password(String password) {
			this.password = password;
			return this;
		}

		public Builder<T> pdbName(String pdbName) {
			this.pdbName = pdbName;
			return this;
		}

		public Builder<T> outServerName(String outServerName) {
			this.outServerName = outServerName;
			return this;
		}

		public Builder<T> connectionAdapter(String connectionAdapter) {
			this.connectionAdapter = connectionAdapter;
			return this;
		}

		/**
		 * The session time zone in database server, e.g. "America/Los_Angeles".
		 * It controls how the TIMESTAMP type in Oracle converted to STRING.
		 */
		public Builder<T> serverTimeZone(String timeZone) {
			this.serverTimeZone = timeZone;
			return this;
		}

		/**
		 * The Debezium Oracle connector properties. For example, "snapshot.mode".
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
			props.setProperty("connector.class", OracleConnector.class.getCanonicalName());
			props.setProperty("database.server.name", "oracle_binlog_source");
			props.setProperty("database.hostname", checkNotNull(hostname));
			props.setProperty("database.user", checkNotNull(username));
			props.setProperty("database.password", checkNotNull(password));
			props.setProperty("database.port", String.valueOf(port));
			props.setProperty("database.dbname", String.valueOf(database));
			props.setProperty("database.out.server.name", String.valueOf(outServerName));

			if (pdbName != null) {
				props.setProperty("database.pdb.name", pdbName);
			}

			if (tableList != null) {
				props.setProperty("table.include.list", String.join(",", tableList));
			}

			if (connectionAdapter != null) {
				props.setProperty("database.connection.adapter", connectionAdapter);
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
