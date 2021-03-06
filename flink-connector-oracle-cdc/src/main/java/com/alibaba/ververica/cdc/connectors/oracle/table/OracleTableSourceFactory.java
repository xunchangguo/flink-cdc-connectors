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

package com.alibaba.ververica.cdc.connectors.oracle.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;

import com.alibaba.ververica.cdc.debezium.table.DebeziumOptions;

import java.time.ZoneId;
import java.util.HashSet;
import java.util.Set;

import static com.alibaba.ververica.cdc.debezium.table.DebeziumOptions.getDebeziumProperties;

/**
 * Factory for creating configured instance of {@link OracleTableSource}.
 */
public class OracleTableSourceFactory implements DynamicTableSourceFactory {

	private static final String IDENTIFIER = "oracle-cdc";

	private static final ConfigOption<String> HOSTNAME = ConfigOptions.key("hostname")
		.stringType()
		.noDefaultValue()
		.withDescription("IP address or hostname of the Oracle database server.");

	private static final ConfigOption<Integer> PORT = ConfigOptions.key("port")
		.intType()
		.defaultValue(1521)
		.withDescription("Integer port number of the Oracle database server.");

	private static final ConfigOption<String> USERNAME = ConfigOptions.key("username")
		.stringType()
		.noDefaultValue()
		.withDescription("Name of the Oracle database to use when connecting to the Oracle database server.");

	private static final ConfigOption<String> PASSWORD = ConfigOptions.key("password")
		.stringType()
		.noDefaultValue()
		.withDescription("Password to use when connecting to the Oracle database server.");

	private static final ConfigOption<String> DATABASE_NAME = ConfigOptions.key("database-name")
		.stringType()
		.noDefaultValue()
		.withDescription("Database name of the Oracle server to monitor.");

	private static final ConfigOption<String> TABLE_NAME = ConfigOptions.key("table-name")
			.stringType()
			.noDefaultValue()
			.withDescription("Table name of the Sqlserver database to monitor.");

	private static final ConfigOption<String> PDB_NAME = ConfigOptions.key("pdb-name")
		.stringType()
		.noDefaultValue()
		.withDescription("Name of the PDB to connect to, when working with the CDB + PDB model.");

	private static final ConfigOption<String> OUT_SERVER_NAME = ConfigOptions.key("out-server-name")
			.stringType()
			.noDefaultValue()
			.withDescription("Name of the XStream outbound server configured in the database.");

	private static final ConfigOption<String> CONNECTION_ADAPTER = ConfigOptions.key("connection-adapter")
			.stringType()
			.noDefaultValue()
			.withDescription("The adapter implementation to use. xstream uses the Oracle XStreams API. logminer uses the native Oracle LogMiner API.");

	private static final ConfigOption<String> SERVER_TIME_ZONE = ConfigOptions.key("server-time-zone")
		.stringType()
		.defaultValue("UTC")
		.withDescription("The session time zone in database server.");

	@Override
	public DynamicTableSource createDynamicTableSource(Context context) {
		final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
		helper.validateExcept(DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX);

		final ReadableConfig config = helper.getOptions();
		String hostname = config.get(HOSTNAME);
		String username = config.get(USERNAME);
		String password = config.get(PASSWORD);
		String databaseName = config.get(DATABASE_NAME);
		String tableName = config.get(TABLE_NAME);
		String pdbName = config.get(PDB_NAME);
		String outServerName = config.get(OUT_SERVER_NAME);
		String connectionAdapter = config.getOptional(CONNECTION_ADAPTER).orElse(null);
		int port = config.get(PORT);
		ZoneId serverTimeZone = ZoneId.of(config.get(SERVER_TIME_ZONE));
		TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

		return new OracleTableSource(
			physicalSchema,
			port,
			hostname,
			databaseName,
			tableName,
			username,
			password,
			pdbName,
			outServerName,
			connectionAdapter,
			serverTimeZone,
			getDebeziumProperties(context.getCatalogTable().getOptions())
		);
	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		Set<ConfigOption<?>> options = new HashSet<>();
		options.add(HOSTNAME);
		options.add(USERNAME);
		options.add(PASSWORD);
		options.add(DATABASE_NAME);
		options.add(TABLE_NAME);
		options.add(OUT_SERVER_NAME);
		return options;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		Set<ConfigOption<?>> options = new HashSet<>();
		options.add(PORT);
		options.add(SERVER_TIME_ZONE);
		options.add(CONNECTION_ADAPTER);
		options.add(PDB_NAME);
		return options;
	}
}
