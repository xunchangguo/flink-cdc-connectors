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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import com.alibaba.ververica.cdc.connectors.oracle.OracleSource;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema;

import java.time.ZoneId;
import java.util.Objects;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link DynamicTableSource} that describes how to create a Oracle source from a logical
 * description.
 */
public class OracleTableSource implements ScanTableSource {

	private final TableSchema physicalSchema;
	private final int port;
	private final String hostname;
	private final String database;
	private final String username;
	private final String password;
	private final String tableName;
	/** Name of the PDB to connect to, when working with the CDB + PDB model. */
	private String pdbName;
	/** Name of the XStream outbound server configured in the database. */
	private String outServerName;
	/** xstream/logminer. The adapter implementation to use. xstream uses the Oracle XStreams API. logminer uses the native Oracle LogMiner API. */
	private String connectionAdapter;
	private final ZoneId serverTimeZone;
	private final Properties dbzProperties;

	public OracleTableSource(
			TableSchema physicalSchema,
			int port,
			String hostname,
			String database,
			String tableName,
			String username,
			String password,
			String pdbName,
			String outServerName,
			String connectionAdapter,
			ZoneId serverTimeZone,
			Properties dbzProperties) {
		this.physicalSchema = physicalSchema;
		this.port = port;
		this.hostname = checkNotNull(hostname);
		this.database = checkNotNull(database);
		this.tableName = checkNotNull(tableName);
		this.username = checkNotNull(username);
		this.password = checkNotNull(password);
		this.pdbName = pdbName;
		this.outServerName = checkNotNull(outServerName);
		this.connectionAdapter = connectionAdapter;
		this.serverTimeZone = serverTimeZone;
		this.dbzProperties = dbzProperties;
	}

	@Override
	public ChangelogMode getChangelogMode() {
		return ChangelogMode.newBuilder()
			.addContainedKind(RowKind.INSERT)
			.addContainedKind(RowKind.UPDATE_BEFORE)
			.addContainedKind(RowKind.UPDATE_AFTER)
			.addContainedKind(RowKind.DELETE)
			.build();
	}

	@SuppressWarnings("unchecked")
	@Override
	public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
		RowType rowType = (RowType) physicalSchema.toRowDataType().getLogicalType();
		TypeInformation<RowData> typeInfo = (TypeInformation<RowData>) scanContext.createTypeInformation(physicalSchema.toRowDataType());
		DebeziumDeserializationSchema<RowData> deserializer = new RowDataDebeziumDeserializeSchema(
			rowType,
			typeInfo,
			((rowData, rowKind) -> {}),
			serverTimeZone);
		OracleSource.Builder<RowData> builder = OracleSource.<RowData>builder()
			.hostname(hostname)
			.port(port)
			.database(database)
			.tableList(tableName)
			.username(username)
			.password(password)
			.pdbName(pdbName)
			.outServerName(outServerName)
			.connectionAdapter(connectionAdapter)
			.serverTimeZone(serverTimeZone.toString())
			.debeziumProperties(dbzProperties)
			.deserializer(deserializer);
		DebeziumSourceFunction<RowData> sourceFunction = builder.build();

		return SourceFunctionProvider.of(sourceFunction, false);
	}

	@Override
	public DynamicTableSource copy() {
		return new OracleTableSource(
			physicalSchema,
			port,
			hostname,
			database,
			tableName,
			username,
			password,
			pdbName,
			outServerName,
			connectionAdapter,
			serverTimeZone,
			dbzProperties
		);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		OracleTableSource that = (OracleTableSource) o;
		return port == that.port &&
			Objects.equals(physicalSchema, that.physicalSchema) &&
			Objects.equals(hostname, that.hostname) &&
			Objects.equals(database, that.database) &&
			Objects.equals(tableName, that.tableName) &&
			Objects.equals(username, that.username) &&
			Objects.equals(password, that.password) &&
			Objects.equals(pdbName, that.pdbName) &&
			Objects.equals(outServerName, that.outServerName) &&
			Objects.equals(connectionAdapter, that.connectionAdapter) &&
			Objects.equals(serverTimeZone, that.serverTimeZone) &&
			Objects.equals(dbzProperties, that.dbzProperties);
	}

	@Override
	public int hashCode() {
		return Objects.hash(physicalSchema, port, hostname, database, tableName, username, password, pdbName, outServerName, connectionAdapter, serverTimeZone, dbzProperties);
	}

	@Override
	public String asSummaryString() {
		return "Oracle-CDC";
	}
}
