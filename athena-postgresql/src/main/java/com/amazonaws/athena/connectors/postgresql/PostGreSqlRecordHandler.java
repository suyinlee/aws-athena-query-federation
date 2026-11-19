/*-
 * #%L
 * athena-postgresql
 * %%
 * Copyright (C) 2019 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.postgresql;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import com.amazonaws.athena.connectors.jdbc.connection.GenericJdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcRecordHandler;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.google.common.annotations.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import static com.amazonaws.athena.connectors.postgresql.PostGreSqlConstants.POSTGRESQL_DEFAULT_PORT;
import static com.amazonaws.athena.connectors.postgresql.PostGreSqlConstants.POSTGRESQL_DRIVER_CLASS;
import static com.amazonaws.athena.connectors.postgresql.PostGreSqlConstants.POSTGRES_NAME;
import static com.amazonaws.athena.connectors.postgresql.PostGreSqlConstants.POSTGRES_QUOTE_CHARACTER;

public class PostGreSqlRecordHandler
        extends JdbcRecordHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PostGreSqlRecordHandler.class);

    private static final int FETCH_SIZE = 1000;

    // Unconstrained PostgreSQL NUMERIC (and computed results: SUM/AVG/ROUND/expressions) is reported by the
    // JDBC driver as precision 0 / scale 0, which otherwise resolves to a scale-0 decimal and silently drops
    // every fractional digit. Resolve those to DECIMAL(38, DEFAULT_NUMERIC_SCALE). An explicit per-connection
    // "default_scale" still overrides DEFAULT_NUMERIC_SCALE.
    private static final int DEFAULT_NUMERIC_PRECISION = 38;
    private static final int DEFAULT_NUMERIC_SCALE = 18;

    private final JdbcSplitQueryBuilder jdbcSplitQueryBuilder;

    /**
     * Instantiates handler to be used by Lambda function directly.
     *
     * Recommend using {@link PostGreSqlMuxCompositeHandler} instead.
     */
    public PostGreSqlRecordHandler(java.util.Map<String, String> configOptions)
    {
        this(JDBCUtil.getSingleDatabaseConfigFromEnv(POSTGRES_NAME, configOptions), configOptions);
    }

    public PostGreSqlRecordHandler(DatabaseConnectionConfig databaseConnectionConfig, java.util.Map<String, String> configOptions)
    {
        this(databaseConnectionConfig, S3Client.create(), SecretsManagerClient.create(), AthenaClient.create(),
                new GenericJdbcConnectionFactory(databaseConnectionConfig, PostGreSqlMetadataHandler.JDBC_PROPERTIES, new DatabaseConnectionInfo(POSTGRESQL_DRIVER_CLASS, POSTGRESQL_DEFAULT_PORT), configOptions),
                new PostGreSqlQueryStringBuilder(POSTGRES_QUOTE_CHARACTER, new PostgreSqlFederationExpressionParser(POSTGRES_QUOTE_CHARACTER)), configOptions);
    }

    @VisibleForTesting
    protected PostGreSqlRecordHandler(DatabaseConnectionConfig databaseConnectionConfig, S3Client amazonS3, SecretsManagerClient secretsManager,
            AthenaClient athena, JdbcConnectionFactory jdbcConnectionFactory, JdbcSplitQueryBuilder jdbcSplitQueryBuilder, java.util.Map<String, String> configOptions)
    {
        super(amazonS3, secretsManager, athena, databaseConnectionConfig, jdbcConnectionFactory, configOptions);
        this.jdbcSplitQueryBuilder = Validate.notNull(jdbcSplitQueryBuilder, "query builder must not be null");
    }

    @Override
    public PreparedStatement buildSplitSql(Connection jdbcConnection, String catalogName, TableName tableName, Schema schema, Constraints constraints, Split split)
    {
        PreparedStatement preparedStatement;
        try {
            if (constraints.isQueryPassThrough()) {
                preparedStatement = buildQueryPassthroughSql(jdbcConnection, constraints);
            }
            else {
                preparedStatement = jdbcSplitQueryBuilder.buildSql(jdbcConnection, null, tableName.getSchemaName(), tableName.getTableName(), schema, constraints, split);
            }
            // Disable fetching all rows.
            preparedStatement.setFetchSize(FETCH_SIZE);
        }
        catch (SQLException e) {
            throw new AthenaConnectorException(e.getMessage(), ErrorDetails.builder().errorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString()).build());
        }

        return preparedStatement;
    }

    /**
     * Correct the export schema for unconstrained PostgreSQL {@code NUMERIC} results.
     *
     * <p>PostgreSQL reports an unconstrained {@code NUMERIC} column, and any computed numeric result
     * ({@code SUM}, {@code AVG}, {@code ROUND}, arithmetic expressions), with precision 0 and scale 0 over
     * JDBC. The base derivation turns that into a scale-0 decimal, so every fractional digit is dropped and
     * the loss is then frozen in by the decimal writer's {@code setScale(...)}. For exactly that case we
     * resolve the column to {@code DECIMAL(38, default_scale)} so fractional digits survive; constrained
     * {@code NUMERIC(p,s)} (which reports a real precision) and all other types are left untouched.
     */
    @Override
    protected Schema deriveExportSchema(ResultSet resultSet) throws SQLException
    {
        Schema baseSchema = super.deriveExportSchema(resultSet);
        ResultSetMetaData metadata = resultSet.getMetaData();
        int defaultScale = resolveDefaultScale();
        List<Field> fields = new ArrayList<>(baseSchema.getFields().size());

        for (int i = 0; i < baseSchema.getFields().size(); i++) {
            Field baseField = baseSchema.getFields().get(i);
            int column = i + 1; // ResultSetMetaData columns are 1-indexed
            if (isUnconstrainedNumeric(metadata, column)) {
                ArrowType.Decimal resolved = new ArrowType.Decimal(DEFAULT_NUMERIC_PRECISION, defaultScale, 128);
                LOGGER.debug("deriveExportSchema: column {} is unconstrained NUMERIC; resolving to DECIMAL({},{})",
                        baseField.getName(), DEFAULT_NUMERIC_PRECISION, defaultScale);
                fields.add(new Field(baseField.getName(),
                        new FieldType(baseField.isNullable(), resolved, null), baseField.getChildren()));
            }
            else {
                fields.add(baseField);
            }
        }
        return new Schema(fields, baseSchema.getCustomMetadata());
    }

    private static boolean isUnconstrainedNumeric(ResultSetMetaData metadata, int column) throws SQLException
    {
        return metadata.getColumnType(column) == Types.NUMERIC
                && metadata.getPrecision(column) == 0
                && metadata.getScale(column) <= 0;
    }

    private int resolveDefaultScale()
    {
        int scale = DEFAULT_NUMERIC_SCALE;
        String configured = configOptions == null ? null : configOptions.get("default_scale");
        if (configured != null && !configured.isBlank()) {
            try {
                int parsed = Integer.parseInt(configured.trim());
                if (parsed >= 0 && parsed <= DEFAULT_NUMERIC_PRECISION) {
                    scale = parsed;
                }
                else {
                    LOGGER.warn("default_scale {} out of range [0,{}]; using {}", parsed, DEFAULT_NUMERIC_PRECISION, scale);
                }
            }
            catch (NumberFormatException e) {
                LOGGER.warn("Invalid default_scale '{}'; using {}", configured, scale);
            }
        }
        return scale;
    }
}
