/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.jdbc.JdbcDatasetUtils;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.sql.ColumnLineage;
import io.openlineage.sql.ColumnMeta;
import io.openlineage.sql.DbTableMeta;
import io.openlineage.sql.ExtractionError;
import io.openlineage.sql.OpenLineageSql;
import io.openlineage.sql.SqlMeta;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions$;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

@Slf4j
public class JdbcSparkUtils {

  public static <D extends OpenLineage.Dataset> List<D> getDatasets(
      DatasetFactory<D> datasetFactory, SqlMeta meta, JDBCRelation relation) {

    StructType schema = relation.schema();
    String jdbcUrl = relation.jdbcOptions().url();
    Properties jdbcProperties = relation.jdbcOptions().asConnectionProperties();

    if (meta.columnLineage().isEmpty()) {
      int numberOfTables = meta.inTables().size();

      return meta.inTables().stream()
          .map(
              dbtm -> {
                if (dbtm.query() != null) {
                  String uri = jdbcUrl.replaceAll("^(?i)jdbc:", "");
                  DatasetIdentifier identifier = new DatasetIdentifier("", uri);
                  return datasetFactory.getDataset(
                      identifier,
                      dbtm.query(),
                      null,
                      dbtm.defaultSchema(),
                      datasetFactory.createCompositeFacetBuilder()
                  );
                }
                DatasetIdentifier di =
                    JdbcDatasetUtils.getDatasetIdentifier(
                        jdbcUrl, dbtm.qualifiedName(), jdbcProperties);

                if (numberOfTables > 1) {
                  return datasetFactory.getDataset(di.getName(), di.getNamespace());
                }

                return datasetFactory.getDataset(di.getName(), di.getNamespace(), schema);
              })
          .collect(Collectors.toList());
    }
    return meta.inTables().stream()
        .map(
            dbtm -> {
              DatasetIdentifier di =
                  JdbcDatasetUtils.getDatasetIdentifier(
                      jdbcUrl, dbtm.qualifiedName(), jdbcProperties);
              return datasetFactory.getDataset(
                  di.getName(), di.getNamespace(), generateSchemaFromSqlMeta(dbtm, schema, meta));
            })
        .collect(Collectors.toList());
  }

  public static StructType generateSchemaFromSqlMeta(
      DbTableMeta origin, StructType schema, SqlMeta sqlMeta) {
    StructType originSchema = new StructType();
    for (StructField f : schema.fields()) {
      List<ColumnMeta> fields =
          sqlMeta.columnLineage().stream()
              .filter(cl -> cl.descendant().name().equals(f.name()))
              .flatMap(
                  cl ->
                      cl.lineage().stream()
                          .filter(
                              cm -> cm.origin().isPresent() && cm.origin().get().equals(origin)))
              .collect(Collectors.toList());
      for (ColumnMeta cm : fields) {
        originSchema = originSchema.add(cm.name(), f.dataType());
      }
    }
    return originSchema;
  }

  private static Optional<String> getParameter(String name, JDBCRelation relation) {
    return ScalaConversionUtils.asJavaOptional(
        relation.jdbcOptions().parameters().get(name));
  }

  public static Optional<SqlMeta> extractQueryFromSpark(JDBCRelation relation) {
    Optional<String> table = JdbcSparkUtils.getParameter(JDBCOptions$.MODULE$.JDBC_TABLE_NAME(), relation);

    boolean tableIsQuery = table.isPresent() && table.get().startsWith("(");
    Optional<String> query = tableIsQuery ? table : JdbcSparkUtils.getParameter("query", relation);

    // in some cases table value can be "(SELECT col1, col2 FROM table_name WHERE some='filter')
    // ALIAS"
    if (!tableIsQuery) {
      DbTableMeta origin = new DbTableMeta(null, null, table.get());
      return Optional.of(
          new SqlMeta(
              Collections.singletonList(origin),
              Collections.emptyList(),
              Arrays.stream(relation.schema().fields())
                  .map(
                      field ->
                          new ColumnLineage(
                              new ColumnMeta(null, field.name()),
                              Collections.singletonList(new ColumnMeta(origin, field.name()))))
                  .collect(Collectors.toList()),
              Collections.emptyList()));
    }

    if (query.isPresent()) {
      String dialect = extractDialectFromJdbcUrl(relation.jdbcOptions().url());
      Optional<String> defaultSchema = Objects.equals(dialect, "oracle") ? getParameter("user", relation) : Optional.empty();
      DbTableMeta origin = new DbTableMeta(query.get(), defaultSchema.orElse(null));
      return Optional.of(
          new SqlMeta(
              Collections.singletonList(origin),
              Collections.emptyList(),
              Collections.emptyList(),
              Collections.emptyList()));
    }

    log.debug("No query or table found in JDBCRelation: {}", relation);
    return Optional.empty();
  }

  public static String queryStringFromJdbcOptions(JDBCOptions options) {
    String tableOrQuery = options.tableOrQuery();
    return tableOrQuery.substring(0, tableOrQuery.lastIndexOf(")")).replaceFirst("\\(", "");
  }

  private static String extractDialectFromJdbcUrl(String jdbcUrl) {
    Pattern pattern = Pattern.compile("^jdbc:([^:]+):.*");
    Matcher matcher = pattern.matcher(jdbcUrl);

    if (matcher.find()) {
      return matcher.group(1);
    } else {
      return null;
    }
  }
}
