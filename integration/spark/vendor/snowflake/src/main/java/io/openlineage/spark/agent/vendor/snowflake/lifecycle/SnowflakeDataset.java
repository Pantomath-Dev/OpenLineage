/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.snowflake.lifecycle;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.SqlUtils;
import io.openlineage.spark.api.DatasetFactory;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slf4j
public class SnowflakeDataset {
  public static final String SNOWFLAKE_PREFIX = "snowflake://";

  private static final Logger logger = LoggerFactory.getLogger(SnowflakeDataset.class);

  public static <D extends OpenLineage.Dataset> List<D> getDatasets(
      DatasetFactory<D> factory,
      String sfFullURL,
      String sfDatabase,
      String sfSchema,
      Optional<String> dbtable,
      Optional<String> query,
      StructType schema) {

    final String namespace =
        String.format("%s%s", SNOWFLAKE_PREFIX, sfFullURL.replace("https://", ""));
    final String tableName;
    // https://docs.snowflake.com/en/user-guide/spark-connector-use#moving-data-from-snowflake-to-spark
    // > Specify one of the following options for the table data to be read:
    // >    - `dbtable`: The name of the table to be read. All columns and records are retrieved
    // >      (i.e. it is equivalent to SELECT * FROM db_table).
    // >    - `query`: The exact query (SELECT statement) to run.
    // If dbtable is null it will be replaced with the string `complex` and it means the query
    // option was used.
    // An improvement could be put the query string in the `DatasetFacets`
    if (dbtable.isPresent()) {
      tableName = dbtable.get();
      String name = String.format("%s.%s.%s", sfDatabase, sfSchema, tableName);
      log.info("NATHAN: dtable is present, name={}", name);
      return Collections.singletonList(factory.getDataset(name, namespace, schema));
    } else if (query.isPresent()) {
      log.info("NATHAN: query is present, query={}", query.get());
      return SqlUtils.getDatasets(
          factory, query.get(), "snowflake", namespace, sfDatabase, sfSchema);
    } else {
      log.warn(
          "NATHAN: Unable to discover Snowflake table property - neither \"dbtable\" nor \"query\" option present");
    }
    return Collections.emptyList();
  }
}
