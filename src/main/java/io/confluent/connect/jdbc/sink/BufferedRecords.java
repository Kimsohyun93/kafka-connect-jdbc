/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.BatchUpdateException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialect.StatementBinder;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SchemaPair;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.TableId;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

public class BufferedRecords {
  private static final Logger log = LoggerFactory.getLogger(BufferedRecords.class);

  private final TableId tableId;
  private final JdbcSinkConfig config;
  private final DatabaseDialect dbDialect;
  private final DbStructure dbStructure;
  private final Connection connection;

  private List<SinkRecord> records = new ArrayList<>();
  private Schema keySchema;
  private Schema valueSchema = SchemaBuilder.struct()
          .field("applicationentity", Schema.STRING_SCHEMA)
          .field("container", Schema.STRING_SCHEMA)
          .field("latitude", Schema.FLOAT64_SCHEMA)
          .field("longitude", Schema.FLOAT64_SCHEMA)
          .field("altitude", Schema.FLOAT64_SCHEMA)
          .field("creationtime", Schema.STRING_SCHEMA)
          .build();
  private RecordValidator recordValidator;
  private FieldsMetadata fieldsMetadata;
  private PreparedStatement updatePreparedStatement;
  private PreparedStatement deletePreparedStatement;
  private StatementBinder updateStatementBinder;
  private StatementBinder deleteStatementBinder;
  private boolean deletesInBatch = false;
  private ObjectMapper objectMapper = new ObjectMapper();

  public BufferedRecords(
      JdbcSinkConfig config,
      TableId tableId,
      DatabaseDialect dbDialect,
      DbStructure dbStructure,
      Connection connection
  ) {
    this.tableId = tableId;
    this.config = config;
    this.dbDialect = dbDialect;
    this.dbStructure = dbStructure;
    this.connection = connection;
    this.recordValidator = RecordValidator.create(config);
  }

  public List<SinkRecord> add(SinkRecord record) throws SQLException, TableAlterOrCreateException {

    recordValidator.validate(record);
    final List<SinkRecord> flushed = new ArrayList<>();

    boolean schemaChanged = false;
    if (!Objects.equals(keySchema, record.keySchema())) {
      keySchema = record.keySchema();
      schemaChanged = true;
    }
    if (isNull(record.valueSchema())) {
      // For deletes, value and optionally value schema come in as null.
      // We don't want to treat this as a schema change if key schemas is the same
      // otherwise we flush unnecessarily.
      if (config.deleteEnabled) {
        deletesInBatch = true;
      }
    } else if (Objects.equals(valueSchema, record.valueSchema())) {
      if (config.deleteEnabled && deletesInBatch) {
        // flush so an insert after a delete of same record isn't lost
        flushed.addAll(flush());
      }
    } else {
      // value schema is not null and has changed. This is a real schema change.
      valueSchema = record.valueSchema();
      schemaChanged = true;
    }
    if (schemaChanged || updateStatementBinder == null) {
      // Each batch needs to have the same schemas, so get the buffered records out
      flushed.addAll(flush());

      // re-initialize everything that depends on the record schema
      final SchemaPair schemaPair = new SchemaPair(
          record.keySchema(),
          valueSchema
      );
      fieldsMetadata = FieldsMetadata.extract(
          tableId.tableName(),
          config.pkMode,
          config.pkFields,
          config.fieldsWhitelist,
          schemaPair
      );
      dbStructure.createOrAmendIfNecessary(
          config,
          connection,
          tableId,
          fieldsMetadata
      );
      final String insertSql = getInsertSql();
      final String deleteSql = getDeleteSql();
      log.debug(
          "{} sql: {} deleteSql: {} meta: {}",
          config.insertMode,
          insertSql,
          deleteSql,
          fieldsMetadata
      );
      close();
      updatePreparedStatement = dbDialect.createPreparedStatement(connection, insertSql);
      updateStatementBinder = dbDialect.statementBinder(
          updatePreparedStatement,
          config.pkMode,
          schemaPair,
          fieldsMetadata,
          dbStructure.tableDefinition(connection, tableId),
          config.insertMode
      );
      if (config.deleteEnabled && nonNull(deleteSql)) {
        deletePreparedStatement = dbDialect.createPreparedStatement(connection, deleteSql);
        deleteStatementBinder = dbDialect.statementBinder(
            deletePreparedStatement,
            config.pkMode,
            schemaPair,
            fieldsMetadata,
            dbStructure.tableDefinition(connection, tableId),
            config.insertMode
        );
      }
    }
    
    // set deletesInBatch if schema value is not null
    if (isNull(record.value()) && config.deleteEnabled) {
      deletesInBatch = true;
    }

    records.add(record);

    if (records.size() >= config.batchSize) {
      flushed.addAll(flush());
    }
    return flushed;
  }

  /**
   * {"rn":"4-202209060223544915105",
   * "ty":"4",
   * "pi":"/Mobius/ae/cnt",
   * "ri":"/Mobius/ae/cnt/4-202209060223544915105","
   * ct":"20220906T022354",
   * "lt":"20220906T022354","
   * st":11,
   * "et":"20240906T022354",
   * "cs":"62",
   * "cnf":"",
   * "con":{"Latitude":16.45243,"Longitude":100.48484,"Altitude":13.4646},
   * "acpi":[],
   * "lbl":[],
   * "at":[],
   * "aa":[],
   * "subl":[],
   * "or":"",
   * "cr":"S20170717074825768bp2l",
   * "spi":"3-20220817050017027728",
   * "sri":"4-20220906022354491376"}
   */

  @SuppressWarnings("unchecked")
  public List<SinkRecord> flush() throws SQLException {
    if (records.isEmpty()) {
      log.debug("Records is empty");
      return new ArrayList<>();
    }
    log.debug("Flushing {} buffered records", records.size());
    for (SinkRecord record : records) {
      // Parsing to Mobius Data format
      Map<String, Object> recordValue = (Map<String, Object>) record.value();

      Map<String, Object> cf = (Map<String, Object>) recordValue.get("con");

      Iterator<String> iteratorKey = cf.keySet().iterator(); // 키값 오름차순
      Map<String, Object> conField = new HashMap<>();
      while (iteratorKey.hasNext()) {
        String key = iteratorKey.next();
        conField.put(key.toLowerCase(), cf.get(key));
      }
      //Map<String, Object> conField = (Map<String, Object>) recordValue.get("con");

      String cinURI = (String) recordValue.get("pi");
      String[] uriArr = cinURI.split("/");


      String creationTime = (String) recordValue.get("ct");
      SimpleDateFormat dateParser  = new SimpleDateFormat("yyyyMMdd'T'HHmmss");
      SimpleDateFormat  dateFormatter   = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      Date parsedTime = null;
      try {
        parsedTime = dateParser.parse(creationTime);
      } catch (ParseException e) {
        e.printStackTrace();
      }
      creationTime = dateFormatter.format(parsedTime);

      // prod kafka
      Map<String, Object> kafkaProdData = conField;
      kafkaProdData.put("applicationentity", uriArr[2]);
      kafkaProdData.put("container", uriArr[3]);
      kafkaProdData.put("creationtime", creationTime);
      try {
        prodKafka("refine.spatial", kafkaProdData);
      } catch (Exception e) {
        e.printStackTrace();
      }

      System.out.println("################## \n\n \nHERE JDBC : :: ::: ::::");
      System.out.println(conField.get("latitude") instanceof Double);
      System.out.println("THIS IS CONFIELD : " + conField);
      System.out.println(conField.get("latitude").getClass().getName());

      Struct valueStruct = new Struct(valueSchema)
              .put("applicationentity", uriArr[2])
              .put("container", uriArr[3])
              .put("latitude", conField.get("latitude") instanceof Double ? (Double)(conField.get("latitude")) : 0.0)
              .put("longitude", conField.get("longitude") instanceof Double ? (Double)conField.get("longitude") : 0.0)
              .put("altitude", conField.get("altitude") instanceof Double ? (Double)conField.get("altitude") : 0.0)
              .put("creationtime", creationTime);

      SinkRecord valueRecord =
              new SinkRecord(
                      record.topic(),
                      record.kafkaPartition(),
                      record.keySchema(),
                      record.key(),
                      valueSchema,
                      valueStruct,
                      record.kafkaOffset()
              );
      if (isNull(record.value()) && nonNull(deleteStatementBinder)) {
        deleteStatementBinder.bindRecord(record);
      } else {
        updateStatementBinder.bindRecord(valueRecord);
      }
    }
    executeUpdates();
    executeDeletes();

    final List<SinkRecord> flushedRecords = records;
    records = new ArrayList<>();
    deletesInBatch = false;
    return flushedRecords;
  }

  private void prodKafka(String topic, Map<String, Object> data) throws Exception {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    Producer<String, String> producer = new KafkaProducer<String, String>(props);

    try {
      producer.send(
              new ProducerRecord<String, String>(topic, objectMapper.writeValueAsString(data))
      );
      System.out.println("Message sent successfully" + data);
      producer.close();
    } catch (Exception e) {
      System.out.println("Kafka Produce Exception : " + e);
    }
  }

  private void executeUpdates() throws SQLException {
    int[] batchStatus = updatePreparedStatement.executeBatch();
    for (int updateCount : batchStatus) {
      if (updateCount == Statement.EXECUTE_FAILED) {
        throw new BatchUpdateException(
                "Execution failed for part of the batch update", batchStatus);
      }
    }
  }

  private void executeDeletes() throws SQLException {
    if (nonNull(deletePreparedStatement)) {
      int[] batchStatus = deletePreparedStatement.executeBatch();
      for (int updateCount : batchStatus) {
        if (updateCount == Statement.EXECUTE_FAILED) {
          throw new BatchUpdateException(
                  "Execution failed for part of the batch delete", batchStatus);
        }
      }
    }
  }

  public void close() throws SQLException {
    log.debug(
        "Closing BufferedRecords with updatePreparedStatement: {} deletePreparedStatement: {}",
        updatePreparedStatement,
        deletePreparedStatement
    );
    if (nonNull(updatePreparedStatement)) {
      updatePreparedStatement.close();
      updatePreparedStatement = null;
    }
    if (nonNull(deletePreparedStatement)) {
      deletePreparedStatement.close();
      deletePreparedStatement = null;
    }
  }

  private String getInsertSql() throws SQLException {
    switch (config.insertMode) {
      case INSERT:
        return dbDialect.buildInsertStatement(
            tableId,
            asColumns(fieldsMetadata.keyFieldNames),
            asColumns(fieldsMetadata.nonKeyFieldNames),
            dbStructure.tableDefinition(connection, tableId)
        );
      case UPSERT:
        if (fieldsMetadata.keyFieldNames.isEmpty()) {
          throw new ConnectException(String.format(
              "Write to table '%s' in UPSERT mode requires key field names to be known, check the"
                  + " primary key configuration",
              tableId
          ));
        }
        try {
          return dbDialect.buildUpsertQueryStatement(
              tableId,
              asColumns(fieldsMetadata.keyFieldNames),
              asColumns(fieldsMetadata.nonKeyFieldNames),
              dbStructure.tableDefinition(connection, tableId)
          );
        } catch (UnsupportedOperationException e) {
          throw new ConnectException(String.format(
              "Write to table '%s' in UPSERT mode is not supported with the %s dialect.",
              tableId,
              dbDialect.name()
          ));
        }
      case UPDATE:
        return dbDialect.buildUpdateStatement(
            tableId,
            asColumns(fieldsMetadata.keyFieldNames),
            asColumns(fieldsMetadata.nonKeyFieldNames),
            dbStructure.tableDefinition(connection, tableId)
        );
      default:
        throw new ConnectException("Invalid insert mode");
    }
  }

  private String getDeleteSql() {
    String sql = null;
    if (config.deleteEnabled) {
      switch (config.pkMode) {
        case RECORD_KEY:
          if (fieldsMetadata.keyFieldNames.isEmpty()) {
            throw new ConnectException("Require primary keys to support delete");
          }
          try {
            sql = dbDialect.buildDeleteStatement(
                tableId,
                asColumns(fieldsMetadata.keyFieldNames)
            );
          } catch (UnsupportedOperationException e) {
            throw new ConnectException(String.format(
                "Deletes to table '%s' are not supported with the %s dialect.",
                tableId,
                dbDialect.name()
            ));
          }
          break;

        default:
          throw new ConnectException("Deletes are only supported for pk.mode record_key");
      }
    }
    return sql;
  }

  private Collection<ColumnId> asColumns(Collection<String> names) {
    return names.stream()
        .map(name -> new ColumnId(tableId, name))
        .collect(Collectors.toList());
  }
}
