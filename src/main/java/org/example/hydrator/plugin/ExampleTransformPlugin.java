/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.example.hydrator.plugin;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Hydrator Transform Plugin Example - This provides a good starting point for building your own Transform Plugin
 * For full documentation, check out: https://docs.cask.co/cdap/current/en/developer-manual/pipelines/developing-plugins/index.html
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("ExampleTransform") // <- NOTE: The name of the plugin should match the name of the docs and widget json files.
@Description("This is an example transform.")
public class ExampleTransformPlugin extends Transform<StructuredRecord, StructuredRecord> {
  // If you want to log things, you will need this line
  private static final Logger LOG = LoggerFactory.getLogger(ExampleTransformPlugin.class);

  // Usually, you will need a private variable to store the config that was passed to your class
  private final Config config;
  private Schema outputSchema;

  public ExampleTransformPlugin(Config config) {
    this.config = config;
  }

  /**
   * This function is called when the pipeline is published. You should use this for validating the config and setting
   * additional parameters in pipelineConfigurer.getStageConfigurer(). Those parameters will be stored and will be made
   * available to your plugin during runtime via the TransformContext. Any errors thrown here will stop the pipeline
   * from being published.
   * @param pipelineConfigurer Configures an ETL Pipeline. Allows adding datasets and streams and storing parameters
   * @throws IllegalArgumentException If the config is invalid.
   */
  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    // It's usually a good idea to validate the configuration at this point. It will stop the pipeline from being
    // published if this throws an error.
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    config.validate(inputSchema);

    Schema oschema;

    try {
       oschema = Schema.parseJson(config.schema);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    pipelineConfigurer.getStageConfigurer().setOutputSchema(oschema);
  }

  /**
   * This function is called when the pipeline has started. The values configured in here will be made available to the
   * transform function. Use this for initializing costly objects and opening connections that will be reused.
   * @param context Context for a pipeline stage, providing access to information about the stage, metrics, and plugins.
   * @throws Exception If there are any issues before starting the pipeline.
   */
  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    //outputSchema = Schema.parseJson(config.schema);
    Schema inputSchema = Schema.parseJson(config.schema);

    outputSchema = context.getOutputSchema();
    System.out.println(outputSchema);
    //outputSchema = getOutputSchema(config, inputSchema);
  }

  /**
   * This is the method that is called for every record in the pipeline and allows you to make any transformations
   * you need and emit one or more records to the next stage.
   * @param input The record that is coming into the plugin
   * @param emitter An emitter allowing you to emit one or more records to the next stage
   * @throws Exception
   */
  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    // Get all the fields that are in the output schema
    List<Schema.Field> fields = outputSchema.getFields();

    // Create a builder for creating the output record
    StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
    StructuredRecord.Builder error = StructuredRecord.builder(input.getSchema());

    // Create schema list
    ArrayList<String> inputSchema = new ArrayList<>();
    for (Schema.Field fd : fields) {
        inputSchema.add(fd.getSchema().toString().replace("\"", ""));
        System.out.println(fd.getSchema());
    }

    // Create list of records that will be dynamically updated
    // For valid records
    ArrayList<Object> validRecordList = new ArrayList<>();

    // For invalid records
    ArrayList<Object> invalidRecordList = new ArrayList<>();

    // Schema list iterator
    int iterator = 0;

    // Add all the values to the builder
    for (Schema.Field field : fields) {

      String name = field.getName();

      if (input.get(name) != null) {

        // Comparing fields for schema validation

        /*
        1. Establish a list of fields and data types from GCS schema bucket
        2. Use a for loop to compare each field of the raw data to schema data types
           Can use built-in Java functions for thi
        3. Records that pass the validation should be emitted

        switch (inputSchema.get(iterator)) {
          case "Int" -> int_validation(input.get(name));
        };

        */

        if (inputSchema.get(iterator).equals("int")) {
          try {
            System.out.println((String) input.get(name));

            Integer.parseInt(input.get(name));
            validRecordList.add(Integer.parseInt(input.get(name)));

          } catch (Exception e) {

            invalidRecordList.add(input.get(name));
            validRecordList.add(input.get(name));

          }

        }

        else if (inputSchema.get(iterator).equals("string")) {

          try {
            String outputString = input.get(name).toString();

            validRecordList.add(outputString);

          } catch (Exception e) {
            invalidRecordList.add(input.get(name));
            validRecordList.add(input.get(name));

          }
        }

        else if (inputSchema.get(iterator).equals("float")) {

          try {
            Float.parseFloat(input.get(name));

            validRecordList.add(Float.parseFloat(input.get(name)));
          }
          catch (Exception e) {
            invalidRecordList.add(input.get(name));
            validRecordList.add(input.get(name));
          }
        }

        else if (inputSchema.get(iterator).equals("long")) {

          try {
            Long.parseLong(input.get(name));

            validRecordList.add(Long.parseLong(input.get(name)));
          }
          catch (Exception e) {
            invalidRecordList.add(input.get(name));
            validRecordList.add(input.get(name));
          }
        }

        else if (inputSchema.get(iterator).equals("double")) {

          try {
            Double.parseDouble(input.get(name));

            validRecordList.add(Double.parseDouble(input.get(name)));
          }
          catch (Exception e) {
            invalidRecordList.add(input.get(name));
            validRecordList.add(input.get(name));
          }
        }

        else if (inputSchema.get(iterator).equals("boolean")) {

          try {
            Boolean.parseBoolean(input.get(name));

            validRecordList.add(Boolean.parseBoolean(input.get(name)));
          }
          catch (Exception e) {
            invalidRecordList.add(input.get(name));
            validRecordList.add(input.get(name));
          }
        }

        else if (inputSchema.get(iterator).equals("bytes")) {

          try {
            Byte.parseByte(input.get(name));

            validRecordList.add(Byte.parseByte(input.get(name)));
          }
          catch (Exception e) {
            invalidRecordList.add(input.get(name));
            validRecordList.add(input.get(name));
          }
        }

        /*
        else if (inputSchema.get(iterator).equals("timestamp")) {

          DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-uuuu HH:mm:ss:SSS");
          try {
            LocalDateTime.parse(input.get(name), formatter);
            validRecordList.add(input.get(name));
          }
          catch (DateTimeParseException e) {
            invalidRecordList.add(input.get(name));
            validRecordList.add(input.get(name));
          }
        }
        */
        iterator++;
      }
    }
        int result = setRecords(invalidRecordList);
        System.out.println(validRecordList.size());

        int rt = 0;
        // No errors
        if (result == 1) {
          while (rt < fields.size()) {
            builder.set(fields.get(rt).getName(), validRecordList.get(rt));
            rt++;
          }
        }
        else if (result == 2) {
          while (rt < fields.size()) {
            error.set(fields.get(rt).getName(), validRecordList.get(rt));
            rt++;
          }
        }

    // If you wanted to make additional changes to the output record, this might be a good place to do it.

    if (!invalidRecordList.isEmpty()) {
      InvalidEntry<StructuredRecord> invalidEntry = new InvalidEntry<>(1, "Records do not match schema", error.build());
      emitter.emitError(invalidEntry);
    }

    else {

      // Finally, build and emit the record.
      emitter.emit(builder.build());
    }
  }

  // Set Output Schema
  private static Schema getOutputSchema(Config config, Schema inputSchema) {
    List<Schema.Field> fields = new ArrayList<>();

    fields.add(Schema.Field.of("name", Schema.of(Schema.Type.STRING)));
    fields.add(Schema.Field.of("age", Schema.of(Schema.Type.INT)));
    fields.add(Schema.Field.of("date", Schema.of(Schema.Type.STRING)));

    return Schema.recordOf(inputSchema.getRecordName(), fields);
  }

  // Emit whole records
  public static int setRecords(ArrayList<Object> invalidRecordList) {

    if (invalidRecordList.isEmpty()) {
     return 1;
    }

    else {
      return 2;
    }
  }


  /**
   * This function will be called at the end of the pipeline. You can use it to clean up any variables or connections.
   */
  @Override
  public void destroy() {
    // No Op
  }

  /**
   * Your plugin's configuration class. The fields here will correspond to the fields in the UI for configuring the
   * plugin.
   */
  public static class Config extends PluginConfig {
    @Name("myOption")
    @Description("This option is required for this transform.")
    @Macro // <- Macro means that the value will be substituted at runtime by the user.
    private final String myOption;

    @Name("myOptionalOption")
    @Description("And this option is not.")
    @Macro
    @Nullable // <- Indicates that the config param is optional
    private final Integer myOptionalOption;

    @Name("schema")
    @Description("Specifies the schema of the records outputted from this plugin.")
    private final String schema;

    public Config(String myOption, Integer myOptionalOption, String schema) {
      this.myOption = myOption;
      this.myOptionalOption = myOptionalOption;
      this.schema = schema;
    }

    private void validate(Schema inputSchema) throws IllegalArgumentException {
      // It's usually a good idea to check the schema. Sometimes users edit
      // the JSON config directly and make mistakes.
      try {
        Schema.parseJson(schema);
      } catch (IOException e) {
        throw new IllegalArgumentException("Output schema cannot be parsed.", e);
      }
      // This method should be used to validate that the configuration is valid.
      if (myOption == null || myOption.isEmpty()) {
        throw new IllegalArgumentException("myOption is a required field.");
      }
      // You can use the containsMacro() function to determine if you can validate at deploy time or runtime.
      // If your plugin depends on fields from the input schema being present or the right type, use inputSchema
    }
  }

  /*
  public void int_validation(String value){

  }
  */
}

