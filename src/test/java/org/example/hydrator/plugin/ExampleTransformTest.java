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

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.mock.common.MockEmitter;
import org.junit.Assert;
import org.junit.Test;

/**
 * This is an example of how you can build unit tests for your transform.
 */
public class ExampleTransformTest {
  private static final Schema INPUT = Schema.recordOf("input",
                                                      // Raw data: string   Schema: int   Valid input data
                                                      Schema.Field.of("int-valid", Schema.of(Schema.Type.STRING)),
                                                      // Raw data: string   Schema: int   Invalid input data
                                                      Schema.Field.of("int-invalid", Schema.of(Schema.Type.STRING)),

                                                      // Raw data: string   Schema: string  Valid input data
                                                      Schema.Field.of("str-valid", Schema.of(Schema.Type.STRING)),
                                                      // Raw data: int   Schema: string  Invalid input data
                                                      Schema.Field.of("str-invalid", Schema.of(Schema.Type.INT)));
  @Test
  public void testMyTransform() throws Exception {
    ExampleTransformPlugin.Config config = new ExampleTransformPlugin.Config("SomeValue", null, INPUT.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new ExampleTransformPlugin(config);
    transform.initialize(null);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(StructuredRecord.builder(INPUT)
                          .set("int-valid", "20")
                          .set("int-invalid", "Twenty")
                          .set("str-valid", "Name")
                          .set("str-invalid", 30).build(), emitter);
    //Assert.assertEquals((Integer) 20, emitter.getEmitted().get(0).get("int-valid"));
    //Assert.assertEquals("Schema error", emitter.getEmitted().get(0).get("int-invalid"));
    //Assert.assertEquals("Name", emitter.getEmitted().get(0).get("str-valid"));
    //Assert.assertEquals("30", emitter.getEmitted().get(0).get("str-invalid"));
    Assert.assertEquals(1, emitter.getErrors().get(0).getErrorCode());

  }
}
