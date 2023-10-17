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

                                                      Schema.Field.of("name", Schema.of(Schema.Type.STRING)),

                                                      Schema.Field.of("age", Schema.of(Schema.Type.STRING)),

                                                      Schema.Field.of("date", Schema.of(Schema.Type.STRING)));

  @Test
  public void testMyTransform() throws Exception {
    ExampleTransformPlugin.Config config = new ExampleTransformPlugin.Config("SomeValue", null, INPUT.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new ExampleTransformPlugin(config);
    transform.initialize(null);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();

    transform.transform(StructuredRecord.builder(INPUT)
                          .set("name", "Ryan")
                          .set("age", "43")
                          .set("date", "27-08-2023 15:14:13:321").build(), emitter);

    transform.transform(StructuredRecord.builder(INPUT)
                          .set("name", "Louise")
                          .set("age", "fourtythree")
                          .set("date", "27;02;2023 15:13:11:987").build(), emitter);

    /*
    StructuredRecord.Builder testBuilder = StructuredRecord.builder(INPUT);
    testBuilder.set
    */

    //Assert.assertEquals("Ryan", emitter.getEmitted().get(0).get("name"));
    //Assert.assertEquals((Integer) 43, emitter.getEmitted().get(0).get("age"));
    //Assert.assertEquals("Louise", emitter.getEmitted().get(1).get("name"));
    //Assert.assertEquals((Integer) 23, emitter.getEmitted().get(1).get("age"));
    Assert.assertEquals(1, emitter.getErrors().get(1).getErrorCode());

  }
}
