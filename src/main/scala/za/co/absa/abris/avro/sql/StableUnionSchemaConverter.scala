/*
 * Copyright 2024 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.abris.avro.sql

import org.apache.avro.Schema
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.types.DataType

/** A [[SchemaConverter]] that maps Avro union branches to stable, name-based Spark struct fields.
  *
  * By default, ABRiS (via Spark's [[SchemaConverters]]) maps each non-null branch of an Avro
  * union to a positional `memberN` field (e.g. `member0`, `member1`). This means column paths
  * silently break if the union branch order changes in the Avro schema.
  *
  * This converter enables Spark's stable-identifier mode, naming each branch `member_<TypeName>`
  * instead (e.g. `member_Address`, `member_Reference`). Paths become self-documenting and robust
  * against union branch reordering.
  *
  * Requires Spark 3.5.0 or later. An [[UnsupportedOperationException]] is thrown at plan
  * evaluation time if the running Spark version does not expose the two-argument overload of
  * [[SchemaConverters.toSqlType]].
  *
  * Use via [[za.co.absa.abris.config.FromAvroConfig.withSchemaConverter]]:
  * {{{
  *   AbrisConfig
  *     .fromConfluentAvro
  *     .downloadReaderSchemaByLatestVersion
  *     .andTopicNameStrategy(topic)
  *     .usingSchemaRegistry(registryUrl)
  *     .withSchemaConverter("stable-union")
  * }}}
  */
class StableUnionSchemaConverter extends SchemaConverter {
  override val shortName: String = "stable-union"

  override def toSqlType(avroSchema: Schema): DataType = {
    try {
      SchemaConverters.toSqlType(avroSchema, useStableIdForUnionType = true).dataType
    } catch {
      case _: NoSuchMethodException =>
        throw new UnsupportedOperationException(
          "The \"stable-union\" schema converter requires Spark 3.5.0 or later. " +
          "The running Spark version does not support the two-argument SchemaConverters.toSqlType overload."
        )
    }
  }
}
