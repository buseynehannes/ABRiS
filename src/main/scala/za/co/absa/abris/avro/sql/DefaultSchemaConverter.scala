/*
 * Copyright 2022 ABSA Group Limited
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

/** Default [[SchemaConverter]] that delegates to Spark's [[SchemaConverters]].
 *
 * @param useStableIdForUnionType when `true`, Avro union branches are decoded into Spark struct
 *                                fields named `member_<TypeName>` (e.g. `member_BrowserPage`)
 *                                instead of the default positional `member0`, `member1`, etc.
 *                                Requires Spark 3.5 or later; see [[AbrisAvroDeserializer]].
 */
class DefaultSchemaConverter(useStableIdForUnionType: Boolean = false) extends SchemaConverter {
  override val shortName: String = "default"
  override def toSqlType(avroSchema: Schema): DataType =
    SchemaConverters.toSqlType(avroSchema, useStableIdForUnionType).dataType
}
