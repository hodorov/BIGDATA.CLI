package ru.hodorov.bigdatacli.model.mapper

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.*
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import ru.hodorov.bigdatacli.model.*
import java.util.*

private val om = jacksonObjectMapper()

class JsonSchemaMapper : SchemaMapper<ObjectNode, Nothing, Nothing, Nothing>(
    name = "json",
    mappers = listOf(
        UnifiedFieldJavaType.INT to Mapper(null, { it }),
        UnifiedFieldJavaType.LONG to Mapper(null, { it }),
        UnifiedFieldJavaType.STRING to Mapper(null, { it }),
        UnifiedFieldJavaType.DATE to Mapper(null, { TextNode(om.dateFormat.format(Date((it as LongNode).asLong()))) }),
        UnifiedFieldJavaType.MAP to Mapper(null, { it }),
    ),
    typeMapping = listOf(),
    subTypeMapping = listOf(),
    typePairsToUnifiedJavaType = listOf(),
    rawNull = om.nullNode()
) {

    override fun fromModel(model: UnifiedModel): List<ObjectNode> {
        return model.values.map { row ->
            val objectNode = om.createObjectNode()

            row.forEach { field ->
                val value = convertUnifiedToRawValue(field.jsonNodeValue, field.fieldSchema.javaType) as JsonNode
                objectNode.set<JsonNode>(field.fieldSchema.name, value)
            }

            return@map objectNode
        }
    }

    override fun toUnifiedModelSchema(schema: Nothing): UnifiedModelSchema {
        TODO("Not yet implemented")
    }

    override fun toModel(path: Path, fs: FileSystem): UnifiedModel {
        TODO("Not yet implemented")
    }

    override fun toSchema(schema: UnifiedModelSchema): Nothing {
        TODO("Not yet implemented")
    }
}
