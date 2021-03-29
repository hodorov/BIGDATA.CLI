package ru.hodorov.bigdatacli.model.mapper

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.*
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import ru.hodorov.bigdatacli.model.*
import java.util.*

class JsonSchemaMapper : SchemaMapper<JsonNode, Nothing, Nothing, Nothing>() {

    private val om = jacksonObjectMapper()

    override fun toUnifiedModelSchema(schema: Nothing): UnifiedModelSchema {
        TODO("Not yet implemented")
    }

    override fun toUnifiedType(type: Nothing): UnifiedFieldType {
        TODO("Not yet implemented")
    }

    override fun toUnifiedSubType(subType: Nothing?): UnifiedFieldSubType {
        TODO("Not yet implemented")
    }

    override fun toModelSchema(schema: UnifiedModelSchema): Nothing {
        TODO("Not yet implemented")
    }

    override fun toType(type: UnifiedFieldType): Nothing? {
        TODO("Not yet implemented")
    }

    override fun toSubType(subType: UnifiedFieldSubType): Nothing? {
        TODO("Not yet implemented")
    }

    override fun toUnifiedFieldJavaType(value: Any, unifiedFieldJavaType: UnifiedFieldJavaType): Any {
        TODO("Not yet implemented")
    }

    override fun toModel(path: Path, fs: FileSystem): UnifiedModel {
        TODO("Not yet implemented")
    }

    override fun fromModel(model: UnifiedModel): List<JsonNode> {
        return model.values.map { row ->
            val objectNode = om.createObjectNode()

            row.forEach { field ->
                val value: JsonNode? = when (field.fieldSchema.getUnifiedJavaType()) {
                    UnifiedFieldJavaType.INT -> field.getOrDefault()?.let { IntNode(it as Int) }
                    UnifiedFieldJavaType.LONG -> field.getOrDefault()?.let { LongNode(it as Long) }
                    UnifiedFieldJavaType.STRING -> field.getOrDefault()?.let { TextNode(it as String) }
                    UnifiedFieldJavaType.DATE -> field.getOrDefault()?.let { TextNode(om.dateFormat.format(it as Date)) }
                }
                objectNode.set<JsonNode>(field.fieldSchema.name, value ?: om.nullNode())
            }

            return@map objectNode
        }
    }

}
