package ru.hodorov.bigdatacli.model.mapper

import com.fasterxml.jackson.databind.JsonNode
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import ru.hodorov.bigdatacli.model.*

data class Mapper(
    val fromRaw: ((raw: Any) -> JsonNode)?,
    val toRaw: ((value: JsonNode) -> Any)?
)

// R - Record
// S - Schema
// T - Type
// ST - SubType
abstract class SchemaMapper<R, S, T, ST>(
    private val name: String,
    mappers: List<Pair<UnifiedFieldJavaType, Mapper>>,
    typeMapping: List<Pair<T, UnifiedFieldType>>,
    subTypeMapping: List<Pair<ST, UnifiedFieldSubType>>,
    typePairsToUnifiedJavaType: List<Pair<Pair<UnifiedFieldType, UnifiedFieldSubType>, UnifiedFieldJavaType>>,
    val rawNull: Any? = null
) {

    private val mappersByUnifiedJavaType = mappers.toMap()

    private val unifiedTypeByType = typeMapping.toMap()
    private val typeByUnifiedType = typeMapping.map { it.second to it.first }.toMap()

    private val unifiedSubTypeBySubType = subTypeMapping.toMap()
    private val subTypeByUnifiedSubType = subTypeMapping.map { it.second to it.first }.toMap()

    private val unifiedJavaTypeByTypePair = typePairsToUnifiedJavaType.toMap()
    private val typePairByUnifiedJavaType = typePairsToUnifiedJavaType.map { it.second to it.first }.toMap()

    // Raw -> unified
    abstract fun toUnifiedModelSchema(schema: S): UnifiedModelSchema

    open fun toUnifiedType(type: T): UnifiedFieldType {
        return unifiedTypeByType[type] ?: error("($name)Can't convert $type to UnifiedFieldType")
    }

    open fun toUnifiedSubType(subType: ST?): UnifiedFieldSubType {
        if (subType == null) {
            return UnifiedFieldSubType.NONE
        }
        return unifiedSubTypeBySubType[subType] ?: error("($name)Can't convert $subType to UnifiedFieldSubType")
    }

    fun toUnifiedJavaType(typePair: Pair<UnifiedFieldType, UnifiedFieldSubType>): UnifiedFieldJavaType {
        return unifiedJavaTypeByTypePair[typePair] ?: error("($name)Can't convert $typePair to UnifiedJavaType")
    }

    fun convertRawValueToUnified(value: Any, unifiedJavaType: UnifiedFieldJavaType): JsonNode {
        val mapper = mappersByUnifiedJavaType[unifiedJavaType] ?: error("($name)No converter for $unifiedJavaType")
        val converter = mapper.fromRaw ?: error("($name)No fromRaw converter for $unifiedJavaType")
        return converter.invoke(value)
    }

    abstract fun toModel(path: Path, fs: FileSystem): UnifiedModel

    // Unified -> raw
    abstract fun toSchema(schema: UnifiedModelSchema): S

    open fun toType(type: UnifiedFieldType): T {
        return typeByUnifiedType[type] ?: error("($name)Can't convert $type to raw type")
    }

    open fun toSubType(subType: UnifiedFieldSubType): ST? {
        if (subType == UnifiedFieldSubType.NONE) {
            return null
        }
        return subTypeByUnifiedSubType[subType] ?: error("($name)Can't convert $subType to raw subtype")
    }

    fun toTypePair(unifiedJavaType: UnifiedFieldJavaType): Pair<UnifiedFieldType, UnifiedFieldSubType> {
        return typePairByUnifiedJavaType[unifiedJavaType] ?: error("($name)Can't convert $unifiedJavaType to type pair")
    }

    fun convertUnifiedToRawValue(value: JsonNode?, unifiedJavaType: UnifiedFieldJavaType): Any? {
        if (value == null) {
            return rawNull
        }
        val mapper = mappersByUnifiedJavaType[unifiedJavaType] ?: error("($name)No converter for $unifiedJavaType")
        val converter = mapper.toRaw ?: error("($name)No toRaw converter for $unifiedJavaType")
        return converter.invoke(value)
    }

    abstract fun fromModel(model: UnifiedModel): List<R>

    companion object {
        val AVRO = AvroSchemaMapper()
        val PARQUET = ParquetSchemaMapper()
        val JSON = JsonSchemaMapper()
    }
}
