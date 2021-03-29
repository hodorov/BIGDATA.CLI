package ru.hodorov.bigdatacli.model

data class UnifiedFieldSchema(
    val name: String,
    val position: Int,
    val type: UnifiedFieldType,
    val subType: UnifiedFieldSubType,
    val default: Any?,
    val required: Boolean
) {
    override fun toString(): String {
        return "$name (pos $position) ${getUnifiedJavaType()} ($type:$subType) default ${if (default is String) "\"$default\"" else default} required $required"
    }

    fun getUnifiedJavaType(): UnifiedFieldJavaType {
        return when {
            type == UnifiedFieldType.STRING && subType == UnifiedFieldSubType.NONE -> UnifiedFieldJavaType.STRING // Avro
            type == UnifiedFieldType.BINARY && subType == UnifiedFieldSubType.STRING -> UnifiedFieldJavaType.STRING // Parquet
            type == UnifiedFieldType.INT && subType == UnifiedFieldSubType.NONE -> UnifiedFieldJavaType.INT
            type == UnifiedFieldType.LONG && subType == UnifiedFieldSubType.NONE -> UnifiedFieldJavaType.LONG
            type == UnifiedFieldType.LONG && subType == UnifiedFieldSubType.TIMESTAMP_MILLIS -> UnifiedFieldJavaType.DATE
            else -> throw IllegalStateException("Unknown java type for $type $subType")
        }
    }
}
