package ru.hodorov.bigdatacli.model

data class UnifiedField(
    val name: String,
    val position: Int,
    val type: UnifiedFieldType,
    val subType: UnifiedFieldSubType,
    val value: Any?,
    val default: Any?,
    val required: Boolean
) {
    override fun toString(): String {
        return """
            $name (pos $position) ${getUnifiedJavaType()} ($type:$subType) default ${if (default is String) "\"$default\"" else default} required $required
        """.trimIndent()
    }

    fun getUnifiedJavaType(): UnifiedJavaType {
        return when {
            type == UnifiedFieldType.STRING && subType == UnifiedFieldSubType.NONE -> UnifiedJavaType.STRING
            type == UnifiedFieldType.BINARY && subType == UnifiedFieldSubType.STRING -> UnifiedJavaType.STRING
            type == UnifiedFieldType.INT && subType == UnifiedFieldSubType.NONE -> UnifiedJavaType.INT
            type == UnifiedFieldType.LONG && subType == UnifiedFieldSubType.NONE -> UnifiedJavaType.LONG
            type == UnifiedFieldType.LONG && subType == UnifiedFieldSubType.TIMESTAMP_MILLIS -> UnifiedJavaType.DATE
            else -> throw IllegalStateException("Unknown java type for $type $subType")
        }
    }
}
