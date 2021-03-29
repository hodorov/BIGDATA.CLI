package ru.hodorov.bigdatacli.model

data class UnifiedFieldSchema(
    val name: String,
    val position: Int,
    val type: UnifiedFieldType,
    val subType: UnifiedFieldSubType,
    val javaType: UnifiedFieldJavaType,
    val default: Any?,
    val required: Boolean
) {
    override fun toString(): String {
        return "$name (pos $position) $javaType ($type:$subType) default ${if (default is String) "\"$default\"" else default} required $required"
    }
}
