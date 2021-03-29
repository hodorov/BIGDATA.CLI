package ru.hodorov.bigdatacli.model

data class UnifiedField(
    val fieldSchema: UnifiedFieldSchema,
    val value: Any?
) {
    fun getOrDefault() = value ?: fieldSchema.default
}
