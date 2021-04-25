package ru.hodorov.bigdatacli.model

import com.fasterxml.jackson.databind.JsonNode

data class UnifiedField(
    val fieldSchema: UnifiedFieldSchema,
    val jsonNodeValue: JsonNode?,
    val rawValue: Any?
)
