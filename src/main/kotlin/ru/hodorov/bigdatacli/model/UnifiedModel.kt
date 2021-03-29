package ru.hodorov.bigdatacli.model

import org.apache.hadoop.fs.Path

data class UnifiedModel(
    val path: Path?,
    val schema: UnifiedModelSchema,
    val values: List<List<UnifiedField>>
) {
    fun merge(other: UnifiedModel): UnifiedModel {
        return UnifiedModel(null, schema, this.values.plus(other.values))
    }
}
