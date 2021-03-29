package ru.hodorov.bigdatacli.model

data class UnifiedModelSchema(
    val name: String,
    val fields: List<UnifiedFieldSchema>
) {
    override fun toString(): String {
        return """
===$name===
${fields.joinToString(separator = System.lineSeparator())}
        """
    }
}
