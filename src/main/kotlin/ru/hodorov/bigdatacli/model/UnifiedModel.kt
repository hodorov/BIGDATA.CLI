package ru.hodorov.bigdatacli.model

data class UnifiedModel(
    val name: String,
    val fields: List<UnifiedField>
) {
    override fun toString(): String {
        return """
===$name===
${fields.joinToString(separator = System.lineSeparator())}
        """
    }
}
