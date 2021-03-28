package ru.hodorov.bigdatacli.extends

import org.apache.hadoop.fs.Path

fun Path.append(text: String) = Path(this, text)
