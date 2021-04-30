package ru.hodorov.bigdatacli.extension

import org.apache.hadoop.fs.Path

fun Path.append(text: String) = Path(this, text)
