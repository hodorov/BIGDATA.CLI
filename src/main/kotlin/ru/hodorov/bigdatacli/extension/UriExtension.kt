package ru.hodorov.bigdatacli.extension

import org.apache.hadoop.fs.Path
import java.net.URI

fun URI.append(path: String): URI {
    return this.resolve(if (path.startsWith("/")) path else "${toString()}/$path").normalize()
}

fun URI.toHadoopPath(): Path {
    return Path(this)
}
