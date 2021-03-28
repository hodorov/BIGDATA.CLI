package ru.hodorov.bigdatacli.utils

import com.google.common.base.Joiner

class DFSUtil {
    companion object {
        // Copy of DFSUtil.addSuffix from org.apache.hadoop:hadoop-hdfs
        private fun addSuffix(key: String, suffix: String?): String {
            if (suffix == null || suffix.isEmpty()) {
                return key
            }
            assert(!suffix.startsWith(".")) { "suffix '$suffix' should not already have '.' prepended." }
            return "$key.$suffix"
        }

        // Copy of DFSUtil.concatSuffixes from org.apache.hadoop:hadoop-hdfs
        private fun concatSuffixes(vararg suffixes: String) = Joiner.on(".").skipNulls().join(suffixes)

        // Copy of DFSUtil.addKeySuffixes from org.apache.hadoop:hadoop-hdfs
        fun addKeySuffixes(key: String, vararg suffixes: String): String {
            val keySuffix = concatSuffixes(*suffixes)
            return addSuffix(key, keySuffix)
        }
    }
}
