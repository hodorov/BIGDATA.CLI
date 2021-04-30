package ru.hodorov.bigdatacli.utils

import mu.KotlinLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.CommonConfigurationKeys
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.LocalFileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys
import org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider


private val log = KotlinLogging.logger { }

class HDFSUtils {
    companion object {
        fun getHdfsConfig(_url: String, timeout: String): Configuration {
            var url = _url
            val configuration = Configuration()
            // Shadow jar bug. Hadoop 2.7.3+ split hadoop-client and hadoop-hdfs-client to two separated jar
            // https://stackoverflow.com/questions/17265002/hadoop-no-filesystem-for-scheme-file
            configuration["fs.hdfs.impl"] = DistributedFileSystem::class.java.name
            configuration["fs.file.impl"] = LocalFileSystem::class.java.name

            // Hosts
            var pos = url.indexOf('?')
            if (pos > 0) {
                val addresses = url.substring(pos + 1).split(",").toTypedArray()
                val nameservice = Path(url.substring(0, pos)).toUri().host
                val namenodes = StringBuffer()
                for (i in 1..addresses.size) {
                    configuration[DFSUtil.addKeySuffixes(HdfsClientConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY, nameservice, "nn$i")] = addresses[i - 1]
                    if (namenodes.isNotEmpty()) {
                        namenodes.append(',')
                    }
                    namenodes.append("nn$i")
                }
                configuration[DFSUtil.addKeySuffixes(HdfsClientConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX, nameservice)] = namenodes.toString()
                configuration[HdfsClientConfigKeys.DFS_NAMESERVICES] = nameservice
                configuration[HdfsClientConfigKeys.Failover.PROXY_PROVIDER_KEY_PREFIX + "." + nameservice] = ConfiguredFailoverProxyProvider::class.java.name
                url = url.substring(0, pos)
            }

            // User
            pos = url.lastIndexOf('@')
            if (pos > 0) {
                val uri = Path(url).toUri()
                val user = uri.userInfo
                url = uri.scheme + "://" + uri.host
                configuration[CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_USER_NAME_KEY] = user
            }

            // URL
            configuration[FileSystem.FS_DEFAULT_NAME_KEY] = url

            // Timeout
            configuration[HdfsClientConfigKeys.DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY] = timeout
            configuration[HdfsClientConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY] = timeout

            return configuration
        }

        fun getHdfs(url: String, timeout: String) = getHdfs(getHdfsConfig(url, timeout))

        fun getHdfs(configuration: Configuration): FileSystem {
            val user = configuration[CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_USER_NAME_KEY]
            val url = configuration[CommonConfigurationKeys.FS_DEFAULT_NAME_KEY]
            log.info("Init hdfs connection: $url / $user")
            return FileSystem.get(FileSystem.getDefaultUri(configuration), configuration, user)
        }
    }
}
