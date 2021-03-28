package ru.hodorov.bigdatacli.config

import mu.KotlinLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.CommonConfigurationKeys
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.LocalFileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys
import org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import ru.hodorov.bigdatacli.utils.DFSUtil

private val log = KotlinLogging.logger { }

@org.springframework.context.annotation.Configuration
class HdfsConfig(
    @Value("\${hdfs.user}")
    @Suppress("SpringJavaInjectionPointsAutowiringInspection")
    private val hdfsUser: String?,
    @Value("\${hdfs.url}")
    @Suppress("SpringJavaInjectionPointsAutowiringInspection")
    private val hdfsUrl: String,
    @Value("\${hdfs.timeout}")
    @Suppress("SpringJavaInjectionPointsAutowiringInspection")
    private val hdfsTimeout: String
) {

    @Bean
    fun fsBean(): FileSystem {
        val configuration = hdfsConfiguration()
        val user = configuration[CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_USER_NAME_KEY]
        log.info("Init hdfs connection: $hdfsUrl / $user")
        return FileSystem.get(FileSystem.getDefaultUri(configuration), configuration, user)
    }


    private fun hdfsConfiguration(): Configuration {
        var hdfsUrl = this.hdfsUrl
        var user = this.hdfsUser
        val configuration = Configuration()
        // Shadow jar bug. Hadoop 2.7.3+ split hadoop-client and hadoop-hdfs-client to two separated jar. TODO: Spring?
        // https://stackoverflow.com/questions/17265002/hadoop-no-filesystem-for-scheme-file
        configuration["fs.hdfs.impl"] = DistributedFileSystem::class.java.name
        configuration["fs.file.impl"] = LocalFileSystem::class.java.name

        // Extract hadoop hosts list
        var pos = hdfsUrl.indexOf('?')
        if (pos > 0) {
            val addresses = hdfsUrl.substring(pos + 1).split(",").toTypedArray()
            val nameservice = Path(hdfsUrl.substring(0, pos)).toUri().host
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
            hdfsUrl = hdfsUrl.substring(0, pos)
        }

        configuration[HdfsClientConfigKeys.DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY] = hdfsTimeout
        configuration["dfs.socket.timeout"] = hdfsTimeout

        // Extract user name
        pos = hdfsUrl.lastIndexOf('@')
        if (pos > 0) {
            val uri = Path(hdfsUrl).toUri()
            if (user == null) user = uri.userInfo
            hdfsUrl = uri.scheme + "://" + uri.host
        }
        configuration[FileSystem.FS_DEFAULT_NAME_KEY] = hdfsUrl
        if (user != null) {
            configuration[CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_USER_NAME_KEY] = user
        }
        return configuration
    }
}
