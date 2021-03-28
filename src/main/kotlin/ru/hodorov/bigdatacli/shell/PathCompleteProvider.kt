package ru.hodorov.bigdatacli.shell

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.springframework.core.MethodParameter
import org.springframework.shell.CompletionContext
import org.springframework.shell.CompletionProposal
import org.springframework.shell.standard.ValueProvider
import org.springframework.stereotype.Component
import ru.hodorov.bigdatacli.extends.append
import ru.hodorov.bigdatacli.extends.toHadoopPath
import ru.hodorov.bigdatacli.utils.FsContext

@Component
class PathCompleteProvider(
    val fsContext: FsContext
) : ValueProvider {

    override fun supports(methodParameter: MethodParameter, completionContext: CompletionContext?): Boolean {
        return methodParameter.parameterName == "path"
    }

    // TODO: rewrite
    override fun complete(methodParameter: MethodParameter, completionContext: CompletionContext, strings: Array<String>?): List<CompletionProposal> {
        val curWord = completionContext.currentWordUpToCursor()
        val curWordSlashIndex = curWord.indexOfFirst { it == '/' }
        val path = fsContext.currentUri.append(completionContext.currentWordUpToCursor()).toHadoopPath()
        if (fsContext.fs.exists(path)) {
            if (fsContext.fs.isDirectory(path)) {
                return fsContext.fs.listStatus(path).map { CompletionProposal("${if (curWordSlashIndex == -1) "." else curWord.substring(0, curWordSlashIndex)}/${fsContext.currentUri.relativize(it.path.toUri())}") }
            }
        } else {
            var uri = path.toUri().toString()
            uri = uri.substring(0, uri.indexOfLast { it == '/' })
            val parentPath = Path(uri)
            if (fsContext.fs.isDirectory(parentPath)) {
                return fsContext.fs.listStatus(parentPath).map { CompletionProposal("${if (curWordSlashIndex == -1) "." else curWord.substring(0, curWordSlashIndex)}/${fsContext.currentUri.relativize(it.path.toUri())}") }
            }
        }

        return emptyList()
    }
}
