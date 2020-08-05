package org.ekstep.analytics.framework.util

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.hadoop.io.IOUtils

import scala.util.Try

class HadoopFileUtil {

    /**
      * Delete a single file.
      */
    def delete(file: String, conf: Configuration): Boolean = {

        val path = new Path(file);
        val fileSystem = path.getFileSystem(conf);
        fileSystem.delete(path, true);
    }

    /**
      * Delete multiple files. Different file sources (aws, azure etc) can be passed here
      */
    def delete(conf: Configuration, files: String*): Seq[Boolean] = {

        for (file <- files) yield {
            val path = new Path(file);
            path.getFileSystem(conf).delete(path, true);
        }

    }

    /**
      * Merge a hadoop source folder/file into another file
      */
    def copyMerge(srcPath: String, destPath: String, conf: Configuration, deleteSrc: Boolean) {

        val srcFilePath = new Path(srcPath);
        val destFilePath = new Path(destPath);
        copyMerge(srcFilePath.getFileSystem(conf), srcFilePath, destFilePath.getFileSystem(conf), destFilePath, deleteSrc, conf)
    }

    def copyMerge(srcFS: FileSystem, srcDir: Path, dstFS: FileSystem, dstFile: Path,
                  deleteSource: Boolean, conf: Configuration): Boolean = {

        println(srcFS,srcDir)
        if (srcFS.getFileStatus(srcDir).isDirectory) {
            val outputFile = dstFS.create(dstFile)
            Try {
                srcFS.listStatus(srcDir).sortBy(_.getPath.getName)
                  .collect {
                      case status if status.isFile() =>
                          val inputFile = srcFS.open(status.getPath())
                          Try(IOUtils.copyBytes(inputFile, outputFile, conf, false))
                          inputFile.close()
                  }
            }
            outputFile.close()
            if (deleteSource) srcFS.delete(srcDir, true) else true
        } else false
    }
}