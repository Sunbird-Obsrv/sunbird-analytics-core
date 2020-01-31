package org.ekstep.analytics.framework.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileUtil

class HadoopFileUtil {
  
  /**
   * Delete a single file.
   */
  def delete(file: String, conf: Configuration) : Boolean = {
    
    val path = new Path(file);
    val fileSystem = path.getFileSystem(conf);
    fileSystem.delete(path, true);
  }
  
  /**
   * Delete multiple files. Different file sources (aws, azure etc) can be passed here
   */
  def delete(conf: Configuration, files: String*) : Seq[Boolean] = {
    
    for(file <- files) yield {
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
    
    FileUtil.copyMerge(srcFilePath.getFileSystem(conf), srcFilePath, destFilePath.getFileSystem(conf), destFilePath, deleteSrc, conf, null)
  }
  
}