package org.apache.tez.stratosphere;

import eu.stratosphere.api.java.io.TextInputFormat;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.fs.*;
import eu.stratosphere.core.io.InputSplit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by filip on 19.05.14.
 */
public class StratosphereHelpers {

    private static final Log LOG = LogFactory.getLog(StratosphereHelpers.class);


    //  From stratosphere AbstractFileInput
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @InterfaceAudience.Private
    public static FileInputSplit[] generateSplits(
            JobContext jobContext, String inputFormatName, int numTasks)
            throws ClassNotFoundException, IOException,
            InterruptedException {

        final String pathURI = "hdfs://" + jobContext.getConfiguration().get(FileInputFormat.INPUT_DIR);
        System.out.println("PathURI: " + pathURI);
        LOG.info("PathURI LOG: " + pathURI);
        TextInputFormat inputFormat = new TextInputFormat(new Path(pathURI));
        inputFormat.configure(new Configuration());
        return inputFormat.createInputSplits(1);
        /*if (pathURI == null) {
            throw new IOException("The path to the file was not found in the runtime configuration.");
        }

        final Path path;
        try {
            path = new Path(pathURI);
        } catch (Exception iaex) {
            throw new IOException("Invalid file path specifier: ", iaex);
        }

        final List<FileInputSplit> inputSplits = new ArrayList<FileInputSplit>();

        // get all the files that are involved in the splits
        final List<FileStatus> files = new ArrayList<FileStatus>();
        long totalLength = 0;

        final FileSystem fs = path.getFileSystem();
        LOG.info("Using Filesystem: " + fs);
        final FileStatus pathFile = fs.getFileStatus(path);

        if (pathFile.isDir()) {
            // input is directory. list all contained files
            final FileStatus[] dir = fs.listStatus(path);
            for (int i = 0; i < dir.length; i++) {
                if (!dir[i].isDir()) {
                    files.add(dir[i]);
                    totalLength += dir[i].getLen();
                }
            }

        } else {
            files.add(pathFile);
            totalLength += pathFile.getLen();
        }

        final long minSplitSize = 1;
        final long maxSplitSize = (numTasks < 1) ? Long.MAX_VALUE : (totalLength / numTasks +
                (totalLength % numTasks == 0 ? 0 : 1));

        // now that we have the files, generate the splits
        int splitNum = 0;
        for (final FileStatus file : files) {

            final long len = file.getLen();
            final long blockSize = file.getBlockSize();

            final long splitSize = Math.max(minSplitSize, Math.min(maxSplitSize, blockSize));
            final long halfSplit = splitSize >>> 1;

            final long maxBytesForLastSplit = (long) (splitSize * MAX_SPLIT_SIZE_DISCREPANCY);

            if (len > 0) {

                // get the block locations and make sure they are in order with respect to their offset
                final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, len);
                Arrays.sort(blocks);

                long bytesUnassigned = len;
                long position = 0;

                int blockIndex = 0;

                while (bytesUnassigned > maxBytesForLastSplit) {
                    // get the block containing the majority of the data
                    blockIndex = getBlockIndexForPosition(blocks, position, halfSplit, blockIndex);
                    // create a new split
                    final FileInputSplit fis = new FileInputSplit(splitNum++, file.getPath(), position, splitSize,
                            blocks[blockIndex]
                                    .getHosts());
                    inputSplits.add(fis);

                    // adjust the positions
                    position += splitSize;
                    bytesUnassigned -= splitSize;
                }

                // assign the last split
                if (bytesUnassigned > 0) {
                    blockIndex = getBlockIndexForPosition(blocks, position, halfSplit, blockIndex);
                    final FileInputSplit fis = new FileInputSplit(splitNum++, file.getPath(), position,
                            bytesUnassigned,
                            blocks[blockIndex].getHosts());
                    inputSplits.add(fis);
                }
            } else {
                // special case with a file of zero bytes size
                final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, 0);
                String[] hosts;
                if (blocks.length > 0) {
                    hosts = blocks[0].getHosts();
                } else {
                    hosts = new String[0];
                }
                final FileInputSplit fis = new FileInputSplit(splitNum++, file.getPath(), 0, 0, hosts);
                inputSplits.add(fis);
            }
        }

        return inputSplits.toArray(new FileInputSplit[inputSplits.size()]);*/
    }

    //  From stratosphere AbstractFileInput
    private static final int getBlockIndexForPosition(final BlockLocation[] blocks, final long offset,
                                               final long halfSplitSize, final int startIndex) {

        // go over all indexes after the startIndex
        for (int i = startIndex; i < blocks.length; i++) {
            long blockStart = blocks[i].getOffset();
            long blockEnd = blockStart + blocks[i].getLength();

            if (offset >= blockStart && offset < blockEnd) {
                // got the block where the split starts
                // check if the next block contains more than this one does
                if (i < blocks.length - 1 && blockEnd - offset < halfSplitSize) {
                    return i + 1;
                } else {
                    return i;
                }
            }
        }
        throw new IllegalArgumentException("The given offset is not contained in the any block.");
    }
}
