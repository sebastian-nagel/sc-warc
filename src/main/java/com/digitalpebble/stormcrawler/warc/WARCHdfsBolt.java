package com.digitalpebble.stormcrawler.warc;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class WARCHdfsBolt extends GzipHdfsBolt {

    private static final Logger LOG = LoggerFactory
            .getLogger(WARCHdfsBolt.class);

    private byte[] header;
    private Map<String, String> warcInfoFields;
    private Map<String, String> warcInfoOptHeader;

    public WARCHdfsBolt() {
        super();
        FileSizeRotationPolicy rotpol = new FileSizeRotationPolicy(1.0f,
                Units.GB);
        withRecordFormat(new WARCRecordFormat()).withRotationPolicy(rotpol);
        // dummy sync policy
        withSyncPolicy(new CountSyncPolicy(1000));
        // trigger rotation on size of compressed WARC file (not uncompressed
        // content)
        this.setRotationCompressedSizeOffset(true);
        // default local filesystem
        withFsUrl("file:///");
    }

    /**
     * Define a header, i.e., the first record of a WARC file.
     * Deprecated, use {@link #setWarcInfo(Map, Map)} instead.
     *
     * @param header
     * @return
     */
    @Deprecated
    public WARCHdfsBolt withHeader(byte[] header) {
        this.header = header;
        return this;
    }

    /**
     * Set content used to generate warcinfo record, see
     * {@link WARCRecordFormat#generateWARCInfo(Map,Map)}.
     *
     * @param warcFields
     * @param optionalHeaderFields
     *            optional fields to be added to the WARC record header If the
     *            map contains a key &quot;WARC-Filename&quot; with null as
     *            value, the current file name is added.
     * @return
     */
    public WARCHdfsBolt setWarcInfo(Map<String, String> warcFields,
            Map<String, String> optionalHeaderFields) {
        this.warcInfoFields = warcFields;
        this.warcInfoOptHeader = optionalHeaderFields;
        return this;
    }

    protected Path createOutputFile() throws IOException {
        Path path = super.createOutputFile();
        LOG.info("Starting new WARC file: {}", path);

        // write the header at the beginning of the file
        if (header != null && header.length > 0) {
            writeRecord(header);
        }

        // write warcinfo record with current date and filename
        if (warcInfoFields != null) {
            Map<String, String> headerFields = new LinkedHashMap<>();
            for (String field : warcInfoOptHeader.keySet()) {
                String value = warcInfoOptHeader.get(field);
                if ("WARC-Filename".equals(field) && value == null) {
                    value = path.getName();
                }
                headerFields.put(field, value);
            }
            byte[] warcinfo = WARCRecordFormat.generateWARCInfo(warcInfoFields,
                    headerFields);
            writeRecord(warcinfo);
        }

        return path;
    }

}
