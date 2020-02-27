package de.einsweniger.solr;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.StopFilterFactory;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.Terms;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IntsRef;
import org.apache.solr.analysis.TokenizerChain;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.*;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.tagger.OffsetCorrector;
import org.apache.solr.handler.tagger.XmlOffsetCorrector;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.*;
import org.apache.solr.util.RefCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.io.StringReader;
import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.concurrent.Callable;

public class TaggerStream extends TupleStream implements Expressible {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private interface DefaultFactory<T> {
        public T build();
    }
    // parameter names
    public static final String OVERLAPS = "overlaps";
    public static final String TAGS_LIMIT = "tagsLimit";
    public static final String MATCH_TEXT = "matchText";
    public static final String SKIP_ALT_TOKENS = "skipAltTokens";
    public static final String INDEXED_FIELD = "field";
    public static final String IGNORE_STOPWORDS = "ignoreStopwords";
    public static final String XML_OFFSET_ADJUST = "xmlOffsetAdjust";
    public static final String ZK_HOST = "zkHost";
    public static final String FIELD_TO_TAG = "fieldToTag";
//    public static final String FILTER_QUERY = "fq";

    // populated in constructor
    private final StreamExpression expression;
    private final StreamFactory factory;
    private final TupleStream inputStream;
    private final String zkHost;
    private final TagClusterReducer tagClusterReducer;
    private final String indexedField;
    private final String overlapString;
//    private final String filterQuery;
    private final String fieldToTag;
//    private final String[] corpusFilterQueries;
    private final boolean skipAltTokens;


    private final boolean addMatchText;
    private final boolean xmlOffsetAdjust;
    private final int tagsLimit;
//    private final OffsetCorrector offsetCorrector;

    final List tags = new ArrayList(2000);
    private FixedBitSet matchDocIdsBS;

    // populated in setStreamContext()
    private Analyzer indexAnalyzer;
    private Analyzer queryAnalyzer;
    private SchemaField idSchemaField;
    private RefCounted<SolrIndexSearcher> searcherHolder;
    private boolean ignoreStopWords;
    private SolrCore core;
    private IndexSchema schema;
    private ReusableTagger tagger;

    // populated in open()
    protected transient SolrClientCache cache;
    protected transient CloudSolrClient cloudSolrClient;

    public TaggerStream(StreamExpression expression, StreamFactory factory) throws IOException {
        this.expression = expression;
        this.factory = factory;
        List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, TupleStream.class);
        if (streamExpressions.size() != 1) {
            throw new IOException(String.format(Locale.ROOT,"invalid expression %s - expected 1 input stream",expression));
        }
        this.inputStream = factory.constructStream(streamExpressions.get(0));
        String collectionName = factory.getValueOperand(expression, 0);
        if(null == collectionName){
            throw new IOException(String.format(Locale.ROOT,"invalid expression %s - collectionName expected as first operand",expression));
        }

        this.zkHost = getString(ZK_HOST, ()->{
            String zkHost = factory.getCollectionZkHost(collectionName);
            if (null != zkHost) {
                return zkHost;
            }
            return factory.getDefaultZkHost();
        });
        if (null == this.zkHost) {
            throw new IOException(String.format(Locale.ROOT,"invalid expression %s - zkHost not found for collection '%s'",expression,collectionName));
        }
        this.indexedField = getString(INDEXED_FIELD, () -> null);
        if (null == this.indexedField) {
            throw new IOException(String.format(Locale.ROOT,"required field: '%s'", INDEXED_FIELD));
        }
        this.fieldToTag = getString(FIELD_TO_TAG, () -> null);
        if (null == this.fieldToTag) {
            throw new IOException(String.format(Locale.ROOT,"required field: '%s'", FIELD_TO_TAG));
        }

        this.overlapString = getString(OVERLAPS, () -> "NO_SUB");
        this.tagClusterReducer = chooseTagClusterReducer(this.overlapString);
        this.skipAltTokens = getBool(SKIP_ALT_TOKENS, () -> false);
        this.addMatchText = getBool(MATCH_TEXT, () -> false);
        this.tagsLimit = getInt(TAGS_LIMIT, () -> 1000);
        this.xmlOffsetAdjust = this.getBool(XML_OFFSET_ADJUST, () -> false);
        if (this.xmlOffsetAdjust) {
            throw new IOException(String.format(Locale.ROOT,"'%s' was requested, but is not implemented", XML_OFFSET_ADJUST));
        }
        //final String[] corpusFilterQueries = req.getParams().getParams("fq");
        // final boolean xmlOffsetAdjust = params.getBool(XML_OFFSET_ADJUST, false);
    }

    @Override
    public void setStreamContext(StreamContext context) {
        Object solrCoreObj = context.get("solr-core");
        if (!(solrCoreObj instanceof SolrCore)) {
            throw new SolrException(SolrException.ErrorCode.INVALID_STATE, "StreamContext must have SolrCore in solr-core key");
        }
        this.core = ((SolrCore) solrCoreObj);
        this.schema = core.getLatestSchema();
        this.searcherHolder = core.getSearcher();
        this.matchDocIdsBS = new FixedBitSet(this.searcherHolder.get().maxDoc());
        this.indexAnalyzer = schema.getFieldType(this.indexedField).getIndexAnalyzer();
        this.queryAnalyzer = schema.getFieldType(this.indexedField).getQueryAnalyzer();
        this.idSchemaField = schema.getUniqueKeyField();
        this.ignoreStopWords = getBool(IGNORE_STOPWORDS, () -> fieldHasIndexedStopFilter(this.indexAnalyzer));
        if (idSchemaField == null) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "The tagger requires a" +
                    "uniqueKey in the schema.");//TODO this could be relaxed
        }
        this.cache = context.getSolrClientCache();
        try {
            buildTagger();
        } catch (IOException e) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "unable to bulid tagger",e);
        }
        this.inputStream.setStreamContext(context);
    }

    @Override
    public List<TupleStream> children() {
        List<TupleStream> l = new ArrayList<>();
        l.add(inputStream);
        return l;
    }

    @Override
    public void open() throws IOException {
        if(cache != null) {
            cloudSolrClient = cache.getCloudSolrClient(zkHost);
        } else {
            final List<String> hosts = new ArrayList<>();
            hosts.add(zkHost);
            cloudSolrClient = new CloudSolrClient.Builder(hosts, Optional.empty()).build();
        }
        this.inputStream.open();
    }

    @Override
    public void close() throws IOException {
        if(null != this.searcherHolder) {
            this.searcherHolder.decref();
            this.searcherHolder = null;
        }
        this.inputStream.close();
    }

    @Override
    public Tuple read() throws IOException {
        tags.clear();
        matchDocIdsBS.clear(0, matchDocIdsBS.length());
        var tuple = this.inputStream.read();
        if (tuple.EOF) {
            return tuple;
        }
        // check if tuple contains the fieldToTag, otherwise raise?
        String text = tuple.getString(this.fieldToTag);
        log.info("text field content: "+ text);
        this.tagger.reset(queryAnalyzer.tokenStream("",new StringReader(text)));
        this.tagger.process();

//        this.parseTags(new StringReader(text), tuple);
        var doclist = getDocList(10000, matchDocIdsBS);

        tuple.put("tags", tags);
        tuple.put("tag_docs", doclist);

        return tuple;
    }

    @Override
    public StreamComparator getStreamSort() {
        return this.inputStream.getStreamSort();
    }

    @Override
    public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
        return toExpression(factory, true);
    }

    private StreamExpression toExpression(StreamFactory factory, boolean includeStreams) throws IOException {
        StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));

        if (includeStreams) {
            if (inputStream instanceof Expressible) {
                expression.addParameter(((Expressible)inputStream).toExpression(factory));
            } else {
                throw new IOException("This TagStream contains a non-expressible TupleStream - it cannot be converted to an expression");
            }
        }

        expression.addParameter(new StreamExpressionNamedParameter(OVERLAPS, overlapString));
        expression.addParameter(new StreamExpressionNamedParameter(TAGS_LIMIT, Integer.toString(tagsLimit)));
        expression.addParameter(new StreamExpressionNamedParameter(MATCH_TEXT, Boolean.toString(addMatchText)));
        expression.addParameter(new StreamExpressionNamedParameter(SKIP_ALT_TOKENS, Boolean.toString(skipAltTokens)));
        expression.addParameter(new StreamExpressionNamedParameter(INDEXED_FIELD, indexedField));
        expression.addParameter(new StreamExpressionNamedParameter(IGNORE_STOPWORDS, Boolean.toString(ignoreStopWords)));
        expression.addParameter(new StreamExpressionNamedParameter(XML_OFFSET_ADJUST, Boolean.toString(xmlOffsetAdjust)));
        expression.addParameter(new StreamExpressionNamedParameter(ZK_HOST, zkHost));
        expression.addParameter(new StreamExpressionNamedParameter(FIELD_TO_TAG, fieldToTag));


        return expression;

    }

    @Override
    public Explanation toExplanation(StreamFactory factory) throws IOException {
        StreamExplanation explanation = new StreamExplanation(getStreamNodeId().toString());

        explanation.setFunctionName(factory.getFunctionName(this.getClass()));
        explanation.setImplementingClass(this.getClass().getName());
        explanation.setExpressionType(Explanation.ExpressionType.STREAM_DECORATOR);
        explanation.setExpression(toExpression(factory, false).toString());

        explanation.addChild(inputStream.toExplanation(factory));

        return explanation;

    }

    private boolean fieldHasIndexedStopFilter(Analyzer indexAnalyzer) {
        if (indexAnalyzer instanceof TokenizerChain) {
            TokenizerChain tokenizerChain = (TokenizerChain) indexAnalyzer;
            TokenFilterFactory[] tokenFilterFactories = tokenizerChain.getTokenFilterFactories();
            for (TokenFilterFactory tokenFilterFactory : tokenFilterFactories) {
                if (tokenFilterFactory instanceof StopFilterFactory)
                    return true;
            }
        }
        return false;
    }

    private void buildTagger() throws IOException {
        var searcher = this.searcherHolder.get();
        Terms terms = searcher.getSlowAtomicReader().terms(indexedField);

        //TODO: remove inputString and figure out something sensible instead.
        String inputString = "";
        if (terms == null)
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                    "field " + indexedField + " has no indexed data");
        this.tagger = new ReusableTagger(terms, computeDocCorpus(), tagClusterReducer,
                skipAltTokens, ignoreStopWords) {
            @SuppressWarnings("unchecked")
            @Override
            protected void tagCallback(int startOffset, int endOffset, Object docIdsKey) {
                if (tags.size() >= tagsLimit)
                    return;
//                    if (offsetCorrector != null) {
//                        int[] offsetPair = offsetCorrector.correctPair(startOffset, endOffset);
//                        if (offsetPair == null) {
//                            return;
//                        }
//                        startOffset = offsetPair[0];
//                        endOffset = offsetPair[1];
//                    }
                Map tag_map = new HashMap();
                tag_map.put("startOffset", startOffset);
                tag_map.put("endOffset", endOffset);
                if (addMatchText) {
                    tag_map.put("matchText", inputString.substring(startOffset, endOffset));
                }
                //below caches, and also flags matchDocIdsBS
                tag_map.put("ids", lookupSchemaDocIds(docIdsKey));
                tags.add(tag_map);
            }

            Map<Object, List> docIdsListCache = new HashMap<>(2000);

            ValueSourceAccessor uniqueKeyCache = new ValueSourceAccessor(searcher,
                    idSchemaField.getType().getValueSource(idSchemaField, null));

            @SuppressWarnings("unchecked")
            private List lookupSchemaDocIds(Object docIdsKey) {
                List schemaDocIds = docIdsListCache.get(docIdsKey);
                if (schemaDocIds != null)
                    return schemaDocIds;
                IntsRef docIds = lookupDocIds(docIdsKey);
                //translate lucene docIds to schema ids
                schemaDocIds = new ArrayList(docIds.length);
                for (int i = docIds.offset; i < docIds.offset + docIds.length; i++) {
                    int docId = docIds.ints[i];
                    assert i == docIds.offset || docIds.ints[i - 1] < docId : "not sorted?";
                    matchDocIdsBS.set(docId);//also, flip docid in bitset
                    try {
                        schemaDocIds.add(uniqueKeyCache.objectVal(docId));//translates here
                    } catch (IOException e) {
                        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
                    }
                }
                assert !schemaDocIds.isEmpty();

                docIdsListCache.put(docIds, schemaDocIds);
                return schemaDocIds;
            }

        };
    }

    private DocList getDocList(int rows, FixedBitSet matchDocIdsBS) throws IOException {
        //Now we must supply a Solr DocList and add it to the response.
        //  Typically this is gotten via a SolrIndexSearcher.search(), but in this case we
        //  know exactly what documents to return, the order doesn't matter nor does
        //  scoring.
        //  Ideally an implementation of DocList could be directly implemented off
        //  of a BitSet, but there are way too many methods to implement for a minor
        //  payoff.
        int matchDocs = matchDocIdsBS.cardinality();
        int[] docIds = new int[ Math.min(rows, matchDocs) ];
        DocIdSetIterator docIdIter = new BitSetIterator(matchDocIdsBS, 1);
        for (int i = 0; i < docIds.length; i++) {
            docIds[i] = docIdIter.nextDoc();
        }
        return new DocSlice(0, docIds.length, docIds, null, matchDocs, 1f);
    }

    protected OffsetCorrector getOffsetCorrector(Callable<String> inputStringProvider) throws Exception {
        if (!xmlOffsetAdjust) {
            return null;
        }
        try {
            return new XmlOffsetCorrector(inputStringProvider.call());
        } catch (XMLStreamException e) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                    "Expecting XML but wasn't: " + e, e);
        }
    }

    private TagClusterReducer chooseTagClusterReducer(String overlaps) {
        TagClusterReducer tagClusterReducer;
        if (overlaps == null || overlaps.equals("NO_SUB")) {
            tagClusterReducer = TagClusterReducer.NO_SUB;
        } else if (overlaps.equals("ALL")) {
            tagClusterReducer = TagClusterReducer.ALL;
        } else if (overlaps.equals("LONGEST_DOMINANT_RIGHT")) {
            tagClusterReducer = TagClusterReducer.LONGEST_DOMINANT_RIGHT;
        } else {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                    "unknown tag overlap mode: "+overlaps);
        }
        return tagClusterReducer;
    }

    /**
     * The set of documents matching the provided 'fq' (filter query). Don't include deleted docs
     * either. If null is returned, then all docs are available.
     */
    private Bits computeDocCorpus() {
        return this.searcherHolder.get().getSlowAtomicReader().getLiveDocs();
//        final Bits docBits;
//        if (corpusFilterQueries != null && corpusFilterQueries.length > 0) {
//            throw new NotImplementedException("filter queries are not supported, yet");
//            List<Query> filterQueries = new ArrayList<Query>(corpusFilterQueries.length);
//            for (String corpusFilterQuery : corpusFilterQueries) {
//                // TODO: Construct Lucene Queries w/o having access to a SolrRequest object.
//                QParser qParser = QParser.getParser(corpusFilterQuery, null, req);
//
//                try {
//                    filterQueries.add(qParser.parse());
//                } catch (SyntaxError e) {
//                    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
//                }
//            }
//
//            final DocSet docSet = searcher.getDocSet(filterQueries);//hopefully in the cache
//            //note: before Solr 4.7 we could call docSet.getBits() but no longer.
//            if (docSet instanceof BitDocSet) {
//                docBits = ((BitDocSet)docSet).getBits();
//            } else {
//                docBits = new Bits() {
//
//                    @Override
//                    public boolean get(int index) {
//                        return docSet.exists(index);
//                    }
//
//                    @Override
//                    public int length() {
//                        return searcher.maxDoc();
//                    }
//                };
//            }
//        } else {
//            docBits = searcher.getSlowAtomicReader().getLiveDocs();
//        }
//        return docBits;
    }

    static class ValueSourceAccessor {
        private final List<LeafReaderContext> readerContexts;
        private final ValueSource valueSource;
        private final Map fContext;
        private final FunctionValues[] functionValuesPerSeg;
        private final int[] functionValuesDocIdPerSeg;

        ValueSourceAccessor(IndexSearcher searcher, ValueSource valueSource) {
            readerContexts = searcher.getIndexReader().leaves();
            this.valueSource = valueSource;
            fContext = ValueSource.newContext(searcher);
            functionValuesPerSeg = new FunctionValues[readerContexts.size()];
            functionValuesDocIdPerSeg = new int[readerContexts.size()];
        }

        Object objectVal(int topDocId) throws IOException {
            // lookup segment level stuff:
            int segIdx = ReaderUtil.subIndex(topDocId, readerContexts);
            LeafReaderContext rcontext = readerContexts.get(segIdx);
            int segDocId = topDocId - rcontext.docBase;
            // unfortunately Lucene 7.0 requires forward only traversal (with no reset method).
            //   So we need to track our last docId (per segment) and re-fetch the FunctionValues. :-(
            FunctionValues functionValues = functionValuesPerSeg[segIdx];
            if (functionValues == null || segDocId < functionValuesDocIdPerSeg[segIdx]) {
                functionValues = functionValuesPerSeg[segIdx] = valueSource.getValues(fContext, rcontext);
            }
            functionValuesDocIdPerSeg[segIdx] = segDocId;

            // get value:
            return functionValues.objectVal(segDocId);
        }
    }
    private String getString(String fieldName, DefaultFactory<String> defaultFactory) {
        var namedOperand = factory.getNamedOperand(expression, fieldName);
        if (null == namedOperand) {
            return defaultFactory.build();
        }
        return ((StreamExpressionValue)namedOperand.getParameter()).getValue();
    }
    private int getInt(String fieldName, DefaultFactory<Integer> defaultFactory) {
        final String intStr = getString(fieldName, () -> null);
        if (null == intStr) return defaultFactory.build();
        return Integer.parseInt(intStr);
    }
    private boolean getBool(String fieldName, DefaultFactory<Boolean> defaultFactory){
        final String boolStr = getString(fieldName, () -> null);
        if (null == boolStr) return defaultFactory.build();
        return Boolean.parseBoolean(boolStr);
    }



}
