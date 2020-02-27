package de.einsweniger.solr;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.Bits;
import org.apache.solr.handler.tagger.TaggingAttribute;

import java.io.IOException;

public abstract class ReusableTagger extends Tagger {

    public ReusableTagger(Terms terms, Bits liveDocs, TagClusterReducer tagClusterReducer, boolean skipAltTokens, boolean ignoreStopWords) throws IOException {
        super(terms, liveDocs, tagClusterReducer, skipAltTokens, ignoreStopWords);
    }

    @Override
    public void process() throws IOException {
        super.process();
        if (null != this.tokenStream) {
            this.tokenStream.close();
        }
    }

    public void reset(TokenStream tokenStream) throws IOException {
        this.tokenStream = tokenStream;
        byteRefAtt = tokenStream.addAttribute(TermToBytesRefAttribute.class);
        posIncAtt = tokenStream.addAttribute(PositionIncrementAttribute.class);
        offsetAtt = tokenStream.addAttribute(OffsetAttribute.class);
        taggingAtt = tokenStream.addAttribute(TaggingAttribute.class);
        tokenStream.reset();
    }
}
