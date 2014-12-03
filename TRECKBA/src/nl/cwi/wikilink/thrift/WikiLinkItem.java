/**
 * Autogenerated by Thrift Compiler (0.8.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package nl.cwi.wikilink.thrift;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikiLinkItem implements org.apache.thrift.TBase<WikiLinkItem, WikiLinkItem._Fields>, java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("WikiLinkItem");

  private static final org.apache.thrift.protocol.TField DOC_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("doc_id", org.apache.thrift.protocol.TType.I32, (short)1);
  private static final org.apache.thrift.protocol.TField URL_FIELD_DESC = new org.apache.thrift.protocol.TField("url", org.apache.thrift.protocol.TType.STRING, (short)2);
  private static final org.apache.thrift.protocol.TField CONTENT_FIELD_DESC = new org.apache.thrift.protocol.TField("content", org.apache.thrift.protocol.TType.STRUCT, (short)3);
  private static final org.apache.thrift.protocol.TField RARE_WORDS_FIELD_DESC = new org.apache.thrift.protocol.TField("rare_words", org.apache.thrift.protocol.TType.LIST, (short)4);
  private static final org.apache.thrift.protocol.TField MENTIONS_FIELD_DESC = new org.apache.thrift.protocol.TField("mentions", org.apache.thrift.protocol.TType.LIST, (short)5);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new WikiLinkItemStandardSchemeFactory());
    schemes.put(TupleScheme.class, new WikiLinkItemTupleSchemeFactory());
  }

  public int doc_id; // required
  public String url; // required
  public PageContentItem content; // required
  public List<RareWord> rare_words; // required
  public List<Mention> mentions; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    DOC_ID((short)1, "doc_id"),
    URL((short)2, "url"),
    CONTENT((short)3, "content"),
    RARE_WORDS((short)4, "rare_words"),
    MENTIONS((short)5, "mentions");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // DOC_ID
          return DOC_ID;
        case 2: // URL
          return URL;
        case 3: // CONTENT
          return CONTENT;
        case 4: // RARE_WORDS
          return RARE_WORDS;
        case 5: // MENTIONS
          return MENTIONS;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __DOC_ID_ISSET_ID = 0;
  private BitSet __isset_bit_vector = new BitSet(1);
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.DOC_ID, new org.apache.thrift.meta_data.FieldMetaData("doc_id", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.URL, new org.apache.thrift.meta_data.FieldMetaData("url", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.CONTENT, new org.apache.thrift.meta_data.FieldMetaData("content", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, PageContentItem.class)));
    tmpMap.put(_Fields.RARE_WORDS, new org.apache.thrift.meta_data.FieldMetaData("rare_words", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, RareWord.class))));
    tmpMap.put(_Fields.MENTIONS, new org.apache.thrift.meta_data.FieldMetaData("mentions", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, Mention.class))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(WikiLinkItem.class, metaDataMap);
  }

  public WikiLinkItem() {
  }

  public WikiLinkItem(
    int doc_id,
    String url,
    PageContentItem content,
    List<RareWord> rare_words,
    List<Mention> mentions)
  {
    this();
    this.doc_id = doc_id;
    setDoc_idIsSet(true);
    this.url = url;
    this.content = content;
    this.rare_words = rare_words;
    this.mentions = mentions;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public WikiLinkItem(WikiLinkItem other) {
    __isset_bit_vector.clear();
    __isset_bit_vector.or(other.__isset_bit_vector);
    this.doc_id = other.doc_id;
    if (other.isSetUrl()) {
      this.url = other.url;
    }
    if (other.isSetContent()) {
      this.content = new PageContentItem(other.content);
    }
    if (other.isSetRare_words()) {
      List<RareWord> __this__rare_words = new ArrayList<RareWord>();
      for (RareWord other_element : other.rare_words) {
        __this__rare_words.add(new RareWord(other_element));
      }
      this.rare_words = __this__rare_words;
    }
    if (other.isSetMentions()) {
      List<Mention> __this__mentions = new ArrayList<Mention>();
      for (Mention other_element : other.mentions) {
        __this__mentions.add(new Mention(other_element));
      }
      this.mentions = __this__mentions;
    }
  }

  public WikiLinkItem deepCopy() {
    return new WikiLinkItem(this);
  }

  @Override
  public void clear() {
    setDoc_idIsSet(false);
    this.doc_id = 0;
    this.url = null;
    this.content = null;
    this.rare_words = null;
    this.mentions = null;
  }

  public int getDoc_id() {
    return this.doc_id;
  }

  public WikiLinkItem setDoc_id(int doc_id) {
    this.doc_id = doc_id;
    setDoc_idIsSet(true);
    return this;
  }

  public void unsetDoc_id() {
    __isset_bit_vector.clear(__DOC_ID_ISSET_ID);
  }

  /** Returns true if field doc_id is set (has been assigned a value) and false otherwise */
  public boolean isSetDoc_id() {
    return __isset_bit_vector.get(__DOC_ID_ISSET_ID);
  }

  public void setDoc_idIsSet(boolean value) {
    __isset_bit_vector.set(__DOC_ID_ISSET_ID, value);
  }

  public String getUrl() {
    return this.url;
  }

  public WikiLinkItem setUrl(String url) {
    this.url = url;
    return this;
  }

  public void unsetUrl() {
    this.url = null;
  }

  /** Returns true if field url is set (has been assigned a value) and false otherwise */
  public boolean isSetUrl() {
    return this.url != null;
  }

  public void setUrlIsSet(boolean value) {
    if (!value) {
      this.url = null;
    }
  }

  public PageContentItem getContent() {
    return this.content;
  }

  public WikiLinkItem setContent(PageContentItem content) {
    this.content = content;
    return this;
  }

  public void unsetContent() {
    this.content = null;
  }

  /** Returns true if field content is set (has been assigned a value) and false otherwise */
  public boolean isSetContent() {
    return this.content != null;
  }

  public void setContentIsSet(boolean value) {
    if (!value) {
      this.content = null;
    }
  }

  public int getRare_wordsSize() {
    return (this.rare_words == null) ? 0 : this.rare_words.size();
  }

  public java.util.Iterator<RareWord> getRare_wordsIterator() {
    return (this.rare_words == null) ? null : this.rare_words.iterator();
  }

  public void addToRare_words(RareWord elem) {
    if (this.rare_words == null) {
      this.rare_words = new ArrayList<RareWord>();
    }
    this.rare_words.add(elem);
  }

  public List<RareWord> getRare_words() {
    return this.rare_words;
  }

  public WikiLinkItem setRare_words(List<RareWord> rare_words) {
    this.rare_words = rare_words;
    return this;
  }

  public void unsetRare_words() {
    this.rare_words = null;
  }

  /** Returns true if field rare_words is set (has been assigned a value) and false otherwise */
  public boolean isSetRare_words() {
    return this.rare_words != null;
  }

  public void setRare_wordsIsSet(boolean value) {
    if (!value) {
      this.rare_words = null;
    }
  }

  public int getMentionsSize() {
    return (this.mentions == null) ? 0 : this.mentions.size();
  }

  public java.util.Iterator<Mention> getMentionsIterator() {
    return (this.mentions == null) ? null : this.mentions.iterator();
  }

  public void addToMentions(Mention elem) {
    if (this.mentions == null) {
      this.mentions = new ArrayList<Mention>();
    }
    this.mentions.add(elem);
  }

  public List<Mention> getMentions() {
    return this.mentions;
  }

  public WikiLinkItem setMentions(List<Mention> mentions) {
    this.mentions = mentions;
    return this;
  }

  public void unsetMentions() {
    this.mentions = null;
  }

  /** Returns true if field mentions is set (has been assigned a value) and false otherwise */
  public boolean isSetMentions() {
    return this.mentions != null;
  }

  public void setMentionsIsSet(boolean value) {
    if (!value) {
      this.mentions = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case DOC_ID:
      if (value == null) {
        unsetDoc_id();
      } else {
        setDoc_id((Integer)value);
      }
      break;

    case URL:
      if (value == null) {
        unsetUrl();
      } else {
        setUrl((String)value);
      }
      break;

    case CONTENT:
      if (value == null) {
        unsetContent();
      } else {
        setContent((PageContentItem)value);
      }
      break;

    case RARE_WORDS:
      if (value == null) {
        unsetRare_words();
      } else {
        setRare_words((List<RareWord>)value);
      }
      break;

    case MENTIONS:
      if (value == null) {
        unsetMentions();
      } else {
        setMentions((List<Mention>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case DOC_ID:
      return Integer.valueOf(getDoc_id());

    case URL:
      return getUrl();

    case CONTENT:
      return getContent();

    case RARE_WORDS:
      return getRare_words();

    case MENTIONS:
      return getMentions();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case DOC_ID:
      return isSetDoc_id();
    case URL:
      return isSetUrl();
    case CONTENT:
      return isSetContent();
    case RARE_WORDS:
      return isSetRare_words();
    case MENTIONS:
      return isSetMentions();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof WikiLinkItem)
      return this.equals((WikiLinkItem)that);
    return false;
  }

  public boolean equals(WikiLinkItem that) {
    if (that == null)
      return false;

    boolean this_present_doc_id = true;
    boolean that_present_doc_id = true;
    if (this_present_doc_id || that_present_doc_id) {
      if (!(this_present_doc_id && that_present_doc_id))
        return false;
      if (this.doc_id != that.doc_id)
        return false;
    }

    boolean this_present_url = true && this.isSetUrl();
    boolean that_present_url = true && that.isSetUrl();
    if (this_present_url || that_present_url) {
      if (!(this_present_url && that_present_url))
        return false;
      if (!this.url.equals(that.url))
        return false;
    }

    boolean this_present_content = true && this.isSetContent();
    boolean that_present_content = true && that.isSetContent();
    if (this_present_content || that_present_content) {
      if (!(this_present_content && that_present_content))
        return false;
      if (!this.content.equals(that.content))
        return false;
    }

    boolean this_present_rare_words = true && this.isSetRare_words();
    boolean that_present_rare_words = true && that.isSetRare_words();
    if (this_present_rare_words || that_present_rare_words) {
      if (!(this_present_rare_words && that_present_rare_words))
        return false;
      if (!this.rare_words.equals(that.rare_words))
        return false;
    }

    boolean this_present_mentions = true && this.isSetMentions();
    boolean that_present_mentions = true && that.isSetMentions();
    if (this_present_mentions || that_present_mentions) {
      if (!(this_present_mentions && that_present_mentions))
        return false;
      if (!this.mentions.equals(that.mentions))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  public int compareTo(WikiLinkItem other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    WikiLinkItem typedOther = (WikiLinkItem)other;

    lastComparison = Boolean.valueOf(isSetDoc_id()).compareTo(typedOther.isSetDoc_id());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetDoc_id()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.doc_id, typedOther.doc_id);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetUrl()).compareTo(typedOther.isSetUrl());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetUrl()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.url, typedOther.url);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetContent()).compareTo(typedOther.isSetContent());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetContent()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.content, typedOther.content);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetRare_words()).compareTo(typedOther.isSetRare_words());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetRare_words()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.rare_words, typedOther.rare_words);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetMentions()).compareTo(typedOther.isSetMentions());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetMentions()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.mentions, typedOther.mentions);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("WikiLinkItem(");
    boolean first = true;

    sb.append("doc_id:");
    sb.append(this.doc_id);
    first = false;
    if (!first) sb.append(", ");
    sb.append("url:");
    if (this.url == null) {
      sb.append("null");
    } else {
      sb.append(this.url);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("content:");
    if (this.content == null) {
      sb.append("null");
    } else {
      sb.append(this.content);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("rare_words:");
    if (this.rare_words == null) {
      sb.append("null");
    } else {
      sb.append(this.rare_words);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("mentions:");
    if (this.mentions == null) {
      sb.append("null");
    } else {
      sb.append(this.mentions);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bit_vector = new BitSet(1);
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class WikiLinkItemStandardSchemeFactory implements SchemeFactory {
    public WikiLinkItemStandardScheme getScheme() {
      return new WikiLinkItemStandardScheme();
    }
  }

  private static class WikiLinkItemStandardScheme extends StandardScheme<WikiLinkItem> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, WikiLinkItem struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // DOC_ID
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.doc_id = iprot.readI32();
              struct.setDoc_idIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // URL
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.url = iprot.readString();
              struct.setUrlIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // CONTENT
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.content = new PageContentItem();
              struct.content.read(iprot);
              struct.setContentIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // RARE_WORDS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list0 = iprot.readListBegin();
                struct.rare_words = new ArrayList<RareWord>(_list0.size);
                for (int _i1 = 0; _i1 < _list0.size; ++_i1)
                {
                  RareWord _elem2; // required
                  _elem2 = new RareWord();
                  _elem2.read(iprot);
                  struct.rare_words.add(_elem2);
                }
                iprot.readListEnd();
              }
              struct.setRare_wordsIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 5: // MENTIONS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list3 = iprot.readListBegin();
                struct.mentions = new ArrayList<Mention>(_list3.size);
                for (int _i4 = 0; _i4 < _list3.size; ++_i4)
                {
                  Mention _elem5; // required
                  _elem5 = new Mention();
                  _elem5.read(iprot);
                  struct.mentions.add(_elem5);
                }
                iprot.readListEnd();
              }
              struct.setMentionsIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, WikiLinkItem struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(DOC_ID_FIELD_DESC);
      oprot.writeI32(struct.doc_id);
      oprot.writeFieldEnd();
      if (struct.url != null) {
        oprot.writeFieldBegin(URL_FIELD_DESC);
        oprot.writeString(struct.url);
        oprot.writeFieldEnd();
      }
      if (struct.content != null) {
        oprot.writeFieldBegin(CONTENT_FIELD_DESC);
        struct.content.write(oprot);
        oprot.writeFieldEnd();
      }
      if (struct.rare_words != null) {
        oprot.writeFieldBegin(RARE_WORDS_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.rare_words.size()));
          for (RareWord _iter6 : struct.rare_words)
          {
            _iter6.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.mentions != null) {
        oprot.writeFieldBegin(MENTIONS_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.mentions.size()));
          for (Mention _iter7 : struct.mentions)
          {
            _iter7.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class WikiLinkItemTupleSchemeFactory implements SchemeFactory {
    public WikiLinkItemTupleScheme getScheme() {
      return new WikiLinkItemTupleScheme();
    }
  }

  private static class WikiLinkItemTupleScheme extends TupleScheme<WikiLinkItem> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, WikiLinkItem struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetDoc_id()) {
        optionals.set(0);
      }
      if (struct.isSetUrl()) {
        optionals.set(1);
      }
      if (struct.isSetContent()) {
        optionals.set(2);
      }
      if (struct.isSetRare_words()) {
        optionals.set(3);
      }
      if (struct.isSetMentions()) {
        optionals.set(4);
      }
      oprot.writeBitSet(optionals, 5);
      if (struct.isSetDoc_id()) {
        oprot.writeI32(struct.doc_id);
      }
      if (struct.isSetUrl()) {
        oprot.writeString(struct.url);
      }
      if (struct.isSetContent()) {
        struct.content.write(oprot);
      }
      if (struct.isSetRare_words()) {
        {
          oprot.writeI32(struct.rare_words.size());
          for (RareWord _iter8 : struct.rare_words)
          {
            _iter8.write(oprot);
          }
        }
      }
      if (struct.isSetMentions()) {
        {
          oprot.writeI32(struct.mentions.size());
          for (Mention _iter9 : struct.mentions)
          {
            _iter9.write(oprot);
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, WikiLinkItem struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(5);
      if (incoming.get(0)) {
        struct.doc_id = iprot.readI32();
        struct.setDoc_idIsSet(true);
      }
      if (incoming.get(1)) {
        struct.url = iprot.readString();
        struct.setUrlIsSet(true);
      }
      if (incoming.get(2)) {
        struct.content = new PageContentItem();
        struct.content.read(iprot);
        struct.setContentIsSet(true);
      }
      if (incoming.get(3)) {
        {
          org.apache.thrift.protocol.TList _list10 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
          struct.rare_words = new ArrayList<RareWord>(_list10.size);
          for (int _i11 = 0; _i11 < _list10.size; ++_i11)
          {
            RareWord _elem12; // required
            _elem12 = new RareWord();
            _elem12.read(iprot);
            struct.rare_words.add(_elem12);
          }
        }
        struct.setRare_wordsIsSet(true);
      }
      if (incoming.get(4)) {
        {
          org.apache.thrift.protocol.TList _list13 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
          struct.mentions = new ArrayList<Mention>(_list13.size);
          for (int _i14 = 0; _i14 < _list13.size; ++_i14)
          {
            Mention _elem15; // required
            _elem15 = new Mention();
            _elem15.read(iprot);
            struct.mentions.add(_elem15);
          }
        }
        struct.setMentionsIsSet(true);
      }
    }
  }

}

