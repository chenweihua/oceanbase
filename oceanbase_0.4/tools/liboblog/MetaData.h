/**
 * Copyright (c) 2012 Taobao.com
 * All rights reserved.
 *
 * 文件名称：MetaData.h
 * 摘要：db、table、column的meta信息存取接口
 * 作者：Benkong <benkong@taobao.com>
 * 日期：2012.5.22
 */
#ifndef _META_DATA_H_
#define _META_DATA_H_

#include "MD.h"

namespace oceanbase
{
namespace liboblog
{

// column的描述信息
struct ColMetaInfo;
class ColMeta : public IColMeta
{
public:
	ColMeta();
	ColMeta(const void *ptr, size_t size);
	virtual ~ColMeta();

public:
	// override
	virtual const char *getName();
	virtual int getType();
	virtual long getLength();
	virtual bool isSigned();
	virtual bool isPK();
	virtual bool isNotNull();
	virtual int getDecimals();
	virtual const char* getDefault();
	virtual const char* getEncoding();
	virtual int getRequired();
	virtual IStrArray* getValuesOfEnumSet();

	virtual void setName(const char* name);
	virtual void setType(int type);
	virtual void setLength(long length);
	virtual void setSigned(bool b);
	virtual void setIsPK(bool b);
	virtual void setNotNull(bool b);
	virtual void setDecimals(int decimals);
	virtual void setDefault(const char* def);
	virtual void setEncoding(const char* enc);
	virtual void setRequired(int required);
	virtual void setValuesOfEnumSet(std::vector<std::string> &v);
	virtual void setValuesOfEnumSet(std::vector<const char*> &v);
	virtual void setValuesOfEnumSet(const char** v, size_t size);

public:
	int appendTo(std::string &s);
	size_t getRealSize();
	int parse(const void *ptr, size_t size);
	bool parsedOK();
private:
	ColMetaInfo *m_col;
};

// table的描述信息
struct TableMetaInfo;
class TableMeta : public ITableMeta
{
public:
	TableMeta();
	TableMeta(const void *ptr, size_t size);
	virtual ~TableMeta();

public:
	// override
	virtual const char* getName();
	virtual bool hasPK();
	virtual const char* getPKs();
	virtual const char* getEncoding();
	virtual IDBMeta* getDBMeta();

	virtual void setName(const char* name);
	virtual void setHasPK(bool b);
	virtual void setPKs(const char* pks);
	virtual void setEncoding(const char* enc);
	virtual void setDBMeta(IDBMeta* dbMeta);

public:

    /**
     * 获得所有字段名
     */
    std::vector<std::string> &getColNames();

    /**
     * PK column名字
     */
    std::vector<std::string> &getPKColNames();

    /**
	 * 通过column的名字获取某个column的meta信息
	 * @param colName   column的名字
	 * @return NULL: 没有colName对应的描述信息; 非NULL: colName对应的描述信息
	 */
	virtual IColMeta* getCol(const char* colName);

	/**
	 * 获取Column数目
	 * @return Column数目
	 */
	virtual int getColCount();

	/*
	 * 通过column在mysql中的内部序号得到某个column的meta信息
	 *
	 * @param index column的序列号
	 * @return NULL: 没有colName对应的描述信息; 非NULL: colName对应的描述信息
	 */
	virtual IColMeta* getCol(int index);
	virtual int getColNum(const char* colName);

	/**
	 * 追加一个column的描述信息
	 * @param colName  column名称
	 * @param colMeta  column的描述信息
	 * @return 0: 成功； <0: 失败
	 */
	virtual int append(const char* colName, IColMeta *colMeta);

public:
	int appendTo(std::string &s);
	size_t getRealSize();
	int parse(const void *ptr, size_t size);
	bool parsedOK();
private:
    TableMetaInfo *m_tbl;
};

// DB的描述信息
struct DBMetaInfo;
class DBMeta : public IDBMeta
{
public:
	DBMeta();
	DBMeta(const void *ptr, size_t size);
	virtual ~DBMeta();

public:
	// override
	virtual const char* getName();
	virtual const char* getEncoding();
	virtual IMetaDataCollections* getMetaDataCollections();

	virtual void setName(const char* name);
	virtual void setEncoding(const char* enc);
	virtual void setMetaDataCollections(IMetaDataCollections *mdc);

    virtual int getTblCount();
	/**
	 * 获取指定table的描述信息
	 * @param tblName  table名称
	 * @return NULL: 没有tblName对应的描述信息; 非NULL: tblName对应的描述信息
	 */
	virtual ITableMeta* get(const char* tblName);
    virtual ITableMeta* get(int index);

	/**
	 * 加入一个table描述信息
	 * @param tblName  table名称
	 * @param tblMeta  table描述信息
	 * @return 0: 成功; <0: 失败
	 */
	virtual int put(const char* tblName, ITableMeta *tblMeta);
public:
	int appendTo(std::string &s);
	size_t getRealSize();
	int parse(const void *ptr, size_t size);
	bool parsedOK();
private:
	DBMetaInfo *m_db;
};

// meta data集合，包括所有的db的描述信息
struct MetaDataCollectionInfo;
class MetaDataCollections : public IMetaDataCollections
{
public:
	MetaDataCollections();
	MetaDataCollections(const void *ptr, size_t size, bool removePtr=false); // removePtr表示是否需要在析构时释放ptr
	virtual ~MetaDataCollections();

public:
	// override
	virtual unsigned getMetaVerNum();
	virtual IMetaDataCollections* getPrev();
	virtual time_t getTimestamp();

	virtual void setMetaVerNum(unsigned metaVerNum);
	virtual void setPrev(IMetaDataCollections* prev);
	virtual void setTimestamp(time_t timestamp);

    /**
     * 获取Db的数目
     * @return Db的数目
     */
    virtual int getDbCount();

    /**
     * 获取指定名称的Db描述信息
     * @param dbname 指定名称
     * @return 指定Db的描述信息
     */
    virtual IDBMeta *get(const char* dbname);

    /**
     * 获取指定位置的Db描述信息
     * @param index 指定位置
     * @return 指定Db的描述信息
     */
    virtual IDBMeta *get(int index);

    /**
	 * 获取指定table的描述信息
	 * @param dbName   db名称
	 * @param tblName  table名称
	 * @return NULL: 没有tblName对应的描述信息; 非NULL: tblName对应的描述信息
	 */
	virtual ITableMeta* get(const char* dbName, const char* tblName);

	/**
	 * 加入一个db描述信息
	 * @param dbName  db名称
	 * @param dbMeta  db描述信息
	 * @return 0: 成功; <0: 失败
	 */
	virtual int put(const char* dbName, IDBMeta *dbMeta);

    /**
     * 序列化为字符串
     * @param s  保存序列化后的结果
     * @return 0: 成功; <0: 失败
     */
	virtual int toString(std::string& s);

	/**
	 * 解析只读记录
     * @param ptr   数据区
     * @param size  ptr所占字节数
	 * @return 0:成功；<0: 失败
	 */
	virtual int parse(const void* ptr, size_t size);

	/**
	 * 解析是否成功
	 * @return true/false
	 */
	virtual bool parsedOK();

	/**
	 * 只读数据区实际有效的字节数，parse()的时候允许size比比有效数据长
	 * @return 字节数
	 */
	virtual size_t getRealSize();
private:
	MetaDataCollectionInfo *m_coll;
};

}
}

#endif

