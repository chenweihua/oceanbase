/**
 * Copyright (c) 2012 Taobao.com
 * All rights reserved.
 *
 * 文件名称：BinlogRecord.h
 * 摘要：通过Binlog解析出来的记录信息定义
 * 作者：Benkong <benkong@taobao.com>
 *       Jierui.lj <jierui.lj@taobao.com>
 * 日期：2012.3.13
 *       2012.8.9 统一框架新增格式化字段和BinlogRecord, Binlog的功能
 *       2013.5.22 beknong 去掉了依赖具体数据库的格式化操作
 */
#ifndef _BINLOG_RECORD_H_
#define _BINLOG_RECORD_H_

#include "ob_define.h"
#include "BR.h"

namespace oceanbase
{
namespace liboblog
{

struct BRInfo;

class BinlogRecord : public IBinlogRecord
{
public:
    /**
     * 构造一个记录
     * @param timestamp  时间戳
     * @param tbleMeta   相应表的描述信息
     */
    BinlogRecord(time_t timestamp, ITableMeta *tblMeta);

    /**
     * 构造一个只读记录
     * @param ptr   数据区
     * @param size  ptr所占字节数
     */
	BinlogRecord(const void *ptr, size_t size);
	BinlogRecord(bool creating=true);

    virtual ~BinlogRecord();

	/**
	 * 解析只读记录
     * @param ptr   数据区
     * @param size  ptr所占字节数
	 * @return 0:成功；<0: 失败
	 */
	int parse(const void *ptr, size_t size);

	/**
	 * 是否解析成功
	 * @return true/false
	 */
	bool parsedOK();

	/**
	 * 只读数据区实际有效的字节数，parse()的时候允许size比比有效数据长
	 * @return 字节数
	 */
	size_t getRealSize();

    /**
     * 增加一个更新前的column的值
     * @parm colName   column名称
     * @parm val       column的旧值
     * @return 0: 成功; <0: 失败
     */
    int putOld(std::string *val);

    /**
     * 增加一个更新后的column的值
     * @parm colName   column名称
     * @parm val       column的新值
     * @return 0: 成功; <0: 失败
     */
    int putNew(std::string *val);

    /**
     * 获取更新前column和值的映射表
     * @return column name => column value
     */
    const std::vector<std::string *>& oldCols();
	IStrArray* parsedOldCols() const;

    /**
     * 获取更新后column和值的映射表
     * @return column name => column value
     */
    const std::vector<std::string *>& newCols();
	IStrArray* parsedNewCols() const;

    IStrArray* parsedColNames() const;

    const uint8_t *parsedColTypes() const;

    void setInstance(const char *instance);
    const char* instance() const;

	void setDbname(const char *dbname);
	const char *dbname() const;

    void setTbname(const char *tbname);
    const char* tbname() const;

    void setPKValue(const char *pkValue);
	const char* pkValue() const;

    bool conflicts(const IBinlogRecord *);
    bool conflicts(const BinlogRecord *);

    /**
     * 序列化为字符串
     * @return 非NULL: 成功; NULL: 失败
     */
    std::string* toParsedString();
	const std::string* toString();

	void clear();
    void clearNew();
    void clearOld();

    bool isAnyChanged() const;
    bool isTxnEnded() const;

    int recordType();
    int setRecordType(int aType);

	const char* recordEncoding();

    void setTimestamp(time_t timestamp);
    time_t getTimestamp();

	void setCheckpoint(unsigned file, unsigned offset);
    const char* getCheckpoint();
    unsigned getCheckpoint1();
    unsigned getCheckpoint2();
    unsigned getFileNameOffset();
    unsigned getFileOffset();

	void setLastInLogevent(bool b);
	bool lastInLogevent();

    void setId(uint64_t id);
    uint64_t id();

	void setTableMeta(ITableMeta *tblMeta);
    ITableMeta* getTableMeta();

    int getSrcType() const;

	long* getTimemark(size_t& length) {/*TODO*/ UNUSED(length); return NULL;};

	void addTimemark(long time) {/*TODO*/ UNUSED(time);};

private:
	BRInfo *m_br;
    std::string m_buf;
};

}
}

#endif

