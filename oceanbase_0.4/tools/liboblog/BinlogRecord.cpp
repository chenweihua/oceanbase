/*=============================================================================
#
# Author: liujie - jierui.lj@taobao.com
#
# Wangwang : 杰睿
#
# Last modified: 2012-08-09 15:09
#                2013.5.22 benkong 数据存取支持流式数据，去掉了与具体数据库的依赖
#
# Filename: BinlogRecord.cpp
#
# Description:
#
=============================================================================*/
#include "BinlogRecord.h"
#include "MsgVarArea.h"
#include "MD.h"
#include <stdint.h>
#include <string.h>

namespace oceanbase
{
namespace liboblog
{

typedef std::vector<std::string*> ColMap;

const uint16_t BR_VERSION = 1; //版本号
const uint64_t BR_INVALID_ID = 0;

// BinlogRecord头信息, sizeof(int32_t)的倍数
struct BRHeader
{
	uint8_t   m_brVersion;
    uint8_t   m_srcType;
	uint8_t   m_op;
	uint8_t   m_lastInLogevent;
    uint32_t  m_encoding;
    uint32_t  m_instanceOffset;
    uint32_t  m_timemarkOffset;
	uint64_t  m_id;
	uint64_t  m_timestamp;
	uint32_t  m_dbNameOffset;
    uint32_t  m_tbNameOffset;
    uint32_t  m_colNamesOffset;
    uint32_t  m_colTypesOffset;
	uint32_t  m_fileNameOffset;
	uint32_t  m_fileOffset;
	uint32_t  m_oldColsOffset;
	uint32_t  m_newColsOffset;
    uint32_t  m_pkValOffset;
};

struct BRInfo
{
    static const char *ddlName;
    static const char *heartbeatName;
    static uint8_t ddlType[1];
    static uint8_t heartbeatType[1];

    bool m_creatingMode;
	bool m_parsedOK;
	BRHeader     *m_brHeader;
	MsgVarArea   *m_brDataArea;

#define MAX_CP_BUF 48
	char m_cpBuf[MAX_CP_BUF];

    ITableMeta *m_tblMeta;    // table的描述信息
    std::vector<std::string *> m_old_cols;
    std::vector<std::string *> m_new_cols;

	BRInfo(time_t timestamp, ITableMeta *tblMeta)
		: m_creatingMode(true), m_parsedOK(false), m_brHeader(NULL), m_brDataArea(NULL), m_tblMeta(tblMeta)
	{
		m_brHeader = new BRHeader;
		m_brDataArea = new MsgVarArea();
		m_brDataArea->appendArray((uint8_t*)m_brHeader, sizeof(BRHeader));

		memset(m_brHeader, -1, sizeof(BRHeader));
		m_brHeader->m_brVersion = BR_VERSION;
        m_brHeader->m_id = BR_INVALID_ID;
		m_brHeader->m_timestamp = timestamp;
		m_brHeader->m_lastInLogevent = false;
        m_brHeader->m_srcType = SRC_OCEANBASE;
	}

	BRInfo(const void *ptr, size_t size)
        : m_creatingMode(false), m_parsedOK(false), m_brHeader(NULL), m_brDataArea(NULL), m_tblMeta(NULL)
	{
		m_brDataArea   = new MsgVarArea(false);
		parse(ptr, size);
	}

	BRInfo(bool creating) : m_creatingMode(creating), m_parsedOK(false), m_brHeader(NULL), m_brDataArea(NULL), m_tblMeta(NULL)
	{
		if (creating)
		{
			m_brHeader = new BRHeader;
			m_brDataArea = new MsgVarArea();
			m_brDataArea->appendArray((uint8_t*)m_brHeader, sizeof(BRHeader));
			memset(m_brHeader, -1, sizeof(BRHeader));
			m_brHeader->m_brVersion = BR_VERSION;
			m_brHeader->m_lastInLogevent = false;
            m_brHeader->m_id = BR_INVALID_ID;
            m_brHeader->m_srcType = SRC_OCEANBASE;
		}
		else
		{
			m_brDataArea   = new MsgVarArea(false);
		}
	}

	~BRInfo()
	{
        clearOld();
        clearNew();
		if (m_creatingMode && m_brHeader != NULL)
			delete m_brHeader;
		if (m_brDataArea != NULL)
			delete m_brDataArea;
	}

	void clear()
	{
		if (m_creatingMode)
		{
			m_brDataArea->clear();
			m_brDataArea->appendArray((uint8_t*)m_brHeader, sizeof(BRHeader));
			memset(m_brHeader, -1, sizeof(BRHeader));
			m_brHeader->m_brVersion = BR_VERSION;
			m_brHeader->m_id = BR_INVALID_ID;
			m_brHeader->m_lastInLogevent = false;
            m_brHeader->m_srcType = SRC_OCEANBASE;

			m_old_cols.clear();
			m_new_cols.clear();
		}
	}

	int parse(const void *ptr, size_t size)
	{
		if (m_creatingMode)
			return -1;

		m_brDataArea->clear();
		if (0 != m_brDataArea->parse(ptr, size))
			return -2;

		const void *v;
		size_t elSize, count;
		int ret = m_brDataArea->getArray(0, v, elSize, count);
		if (ret != 0)
			return -3;
		if (elSize != sizeof(uint8_t) || count != sizeof(BRHeader))
			return -4;

		m_brHeader = (BRHeader*)v;
		m_parsedOK = true;
		return 0;
	}

	size_t getRealSize()
	{
		if (m_parsedOK)
			return m_brDataArea->getRealSize();
		return 0;
	}

	void clearNew()
	{
		ColMap::iterator it;
		for (it = m_new_cols.begin(); it != m_new_cols.end(); it ++)
		{
			if (*it)
				delete *it;
		}
		m_new_cols.clear();
	}

    void clearOld()
	{
		ColMap::iterator it;
		for (it = m_old_cols.begin(); it != m_old_cols.end(); it ++)
		{
			if (*it)
				delete *it;
		}
		m_old_cols.clear();
	}

	int putOld(std::string *val)
	{
		m_old_cols.push_back(val);
		return 0;
	}

	int putNew(std::string *val)
	{
		m_new_cols.push_back(val);
		return 0;
	}

	IStrArray* oldCols() const
	{
		if (m_parsedOK)
		{
			return m_brDataArea->getStringArray(m_brHeader->m_oldColsOffset);
		}
		return NULL;
	}

	IStrArray* newCols() const
	{
		if (m_parsedOK)
		{
			return m_brDataArea->getStringArray(m_brHeader->m_newColsOffset);
		}
		return NULL;
	}

	IStrArray* colNames() const
	{
		if (m_parsedOK)
		{
			return m_brDataArea->getStringArray(m_brHeader->m_colNamesOffset);
		}
		return NULL;
	}

    const uint8_t* colTypes() const
    {
        if (m_parsedOK)
        {
            const void *v;
		    size_t elSize, count;
    		int ret = m_brDataArea->getArray(m_brHeader->m_colTypesOffset, v, elSize, count);
            if (ret != 0 || elSize != sizeof(uint8_t))
                return NULL;
            return (uint8_t *)v;
        }
        return NULL;
    }

    const std::string* toString()
	{
		if (m_creatingMode)
		{
            if (m_brHeader->m_op <= EREPLACE) // insert|delete|update/REPLACE
            {
                if (m_tblMeta == NULL)
                    return NULL;

    		    m_brHeader->m_colNamesOffset = (uint32_t)m_brDataArea->appendStringArray(m_tblMeta->getColNames());
		        m_brHeader->m_pkValOffset = (uint32_t)m_brDataArea->appendString(m_tblMeta->getPKs());
                m_brHeader->m_encoding = (uint32_t)m_brDataArea->appendString(m_tblMeta->getEncoding());

                int size = m_tblMeta->getColCount();
                uint8_t *col_types = new uint8_t[size];
                for (int i = 0; i < size; i ++)
                    col_types[i] = (uint8_t)(m_tblMeta->getCol(i)->getType());
                m_brHeader->m_colTypesOffset = (uint32_t)m_brDataArea->appendArray(col_types, size);
                delete []col_types;
            }
            else if (m_brHeader->m_op == EDDL)
            {
    		    m_brHeader->m_colNamesOffset = (uint32_t)m_brDataArea->appendStringArray(&ddlName, 1);
                m_brHeader->m_colTypesOffset = (uint32_t)m_brDataArea->appendArray(ddlType, 1);
                m_brHeader->m_encoding = (uint32_t)m_brDataArea->appendString("US-ASCII");
            }
            else if (m_brHeader->m_op == HEARTBEAT)
            {
    		    // m_brHeader->m_colNamesOffset = m_brDataArea->appendStringArray(&heartbeatName, 1);
                // m_brHeader->m_colTypesOffset = m_brDataArea->appendArray(heartbeatType, 1);
                m_brHeader->m_encoding = (uint32_t)m_brDataArea->appendString("US-ASCII");
            }
            else
            {
                m_brHeader->m_encoding = (uint32_t)m_brDataArea->appendString("US-ASCII");
            }

            m_brHeader->m_oldColsOffset  = (uint32_t)m_brDataArea->appendStringArray(m_old_cols);
			m_brHeader->m_newColsOffset  = (uint32_t)m_brDataArea->appendStringArray(m_new_cols);
			// replace header
			const std::string &m = m_brDataArea->getMessage();
			const void *v;
			size_t elSize, count;
			int ret = m_brDataArea->getArray(0, v, elSize, count);
			if (ret != 0 || elSize != sizeof(uint8_t) || count != sizeof(BRHeader))
				return NULL;
			memcpy((void*)v, m_brHeader, sizeof(BRHeader));
			m_parsedOK = true; // to support fetching
			return &m;
		}
		return NULL;
	}

    std::string* toParsedString()
    {
        if (m_parsedOK)
        {
            std::string *s = new std::string("record_type:");
            switch (m_brHeader->m_op)
            {
            case EINSERT:
                s->append("insert");
                break;
            case EUPDATE:
                s->append("update");
                break;
            case EDELETE:
                s->append("delete");
                break;
            case EREPLACE:
                s->append("replace");
                break;
            case EDDL:
                s->append("ddl");
                break;
            case HEARTBEAT:
                s->append("heartbeat");
                break;
            case EBEGIN:
                s->append("begin");
                break;
            case ECOMMIT:
                s->append("commit");
                break;
            case EROLLBACK:
                s->append("rollback");
                break;
            default:
                s->append("unknown");
                break;
            }
            s->append("\n");

            s->append("record_encoding:").append(recordEncoding()).append("\n");

            s->append("source_type:");
            switch (m_brHeader->m_srcType)
            {
            case SRC_MYSQL:
                s->append("mysql");
                break;
            case SRC_OCEANBASE:
                s->append("oceanbase");
                break;
            default:
                s->append("unknown");
                break;
            }
            s->append("\n");

            const char* db = dbname();
            if (db)
                s->append("db:").append(db).append("\n");

            const char* tb = tbname();
            if (tb)
                s->append("table_name:").append(tb).append("\n");

            if (m_brHeader->m_op != HEARTBEAT)
            {
                const char* cp = getCheckpoint();
                if (cp)
                    s->append("checkpoint:").append(cp).append("\n");
            }

            char buf[12];
            snprintf(buf, 11, "%lu", getTimestamp());
            s->append("timestamp:").append(buf).append("\n");

            const char* pks = pkValue();
            if (pks)
                s->append("primary:").append(pks).append("\n");

            if (lastInLogevent())
                s->append("logevent:").append("1").append("\n");

            s->append("\n");

            IStrArray *parsedColNames = colNames();
            const uint8_t *parsedColTypes = colTypes();
            IStrArray *parsedOldCols = oldCols();
            IStrArray *parsedNewCols = newCols();

            // if (parsedColNames && parsedColTypes && (oldCols || newCols))
            if (parsedColNames && parsedColTypes)
            {
                char buf[32];
                size_t size = parsedColNames->size();
                for (size_t i = 0; i < size; i ++)
                {
                    s->append((*parsedColNames)[(int)i]).append("\n");
                    snprintf(buf, 32, "%d", parsedColTypes[i]);
                    s->append(buf).append("\n");
                    if (parsedOldCols)
                    {
                        int len = ((*parsedOldCols)[(int)i]) ? (int)strlen((*parsedOldCols)[(int)i]) : -1;
                        snprintf(buf, 32, "%d", len);
                        s->append(buf).append("\n");
                        if (len > 0)
                            s->append((*parsedOldCols)[(int)i]);
                        s->append("\n");

                    }
                    if (parsedNewCols)
                    {
                        int len = ((*parsedNewCols)[(int)i]) ? (int)strlen((*parsedNewCols)[(int)i]) : -1;
                        snprintf(buf, 32, "%d", len);
                        s->append(buf).append("\n");
                        if (len > 0)
                            s->append((*parsedNewCols)[(int)i]);
                        s->append("\n");
                    }
                }
                delete parsedColNames;
                if (parsedOldCols)
                    delete parsedOldCols;
                if (parsedNewCols)
                    delete parsedNewCols;
            }
            s->append("\n");
            return s;
        }
        return NULL;
    }

	int setRecordType(int aType)
	{
		m_brHeader->m_op = (uint8_t)aType;
		return 0;
	}

	int recordType()
	{
		if (m_creatingMode || m_parsedOK)
			return m_brHeader->m_op;
		else
			return -1;
	}

	void setTimestamp(time_t timestamp)
	{
		m_brHeader->m_timestamp = timestamp;
	}

	time_t getTimestamp()
	{
		if (m_creatingMode || m_parsedOK)
			return m_brHeader->m_timestamp;
		return 0;
	}

	const char* recordEncoding()
    {
        return m_brDataArea->getString(m_brHeader->m_encoding);
    }

    void setInstance(const char* instance)
    {
        m_brHeader->m_instanceOffset = (uint32_t)m_brDataArea->appendString(instance);
    }

    const char* instance()
    {
        return m_brDataArea->getString(m_brHeader->m_instanceOffset);
    }

	void setDbname(const char *dbname)
	{
		m_brHeader->m_dbNameOffset = (uint32_t)m_brDataArea->appendString(dbname);
	}

	const char* dbname() const
	{
		return m_brDataArea->getString(m_brHeader->m_dbNameOffset);
	}

    void setTbname(const char *tbname)
    {
        m_brHeader->m_tbNameOffset = (uint32_t)m_brDataArea->appendString(tbname);
    }

    const char* tbname() const
    {
        return m_brDataArea->getString(m_brHeader->m_tbNameOffset);
    }

	void setCheckpoint(unsigned file, unsigned offset)
	{
        m_brHeader->m_fileNameOffset = file;
		m_brHeader->m_fileOffset = offset;
		// m_brHeader->m_fileNameOffset = m_brDataArea->appendString(file);
	}

	const char* getCheckpoint()
	{
		if (m_creatingMode || m_parsedOK)
		{
			snprintf(m_cpBuf, MAX_CP_BUF, "%u@%d", m_brHeader->m_fileOffset, m_brHeader->m_fileNameOffset);
			return m_cpBuf;
		}
		return NULL;
	}

    unsigned getFileNameOffset()
    {
        if (m_creatingMode || m_parsedOK)
            return m_brHeader->m_fileNameOffset;
        return 0;
    }

    unsigned getFileOffset()
    {
        if (m_creatingMode || m_parsedOK)
            return m_brHeader->m_fileOffset;
        return 0;
    }

	void setLastInLogevent(bool b)
	{
		m_brHeader->m_lastInLogevent = b;
	}

	bool lastInLogevent()
	{
		return m_brHeader->m_lastInLogevent;
	}

    void setId(uint64_t id)
    {
        m_brHeader->m_id = id;
    }

    uint64_t id()
    {
        return m_brHeader->m_id;
    }

    void setPKValue(const char *pkValue)
	{
		m_brHeader->m_pkValOffset = (uint32_t)m_brDataArea->appendString(pkValue);
	}

	const char* pkValue() const
	{
		if (m_creatingMode || m_parsedOK)
			return m_brDataArea->getString(m_brHeader->m_pkValOffset);
		return NULL;
	}

	bool isAnyChanged() const
	{
		if (m_creatingMode)
		{
			if (m_brHeader->m_op != EUPDATE)
				return true;

			if (m_old_cols.size() != m_new_cols.size())
				return true;

			std::vector<std::string*>::const_iterator itOld = m_old_cols.begin();
			std::vector<std::string*>::const_iterator itNew = m_new_cols.begin();
			for( ; itOld != m_old_cols.end() && itNew != m_new_cols.end(); itOld ++, itNew ++)
			{
				if (*itOld == NULL)
				{
					if (*itNew != NULL)
						return true;
				}
				else if (*itNew == NULL)
				{
					if (*itOld != NULL)
						return true;
				}
				else if ((*itOld)->compare(**itNew) != 0)
					return true;
			}
		}
		else if (!m_parsedOK)
		{
			if (m_brHeader->m_op != EUPDATE)
				return true;

			IStrArray *old_cols = m_brDataArea->getStringArray(m_brHeader->m_oldColsOffset);
			IStrArray *new_cols = m_brDataArea->getStringArray(m_brHeader->m_newColsOffset);
			bool ret = true;
			if (old_cols == NULL || new_cols == NULL)
			{
				ret = false;
				goto EXIT;
			}
			if (old_cols->size() != new_cols->size())
				goto EXIT;

			for (size_t i=0,size=old_cols->size(); i<size; ++i)
			{
				const char *itOld = (*old_cols)[(int)i];
				const char *itNew = (*new_cols)[(int)i];
				if (itOld == NULL)
				{
					if (itNew != NULL)
						goto EXIT;
				}
				else if (itNew == NULL)
				{
					if (itOld != NULL)
						goto EXIT;
				}
				else if (strcmp(itOld, itNew) != 0)
					goto EXIT;
			}
EXIT:
			if (old_cols != NULL)
				delete old_cols;
			if (new_cols != NULL)
				delete new_cols;
			return ret;
		}
		return false;
	}

	bool isTxnEnded() const
	{
		if (m_creatingMode || m_parsedOK)
			return (m_brHeader->m_op == ECOMMIT);
		return false;
	}
};

const char *BRInfo::ddlName = "ddl";
const char *BRInfo::heartbeatName = "heartbeat";
uint8_t BRInfo::ddlType[1] = {DRCMSG_TYPE_VAR_STRING};
uint8_t BRInfo::heartbeatType[1] = {DRCMSG_TYPE_LONG};

BinlogRecord::BinlogRecord(time_t timestamp, ITableMeta *tblMeta)
{
	m_br = new BRInfo(timestamp, tblMeta);
}

BinlogRecord::BinlogRecord(const void *ptr, size_t size)
{
	m_br = new BRInfo(ptr, size);
}

BinlogRecord::BinlogRecord(bool creating)
{
	m_br = new BRInfo(creating);
}

BinlogRecord::~BinlogRecord()
{
	delete m_br;
}

void BinlogRecord::clear()
{
	m_br->clear();
}

int BinlogRecord::parse(const void *ptr, size_t size)
{
    // m_buf.assign((const char*)ptr, size);
	return m_br->parse(ptr, size);
}

bool BinlogRecord::parsedOK()
{
	return m_br->m_parsedOK;
}

size_t BinlogRecord::getRealSize()
{
	return m_br->getRealSize();
}

void BinlogRecord::clearOld()
{
    m_br->clearOld();
}

void BinlogRecord::clearNew()
{
	m_br->clearNew();
}

int BinlogRecord::putOld(std::string *val)
{
	return m_br->putOld(val);
}

int BinlogRecord::putNew(std::string *val)
{
	return m_br->putNew(val);
}

const std::vector<std::string *>& BinlogRecord::oldCols()
{
    return m_br->m_old_cols;
}

IStrArray* BinlogRecord::parsedOldCols() const
{
	return m_br->oldCols();
}

const std::vector<std::string *>& BinlogRecord::newCols()
{
    return m_br->m_new_cols;
}

IStrArray* BinlogRecord::parsedNewCols() const
{
	return m_br->newCols();
}

IStrArray* BinlogRecord::parsedColNames() const
{
	return m_br->colNames();
}

const uint8_t* BinlogRecord::parsedColTypes() const
{
    return m_br->colTypes();
}

const std::string* BinlogRecord::toString()
{
	return m_br->toString();
}

int BinlogRecord::recordType()
{
	return m_br->recordType();
}

int BinlogRecord::setRecordType(int aType)
{
	return m_br->setRecordType(aType);
}

void BinlogRecord::setTimestamp(time_t timestamp)
{
	m_br->setTimestamp(timestamp);
}

time_t BinlogRecord::getTimestamp()
{
	return m_br->getTimestamp();
}

void BinlogRecord::setInstance(const char *instance)
{
    m_br->setInstance(instance);
}

const char* BinlogRecord::instance() const
{
    return m_br->instance();
}

void BinlogRecord::setDbname(const char *dbname)
{
	m_br->setDbname(dbname);
}

const char* BinlogRecord::dbname() const
{
	return m_br->dbname();
}

void BinlogRecord::setTbname(const char *tbname)
{
    m_br->setTbname(tbname);
}

const char* BinlogRecord::tbname() const
{
    return m_br->tbname();
}

void BinlogRecord::setCheckpoint(unsigned file, unsigned offset)
{
	m_br->setCheckpoint(file, offset);
}

const char* BinlogRecord::getCheckpoint()
{
	return m_br->getCheckpoint();
}

unsigned BinlogRecord::getFileNameOffset()
{
    return m_br->getFileNameOffset();
}

unsigned BinlogRecord::getFileOffset()
{
    return m_br->getFileOffset();
}

void BinlogRecord::setTableMeta(ITableMeta *tblMeta)
{
	m_br->m_tblMeta = tblMeta;
}

ITableMeta* BinlogRecord::getTableMeta()
{
    return m_br->m_tblMeta;
}

void BinlogRecord::setPKValue(const char *pkValue)
{
	m_br->setPKValue(pkValue);
}

const char* BinlogRecord::pkValue() const
{
	return m_br->pkValue();
}

void BinlogRecord::setLastInLogevent(bool b)
{
	m_br->setLastInLogevent(b);
}

bool BinlogRecord::lastInLogevent()
{
	return m_br->lastInLogevent();
}

void BinlogRecord::setId(uint64_t id)
{
    m_br->setId(id);
}

uint64_t BinlogRecord::id()
{
    return m_br->id();
}

bool BinlogRecord::conflicts(const IBinlogRecord *r)
{
    if (r->getSrcType() != SRC_OCEANBASE)
        return false;
    return conflicts((BinlogRecord*)r);
}

bool BinlogRecord::conflicts(const BinlogRecord *record)
{
	const char *dbname1 = m_br->dbname();
	const char *dbname2 = record->dbname();
    if (strcmp(dbname1, dbname2) != 0)
        return false;
    if (m_br->m_tblMeta != record->m_br->m_tblMeta)
        return false;
    const char *pkvalue1 = pkValue();
    const char *pkvalue2 = record->pkValue();

    if (pkvalue1 == NULL  && pkvalue2 == NULL)
        return true;
    if (pkvalue1 == NULL || pkvalue2 == NULL)
        return false;

	return (strcmp(pkvalue1, pkvalue2) == 0);
}

bool BinlogRecord::isAnyChanged() const
{
	return m_br->isAnyChanged();
}

bool BinlogRecord::isTxnEnded() const
{
	return m_br->isTxnEnded();
}

int BinlogRecord::getSrcType() const
{
    return SRC_OCEANBASE;
}

unsigned BinlogRecord::getCheckpoint1()
{
    return getFileNameOffset();
}

unsigned BinlogRecord::getCheckpoint2()
{
    return getFileOffset();
}

std::string* BinlogRecord::toParsedString()
{
    return m_br->toParsedString();
}

const char* BinlogRecord::recordEncoding()
{
    return m_br->recordEncoding();
}

}
}

