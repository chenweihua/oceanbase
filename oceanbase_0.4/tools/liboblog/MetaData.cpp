/**
 * Copyright (c) 2012 Taobao.com
 * All rights reserved.
 *
 * 文件名称：MetaData.h
 * 摘要：db、table、column的meta信息存取接口
 *       MetaDataCollection流式格式：
 *       MetaDataCollection头
 *          DB1头
 *             DB1-TBL1头
 *               DB1-TBL1-COL1
 *               DB1-TBL1-COL2
 *               ...
 *             DB1-TBL2头
 *             ...
 *          DB2头
 *          ...
 * 作者：Benkong <benkong@taobao.com>
 * 日期：2012.5.22
 */
#include "MetaData.h"
#include "MsgVarArea.h"
#include <tr1/unordered_map>
#include <string.h>
#include <stdint.h>

namespace oceanbase
{
namespace liboblog
{

class StrHash
{
public:
    size_t operator()(const char* const &s) const
    {
        unsigned int hash = 1315423911;
        const char *str = s;
        while(*str) {
            if (*str >= 'A' && *str <= 'Z')
                hash ^= ((hash << 5) + (*str++ + ('a' - 'A')) + (hash >> 2));
            else
                hash ^= ((hash << 5) + (*str++) + (hash >> 2));
        }
        return (hash & 0x7FFFFFFF);
    }

};

// 不区分大小写的比较函数
class IgnoreCaseComparator
{
public:
	bool operator()(const char* const &s1, const char* const &s2) const
	{
		return (strcasecmp(s1, s2) == 0);
	}
};

typedef std::tr1::unordered_map<const char*, int, StrHash, IgnoreCaseComparator> NameIndexMap;
typedef std::tr1::unordered_map<const char*, TableMeta*, StrHash, IgnoreCaseComparator> NameTblmetaMap;
typedef std::tr1::unordered_map<const char*, DBMeta*, StrHash, IgnoreCaseComparator> NameDbmetaMap;

// base class of ColMetaInfo, TableMetaInfo, BMetaInfo, ...
struct MetaInfo
{
public:
	MetaInfo(size_t headerSize, bool creating) : m_header(NULL), m_headerSize(headerSize), m_creating(creating), m_parsedOK(false)
	{
		if (creating)
		{
			m_header = new uint8_t[headerSize];
			m_data.appendArray(m_header, headerSize);
			memset(m_header, -1, headerSize);
		}
	}

	~MetaInfo()
	{
		if (m_creating)
			delete []m_header;
	}

	int appendTo(std::string &s)
	{
		if (m_creating)
		{
			// replace header
			const std::string &m = m_data.getMessage();
			const void *v;
			size_t elSize, count;
			int ret = m_data.getArray(0, v, elSize, count);
			if (ret != 0 || elSize != sizeof(uint8_t) || count != m_headerSize)
				return -1;
			memcpy((void*)v, m_header, m_headerSize);

			m_parsedOK = true; // to support fetching
			s.append(m);
			return 0;
		}
		return -2;
	}

	int parse(const void *ptr, size_t size)
	{
		if (m_creating)
			return -1;

		m_parsedOK = false;
		m_data.clear();
		if (0 != m_data.parse(ptr, size))
			return -2;

		return afterParsing();
	}

	size_t getRealSize()
	{
		return m_data.getRealSize();
	}

protected:
	int afterParsing()
	{
		const void *p;
		size_t elSize, count;
		int ret = m_data.getArray(0, p, elSize, count);
		if (ret != 0 || elSize != sizeof(uint8_t) || count != m_headerSize)
			return -3;

		m_header = (uint8_t*)p;
		m_parsedOK = true;
		return 0;
	}

public:
	MsgVarArea m_data;
	uint8_t *m_header;
	size_t   m_headerSize;
	bool m_creating;
	bool m_parsedOK;
};

// ---------------------ColMeta-----------------------
// 字节数为sizeof(int32_t)的倍数
struct ColMetaHeader
{
	uint32_t  m_nameOffset;
	int32_t   m_type;
	uint8_t   m_signed;
	uint8_t   m_isPK;
	uint8_t   m_notNull;
	uint8_t   m_reserved;
	int64_t   m_length;
	int32_t   m_decimals;
	uint32_t  m_defOffset;
	uint32_t  m_encOffset;
	uint32_t  m_enumSetOffset;
};

struct ColMetaInfo : public MetaInfo
{
	ColMetaHeader  *m_colMetaHeader;
	int  m_required;

	ColMetaInfo() : MetaInfo(sizeof(ColMetaHeader), true), m_colMetaHeader(NULL), m_required(-1)
	{
		m_colMetaHeader = (ColMetaHeader*)m_header;
		m_colMetaHeader->m_signed  = true;
		m_colMetaHeader->m_isPK    = false;
		m_colMetaHeader->m_notNull = false;
	}

	ColMetaInfo(const void *ptr, size_t size) : MetaInfo(sizeof(ColMetaHeader), false), m_colMetaHeader(NULL), m_required(-1)
	{
		parse(ptr, size);
	}

	~ColMetaInfo()
	{
	}

	int parse(const void *ptr, size_t size)
	{
		int ret = MetaInfo::parse(ptr, size);
		if (ret == 0)
		{
			m_required = -1;
			m_colMetaHeader = (ColMetaHeader*)m_header;
			return 0;
		}
		return ret;
	}
};

ColMeta::ColMeta()
{
	m_col = new ColMetaInfo();
}

ColMeta::ColMeta(const void *ptr, size_t size)
{
	m_col = new ColMetaInfo(ptr, size);
}

ColMeta::~ColMeta()
{
	delete m_col;
}

const char* ColMeta::getName()
{
	return m_col->m_data.getString(m_col->m_colMetaHeader->m_nameOffset);
}

int ColMeta::getType()
{
	return m_col->m_colMetaHeader->m_type;
}

long ColMeta::getLength()
{
	return m_col->m_colMetaHeader->m_length;
}

bool ColMeta::isSigned()
{
	return m_col->m_colMetaHeader->m_signed;
}

bool ColMeta::isPK()
{
	return m_col->m_colMetaHeader->m_isPK;
}

bool ColMeta::isNotNull()
{
	return m_col->m_colMetaHeader->m_notNull;
}

int ColMeta::getDecimals()
{
	return m_col->m_colMetaHeader->m_decimals;
}

const char* ColMeta::getDefault()
{
	return m_col->m_data.getString(m_col->m_colMetaHeader->m_defOffset);
}

const char* ColMeta::getEncoding()
{
	return m_col->m_data.getString(m_col->m_colMetaHeader->m_encOffset);
}

int ColMeta::getRequired()
{
	return m_col->m_required;
}

IStrArray* ColMeta::getValuesOfEnumSet()
{
	return m_col->m_data.getStringArray(m_col->m_colMetaHeader->m_enumSetOffset);
}

void ColMeta::setName(const char* name)
{
	m_col->m_colMetaHeader->m_nameOffset = (uint32_t)m_col->m_data.appendString(name);
}

void ColMeta::setType(int type)
{
	m_col->m_colMetaHeader->m_type = type;
}

void ColMeta::setLength(long length)
{
	m_col->m_colMetaHeader->m_length = length;
}

void ColMeta::setSigned(bool b)
{
	m_col->m_colMetaHeader->m_signed = b;
}

void ColMeta::setIsPK(bool b)
{
	m_col->m_colMetaHeader->m_isPK = b;
}

void ColMeta::setNotNull(bool b)
{
	m_col->m_colMetaHeader->m_notNull = b;
}

void ColMeta::setDecimals(int decimals)
{
	m_col->m_colMetaHeader->m_decimals = decimals;
}

void ColMeta::setDefault(const char* def)
{
	m_col->m_colMetaHeader->m_defOffset = (uint32_t)m_col->m_data.appendString(def);
}

void ColMeta::setEncoding(const char* enc)
{
	m_col->m_colMetaHeader->m_encOffset = (uint32_t)m_col->m_data.appendString(enc);
}

void ColMeta::setRequired(int required)
{
	m_col->m_required = required;
}

void ColMeta::setValuesOfEnumSet(std::vector<std::string> &v)
{
	m_col->m_colMetaHeader->m_enumSetOffset= (uint32_t)m_col->m_data.appendStringArray(v);
}

void ColMeta::setValuesOfEnumSet(std::vector<const char*> &v)
{
	m_col->m_colMetaHeader->m_enumSetOffset= (uint32_t)m_col->m_data.appendStringArray(v);
}

void ColMeta::setValuesOfEnumSet(const char** v, size_t size)
{
	m_col->m_colMetaHeader->m_enumSetOffset= (uint32_t)m_col->m_data.appendStringArray(v, size);
}

int ColMeta::appendTo(std::string &s)
{
	return m_col->appendTo(s);
}

size_t ColMeta::getRealSize()
{
	return m_col->getRealSize();
}

int ColMeta::parse(const void *ptr, size_t size)
{
	return m_col->parse(ptr, size);
}

bool ColMeta::parsedOK()
{
	return m_col->m_parsedOK;
}

// --------------------TableMeta-------------------
// 字节数为sizeof(int32_t)的倍数
struct TableMetaHeader
{
	uint32_t m_nameOffset;
	uint8_t  m_hasPK;
	uint8_t  m_reserved;
	uint16_t m_colCount;
	uint32_t m_pksOffset;
	uint32_t m_encOffset;
	uint32_t m_colsSize;
};

struct TableMetaInfo : public MetaInfo
{
	TableMetaHeader          *m_tblMetaHeader;
	std::vector<ColMeta*>    m_cols;
    std::vector<std::string> m_colNames;
    std::vector<std::string> m_pkNames;
	NameIndexMap             m_colsMap;
	IDBMeta                  *m_dbMeta;

	TableMetaInfo() : MetaInfo(sizeof(TableMetaHeader), true), m_tblMetaHeader(NULL), m_dbMeta(NULL)
	{
		m_tblMetaHeader = (TableMetaHeader*)m_header;
		m_tblMetaHeader->m_hasPK  = false;
		m_tblMetaHeader->m_colCount = 0;
		m_tblMetaHeader->m_colsSize = 0;
	}

	TableMetaInfo(const void *ptr, size_t size) : MetaInfo(sizeof(TableMetaHeader), false), m_tblMetaHeader(NULL), m_dbMeta(NULL)
	{
		parse(ptr, size);
	}

	~TableMetaInfo()
	{
		clear();
	}

	int appendTo(std::string &s)
	{
		if (!m_creating)
			return -11;

		// table header
		MetaInfo::appendTo(s);

		// merge all the cols
		size_t colCount = m_cols.size();
		for (size_t i=0; i<colCount; ++i)
		{
			m_cols[i]->appendTo(s);
		}
		return 0;
	}

	int parse(const void *ptr, size_t size)
	{
		// parse table
		int ret = MetaInfo::parse(ptr, size);
		if (ret == 0)
		{
			clear();
			m_cols.clear();
			m_colsMap.clear();

			m_tblMetaHeader = (TableMetaHeader*)m_header;
			size_t colCount = m_tblMetaHeader->m_colCount;
			size_t dataSize = m_tblMetaHeader->m_colsSize;
			if (m_data.getRealSize()+dataSize > size)
				return -11;

			// parse col meta
			const char *p = (char*)ptr + m_data.getRealSize();
			for (size_t i=0; i<colCount; ++i)
			{
				ColMeta *colMeta = new ColMeta(p, dataSize);
				if (!colMeta->parsedOK())
				{
					delete colMeta;
					return -12;
				}
				size_t colSize = colMeta->getRealSize();
				if (colSize > dataSize)
				{
					delete colMeta;
					return -13;
				}
				m_colsMap[colMeta->getName()] = (int)m_cols.size();
				m_cols.push_back(colMeta);
                m_colNames.push_back(colMeta->getName());
				p += colSize;
				dataSize -= colSize;
			}
			return 0;
		}

		return ret;
	}

	size_t getRealSize()
	{
		if (!m_parsedOK)
		{
			size_t colCount = m_cols.size();
			m_tblMetaHeader->m_colCount = (uint16_t)colCount;
			size_t colsSize = 0;
			for (size_t i=0; i<colCount; ++i)
				colsSize += m_cols[i]->getRealSize();
			m_tblMetaHeader->m_colsSize = (uint32_t)colsSize;
		}
		return m_data.getRealSize() + m_tblMetaHeader->m_colsSize;
	}

private:
	void clear()
	{
		for (size_t i=0; i<m_cols.size(); ++i)
			if (m_cols[i] != NULL)
				delete m_cols[i];
	}
};

TableMeta::TableMeta()
{
	m_tbl = new TableMetaInfo();
}

TableMeta::TableMeta(const void *ptr, size_t size)
{
	m_tbl = new TableMetaInfo(ptr, size);
}

TableMeta::~TableMeta()
{
	delete m_tbl;
}

const char* TableMeta::getName()
{
	return m_tbl->m_data.getString(m_tbl->m_tblMetaHeader->m_nameOffset);
}

bool TableMeta::hasPK()
{
	return m_tbl->m_tblMetaHeader->m_hasPK;
}

const char* TableMeta::getPKs()
{
	return m_tbl->m_data.getString(m_tbl->m_tblMetaHeader->m_pksOffset);
}

const char* TableMeta::getEncoding()
{
	return m_tbl->m_data.getString(m_tbl->m_tblMetaHeader->m_encOffset);
}

IDBMeta* TableMeta::getDBMeta()
{
	return m_tbl->m_dbMeta;
}

void TableMeta::setName(const char* name)
{
	m_tbl->m_tblMetaHeader->m_nameOffset = (uint32_t)m_tbl->m_data.appendString(name);
}

void TableMeta::setHasPK(bool b)
{
	m_tbl->m_tblMetaHeader->m_hasPK = b;
}

void TableMeta::setPKs(const char* pks)
{
	m_tbl->m_tblMetaHeader->m_pksOffset = (uint32_t)m_tbl->m_data.appendString(pks);
	char *cpks = new char[strlen(pks) + 1];
	char *src = strcpy(cpks, pks);
	char *token, *save;
	do {
		token = strtok_r(src, ",", &save);
		src = NULL;
		if (token)
		{
			m_tbl->m_pkNames.push_back(std::string (token));
		}
		else
		{
			break;
		}
	} while (true);
	delete []cpks;
}

std::vector<std::string> &TableMeta::getPKColNames()
{
    return m_tbl->m_pkNames;
}

void TableMeta::setEncoding(const char* enc)
{
	m_tbl->m_tblMetaHeader->m_encOffset = (uint32_t)m_tbl->m_data.appendString(enc);
}

void TableMeta::setDBMeta(IDBMeta* dbMeta)
{
	m_tbl->m_dbMeta = dbMeta;
}

std::vector<std::string> &TableMeta::getColNames()
{
    return m_tbl->m_colNames;
}

IColMeta* TableMeta::getCol(const char *colName)
{
	NameIndexMap &colsMap = m_tbl->m_colsMap;
	NameIndexMap::iterator i = colsMap.find(colName);
	if (i != colsMap.end())
		return m_tbl->m_cols[i->second];
	return NULL;
}

int TableMeta::getColCount()
{
	return (int)m_tbl->m_cols.size();
}

IColMeta* TableMeta::getCol(int index)
{
	int len = (int)m_tbl->m_cols.size();
	if (index >= 0 && index < len)
		return m_tbl->m_cols[index];
	return NULL;
}

int TableMeta::getColNum(const char *colName)
{
	NameIndexMap &colsMap = m_tbl->m_colsMap;
	NameIndexMap::iterator i = colsMap.find(colName);
	if (i != colsMap.end())
		return i->second;
	return -1;
}

int TableMeta::append(const char *colName, IColMeta *colMeta)
{
	NameIndexMap &colsMap = m_tbl->m_colsMap;
	NameIndexMap::iterator i = colsMap.find(colName);
	if (i != colsMap.end())
		return -1;
	colsMap[colMeta->getName()] = (int)m_tbl->m_cols.size();
	m_tbl->m_cols.push_back((ColMeta*)colMeta);
    m_tbl->m_colNames.push_back(colMeta->getName());
	return 0;
}

int TableMeta::appendTo(std::string &s)
{
	return m_tbl->appendTo(s);
}

size_t TableMeta::getRealSize()
{
	return m_tbl->getRealSize();
}

int TableMeta::parse(const void *ptr, size_t size)
{
	return m_tbl->parse(ptr, size);
}

bool TableMeta::parsedOK()
{
	return m_tbl->m_parsedOK;
}

// ------------------DBMeta----------------
struct DBMetaHeader
{
	uint32_t m_nameOffset;
	uint32_t m_encOffset;
	uint32_t m_tblCount;
	uint32_t m_tblsSize;
};

struct DBMetaInfo : public MetaInfo
{
	DBMetaHeader *m_dbMetaHeader;
	NameTblmetaMap m_tables;
    std::vector<TableMeta *> m_tableOrders;
	IMetaDataCollections *m_metaDataCollections;

	DBMetaInfo() : MetaInfo(sizeof(DBMetaHeader), true), m_dbMetaHeader(NULL), m_metaDataCollections(NULL)
	{
		m_dbMetaHeader = (DBMetaHeader*)m_header;
		m_dbMetaHeader->m_tblCount = 0;
		m_dbMetaHeader->m_tblsSize = 0;
	}

	DBMetaInfo(const void *ptr, size_t size, DBMeta *owner) : MetaInfo(sizeof(DBMetaHeader), false), m_dbMetaHeader(NULL), m_metaDataCollections(NULL)
	{
		this->parse(ptr, size, owner);
	}

	~DBMetaInfo()
	{
		clear();
	}

	int appendTo(std::string &s)
	{
		if (!m_creating)
			return -21;

		// db header
		MetaInfo::appendTo(s);

		// merge all the tbls
		NameTblmetaMap::iterator i;
		for (i=m_tables.begin(); i!=m_tables.end(); ++i)
		{
			TableMeta *tblMeta = i->second;
			tblMeta->appendTo(s);
		}
		return 0;
	}

	int parse(const void *ptr, size_t size, DBMeta *owner)
	{
		// parse db
		int ret = MetaInfo::parse(ptr, size);
		if (ret == 0)
		{
			clear();

			m_dbMetaHeader = (DBMetaHeader*)m_header;
			size_t tblCount = m_dbMetaHeader->m_tblCount;
			size_t dataSize = m_dbMetaHeader->m_tblsSize;
			if (m_data.getRealSize()+dataSize > size)
				return -21;

			// parse tbl meta
			const char *p = (char*)ptr + m_data.getRealSize();
			for (size_t i=0; i<tblCount; ++i)
			{
				TableMeta *tblMeta = new TableMeta(p, dataSize);
				if (!tblMeta->parsedOK())
				{
					delete tblMeta;
					return -22;
				}
				size_t tblSize = tblMeta->getRealSize();
				if (tblSize > dataSize)
				{
					delete tblMeta;
					return -23;
				}
				tblMeta->setDBMeta(owner);
				m_tables[tblMeta->getName()] = tblMeta;
                m_tableOrders.push_back(tblMeta);
				p += tblSize;
				dataSize -= tblSize;
			}
			return 0;
		}
		return ret;
	}

	size_t getRealSize()
	{
		if (!m_parsedOK)
		{
			m_dbMetaHeader->m_tblCount = (uint32_t)m_tables.size();
			size_t tblsSize = 0;
			NameTblmetaMap::iterator i;
			for (i=m_tables.begin(); i!=m_tables.end(); ++i)
				tblsSize += i->second->getRealSize();
			m_dbMetaHeader->m_tblsSize = (uint32_t)tblsSize;
		}
		return m_data.getRealSize() + m_dbMetaHeader->m_tblsSize;
	}

private:
	void clear()
	{
		NameTblmetaMap::iterator i;;
        m_tableOrders.clear();
		for (i=m_tables.begin(); i!=m_tables.end(); ++i)
		{
			delete i->second;
		}
	}
};

DBMeta::DBMeta()
{
	m_db = new DBMetaInfo();
}

DBMeta::DBMeta(const void *ptr, size_t size)
{
	m_db = new DBMetaInfo(ptr, size, this);
}

DBMeta::~DBMeta()
{
	delete m_db;
}

const char* DBMeta::getName()
{
	return m_db->m_data.getString(m_db->m_dbMetaHeader->m_nameOffset);
}

const char* DBMeta::getEncoding()
{
	return m_db->m_data.getString(m_db->m_dbMetaHeader->m_encOffset);
}

IMetaDataCollections* DBMeta::getMetaDataCollections()
{
	return m_db->m_metaDataCollections;
}

void DBMeta::setName(const char* name)
{
	m_db->m_dbMetaHeader->m_nameOffset = (uint32_t)m_db->m_data.appendString(name);
}

void DBMeta::setEncoding(const char* enc)
{
	m_db->m_dbMetaHeader->m_encOffset = (uint32_t)m_db->m_data.appendString(enc);
}

void DBMeta::setMetaDataCollections(IMetaDataCollections *mdc)
{
	m_db->m_metaDataCollections = mdc;
}

int DBMeta::getTblCount()
{
    return (int)m_db->m_tables.size();
}

ITableMeta* DBMeta::get(const char* tblName)
{
	NameTblmetaMap::iterator i = m_db->m_tables.find(tblName);
	if (i != m_db->m_tables.end())
		return i->second;
	return NULL;
}

ITableMeta* DBMeta::get(int index)
{
    if (index >= 0 && index < (int)m_db->m_tableOrders.size())
        return m_db->m_tableOrders[index];
    return NULL;
}

int DBMeta::put(const char *tblName, ITableMeta *tblMeta)
{
	NameTblmetaMap::iterator i = m_db->m_tables.find(tblName);
	if (i != m_db->m_tables.end())
		return -1;
	m_db->m_tables[tblMeta->getName()] = (TableMeta*)tblMeta;
    m_db->m_tableOrders.push_back((TableMeta*)tblMeta);
	return 0;
}

int DBMeta::appendTo(std::string &s)
{
	return m_db->appendTo(s);
}

size_t DBMeta::getRealSize()
{
	return m_db->getRealSize();
}

int DBMeta::parse(const void *ptr, size_t size)
{
	return m_db->parse(ptr, size, this);
}

bool DBMeta::parsedOK()
{
	return m_db->m_parsedOK;
}

// -----------------MetaDataCollections-----------------
struct MetaDataCollectionHeader
{
	uint32_t m_metaVerNum;
	uint32_t m_dbCount;
	uint64_t m_timestamp;
	uint32_t m_dbsSize;
};

struct MetaDataCollectionInfo : public MetaInfo
{
	MetaDataCollectionHeader *m_collHeader;
	NameDbmetaMap m_dbs;
    std::vector<DBMeta *> m_dbOrders;
	IMetaDataCollections *m_prev;
	bool m_removePtr;
	const void *m_ptr;

	MetaDataCollectionInfo() : MetaInfo(sizeof(MetaDataCollectionHeader), true), m_collHeader(NULL), m_prev(NULL), m_removePtr(false), m_ptr(NULL)
	{
		m_collHeader = (MetaDataCollectionHeader*)m_header;
		m_collHeader->m_metaVerNum = 0;
		m_collHeader->m_dbCount = 0;
		m_collHeader->m_dbsSize = 0;
	}

	MetaDataCollectionInfo(const void *ptr, size_t size, bool removePtr, IMetaDataCollections *owner) : MetaInfo(sizeof(MetaDataCollectionHeader), false), m_collHeader(NULL), m_prev(NULL), m_removePtr(removePtr), m_ptr(ptr)
	{
		this->parse(ptr, size, owner);
	}

	~MetaDataCollectionInfo()
	{
		if (m_removePtr)
			delete [](char*)m_ptr;
		clear();
	}

	int appendTo(std::string &s)
	{
		if (!m_creating)
			return -31;

		// coll header
		this->getRealSize(); // very important
		MetaInfo::appendTo(s);

		// merge all the dbs
		NameDbmetaMap::iterator i;
		for (i=m_dbs.begin(); i!=m_dbs.end(); ++i)
		{
			DBMeta *dbMeta = i->second;
			dbMeta->appendTo(s);
		}
		return 0;
	}

	int parse(const void *ptr, size_t size, IMetaDataCollections *owner)
	{
		// parse coll
		int ret = MetaInfo::parse(ptr, size);
		if (ret == 0)
		{
			clear();

			m_collHeader = (MetaDataCollectionHeader*)m_header;
			size_t dbCount = m_collHeader->m_dbCount;
			size_t dataSize = m_collHeader->m_dbsSize;
			if (m_data.getRealSize()+dataSize > size)
				return -31;

			// parse db meta
			const char *p = (char*)ptr + m_data.getRealSize();
			for (size_t i=0; i<dbCount; ++i)
			{
				DBMeta *dbMeta = new DBMeta(p, dataSize);
				if (!dbMeta->parsedOK())
				{
					delete dbMeta;
					return -32;
				}
				size_t dbSize = dbMeta->getRealSize();
				if (dbSize > dataSize)
				{
					delete dbMeta;
					return -33;
				}
				dbMeta->setMetaDataCollections(owner);
				m_dbs[dbMeta->getName()] = dbMeta;
                m_dbOrders.push_back(dbMeta);
				p += dbSize;
				dataSize -= dbSize;
			}
			return 0;
		}
		return ret;
	}

	size_t getRealSize()
	{
		if (!m_parsedOK)
		{
			m_collHeader->m_dbCount = (uint32_t)m_dbs.size();
			size_t dbsSize = 0;
			NameDbmetaMap::iterator i;
			for (i=m_dbs.begin(); i!=m_dbs.end(); ++i)
				dbsSize += i->second->getRealSize();
			m_collHeader->m_dbsSize = (uint32_t)dbsSize;
		}
		return m_data.getRealSize() + m_collHeader->m_dbsSize;
	}

private:
	void clear()
	{
		NameDbmetaMap::iterator i;;
		for (i=m_dbs.begin(); i!=m_dbs.end(); ++i)
		{
			delete i->second;
		}
        m_dbOrders.clear();
	}
};

MetaDataCollections::MetaDataCollections()
{
	m_coll = new MetaDataCollectionInfo();
}

MetaDataCollections::MetaDataCollections(const void *ptr, size_t size, bool removePtr)
{
	m_coll = new MetaDataCollectionInfo(ptr, size, removePtr, this);
}

MetaDataCollections::~MetaDataCollections()
{
	delete m_coll;
}

unsigned MetaDataCollections::getMetaVerNum()
{
	return m_coll->m_collHeader->m_metaVerNum;
}

IMetaDataCollections* MetaDataCollections::getPrev()
{
	return m_coll->m_prev;
}

time_t MetaDataCollections::getTimestamp()
{
	return m_coll->m_collHeader->m_timestamp;
}

void MetaDataCollections::setMetaVerNum(unsigned metaVerNum)
{
	m_coll->m_collHeader->m_metaVerNum = metaVerNum;
}

void MetaDataCollections::setPrev(IMetaDataCollections* prev)
{
	m_coll->m_prev = prev;
}

void MetaDataCollections::setTimestamp(time_t timestamp)
{
	m_coll->m_collHeader->m_timestamp = timestamp;
}

int MetaDataCollections::getDbCount()
{
    return (int)m_coll->m_dbs.size();
}

IDBMeta *MetaDataCollections::get(const char* dbName)
{
	NameDbmetaMap::iterator i = m_coll->m_dbs.find(dbName);
	if (i == m_coll->m_dbs.end())
		return NULL;
	return i->second;
}

IDBMeta *MetaDataCollections::get(int index)
{
    if (index >= 0 && index < (int)m_coll->m_dbs.size())
        return m_coll->m_dbOrders[index];
    return NULL;
}

ITableMeta* MetaDataCollections::get(const char *dbName, const char *tblName)
{
	NameDbmetaMap::iterator i = m_coll->m_dbs.find(dbName);
	if (i == m_coll->m_dbs.end())
		return NULL;
	DBMeta* dbMeta = i->second;
	if (dbMeta == NULL)
		return NULL;
	return dbMeta->get(tblName);
}

int MetaDataCollections::put(const char *dbName, IDBMeta *dbMeta)
{
	NameDbmetaMap &dbs = m_coll->m_dbs;
	NameDbmetaMap::iterator i = dbs.find(dbName);
	if (i != dbs.end())
		return -1;
	dbs[dbMeta->getName()] = (DBMeta*)dbMeta;
    m_coll->m_dbOrders.push_back((DBMeta*)dbMeta);
	return 0;
}

int MetaDataCollections::toString(std::string &s)
{
	s.clear();
	return m_coll->appendTo(s);
}

int MetaDataCollections::parse(const void *ptr, size_t size)
{
	return m_coll->parse(ptr, size, this);
}

bool MetaDataCollections::parsedOK()
{
	return m_coll->m_parsedOK;
}

size_t MetaDataCollections::getRealSize()
{
	return m_coll->getRealSize();
}

}
}

