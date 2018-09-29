#pragma once

#import "msado15.dll" no_namespace rename("EOF", "adoEOF")

#include "sqlite3.h"

#if _MSC_VER == 1600
	#if defined _M_X64
		#ifdef _DEBUG
			#pragma comment(lib, "../../../base/mCDatabase/VS2010/64/Debug/lib-sqlite3.lib")
		#else
			#pragma comment(lib, "../../../base/mCDatabase/VS2010/64/Release/lib-sqlite3.lib")
		#endif
	#else
		#ifdef _DEBUG
			#pragma comment(lib, "../../../base/mCDatabase/VS2010/32/Debug/lib-sqlite3.lib")
		#else
			#pragma comment(lib, "../../../base/mCDatabase/VS2010/32/Release/lib-sqlite3.lib")
		#endif
	#endif
#else
	#if defined _M_X64
		#ifdef _DEBUG
			#pragma comment(lib, "../../../base/mCDatabase/VS2015/64/Debug/lib-sqlite3.lib")
		#else
			#pragma comment(lib, "../../../base/mCDatabase/VS2015/64/Release/lib-sqlite3.lib")
		#endif
	#else
		#ifdef _DEBUG
			#pragma comment(lib, "../../../base/mCDatabase/VS2015/32/Debug/lib-sqlite3.lib")
		#else
			#pragma comment(lib, "../../../base/mCDatabase/VS2015/32/Release/lib-sqlite3.lib")
		#endif
	#endif
#endif

class mCDatabase
{
public:
	class CRs
	{
	public:
		CRs(mCDatabase *pDb);
		~CRs();

		mCDatabase *m_pDb;

		_RecordsetPtr m_pRsAdo;
		sqlite3_stmt *m_pRsSqlite;

		Json::Value m_kv;
		CStringW m_usWhere;
		string m_asWhere;

		bool eof;

		bool m_bQuery;
		CStringW m_usSql;
		string m_asSql;

		void Add(CStringW TableName);
		void Delete(CStringW TableName);
		void Modify(CStringW TableName);
		void Query(CStringW sql);

		void Add(string TableName);
		void Delete(string TableName);
		void Modify(string TableName);
		void Query(string sql);

		void MoveNext();

		void SetInt(CStringW key, int value);
		void SetDouble(CStringW key, double value);
		void SetString(CStringW key, CStringW value);
		void SetString(CStringW key, string value);
		void SetDateTime(CStringW key, CStringW value, bool bIncludingMS);
		void SetDateTime(CStringW key, string value, bool bIncludingMS);

		void SetInt(string key, int value);
		void SetDouble(string key, double value);
		void SetString(string key, CStringW value);
		void SetString(string key, string value);
		void SetDateTime(string key, CStringW value, bool bIncludingMS);
		void SetDateTime(string key, string value, bool bIncludingMS);

		void SetWhere(CStringW where);
		void SetWhere(string where);

		int KeyToIndex(CStringW key);
		int KeyToIndex(string key);

		unsigned int GetRecordSize();
		unsigned int GetColumnSize();
		void GetColumnName(int index, CStringW &name);
		void GetColumnName(int index, string &name);

		int GetInt(CStringW key);
		double GetDouble(CStringW key);
		void GetString(CStringW key, CStringW &value);
		void GetString(CStringW key, string &value);
		void GetDateTime(CStringW key, CStringW &value);
		void GetDateTime(CStringW key, string &value);

		int GetInt(string key);
		double GetDouble(string key);
		void GetString(string key, CStringW &value);
		void GetString(string key, string &value);
		void GetDateTime(string key, CStringW &value);
		void GetDateTime(string key, string &value);
	};

public:
	mCDatabase();
	~mCDatabase();

	string m_dbtype;

	_ConnectionPtr m_pConnAdo;
	sqlite3 *m_pConnSqlite;

	bool CheckError(int iError, _com_error *pe, const char *pErrorMsg, CStringW sql);
	bool CheckError(int iError, _com_error *pe, const char *pErrorMsg, string sql);

	void OpenDatabase(CStringW OptionFile);
	void OpenDatabase(Json::Value option);
	void CloseDatabase();

	void NewTable(CStringW SqlFile);
	void DeleteTable(CStringW TableName);

	void DeleteTable(string TableName);

	CRs *OpenRs();
	void CloseRs(CRs *pRs);
};