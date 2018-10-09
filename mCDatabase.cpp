#include "stdafx.h"
#include "mCDatabase.h"


mCDatabase::CRs::CRs(mCDatabase *pDb)
{
	m_pDb = pDb;

	m_usWhere = L"";

	eof = true;

	if(m_pDb->m_dbtype == "mysql" || m_pDb->m_dbtype == "sqlserver" || m_pDb->m_dbtype == "access")
	{
		try	
		{
			m_pRsAdo.CreateInstance( __uuidof(Recordset) );
		}
		catch(_com_error &e)
		{
			if( m_pDb->CheckError( 501, &e, NULL, L"OpenRs" ) ) return;
		}
	}

	if(m_pDb->m_dbtype == "sqlite")
	{
		m_pRsSqlite = NULL;
		m_bQuery = false;
	}
}
mCDatabase::CRs::~CRs()
{
	if(m_pDb->m_dbtype == "mysql" || m_pDb->m_dbtype == "sqlserver" || m_pDb->m_dbtype == "access")
	{
		if(m_pRsAdo != NULL)
		{
			try	
			{
				if(m_pRsAdo->State != adStateClosed) m_pRsAdo->Close();

				m_pRsAdo.Release();
				m_pRsAdo = NULL;
			}
			catch(_com_error &e)
			{
				if( m_pDb->CheckError( 502, &e, NULL, L"CloseRs" ) ) return;
			}
		}
	}

	if(m_pDb->m_dbtype == "sqlite")
	{
		if(m_bQuery)
		{
			m_bQuery = false;
			sqlite3_finalize(m_pRsSqlite);
		}
	}
}


void mCDatabase::CRs::Add(CStringW TableName)
{
	CStringW sql = L"insert into " + TableName + L"(";

	unsigned int size = m_kv.size();

	for(unsigned int i = 0; i < size; i++)
	{
		sql += m_kv[i][0].asCString();
		if(i != size - 1) sql += L",";
	}

	sql += L")values(";

	CStringW value;
	for(unsigned int i = 0; i < size; i++)
	{
		switch( m_kv[i][1].type() )
		{
		case Json::intValue:
			{
				value.Format( L"%d", m_kv[i][1].asInt() );
				sql += value;
			}
			break;

		case Json::realValue:
			{
				value.Format( L"%lf", m_kv[i][1].asDouble() );
				sql += value;
			}
			break;

		case Json::stringValue:
			{
				sql += L"'" + CStringW( m_kv[i][1].asCString() ) + L"'";
			}
			break;
		}

		if(i != size - 1) sql += L",";
	}

	sql += L")";

	if(m_pDb->m_dbtype == "mysql" || m_pDb->m_dbtype == "sqlserver" || m_pDb->m_dbtype == "access")
	{
		try
		{
			m_pRsAdo->Open( (_variant_t)sql, m_pDb->m_pConnAdo.GetInterfacePtr(), adOpenStatic, adLockOptimistic, adCmdText );
		}
		catch(_com_error &e)
		{
			if( m_pDb->CheckError(503, &e, NULL, sql) ) return;
		}
	}

	if(m_pDb->m_dbtype == "sqlite")
	{
		int iReturn = 0;
		char *pErrorMsg = NULL;

		iReturn = sqlite3_exec(m_pDb->m_pConnSqlite, CT2A(sql, CP_UTF8), NULL, NULL, &pErrorMsg);
		if(iReturn != SQLITE_OK)
		{
			m_pDb->CheckError(503, NULL, pErrorMsg, sql);

			sqlite3_free(pErrorMsg);
		}
	}
}
void mCDatabase::CRs::Delete(CStringW TableName)
{
	CStringW sql = L"delete from " + TableName;

	if( m_usWhere != L"" ) sql += L" where " + m_usWhere;

	if(m_pDb->m_dbtype == "mysql" || m_pDb->m_dbtype == "sqlserver" || m_pDb->m_dbtype == "access")
	{
		try	
		{
			m_pRsAdo->Open( (_variant_t)sql, m_pDb->m_pConnAdo.GetInterfacePtr(), adOpenStatic, adLockOptimistic, adCmdText );
		}
		catch(_com_error &e)
		{
			if( m_pDb->CheckError(504, &e, NULL, sql) ) return;
		}
	}

	if(m_pDb->m_dbtype == "sqlite")
	{
		int iReturn = 0;
		char *pErrorMsg = NULL;

		iReturn = sqlite3_exec(m_pDb->m_pConnSqlite, CT2A(sql, CP_UTF8), NULL, NULL, &pErrorMsg);
		if(iReturn != SQLITE_OK)
		{
			m_pDb->CheckError(504, NULL, pErrorMsg, sql);

			sqlite3_free(pErrorMsg);
		}
	}
}
void mCDatabase::CRs::Modify(CStringW TableName)
{
	CStringW sql = L"update " + TableName + L" set ";

	unsigned int size = m_kv.size();
	CStringW value;
	for(unsigned int i = 0; i < size; i++)
	{
		sql += CStringW( m_kv[i][0].asCString() ) + L"=";

		switch( m_kv[i][1].type() )
		{
		case Json::intValue:
			{
				value.Format( L"%d", m_kv[i][1].asInt() );
				sql += value;
			}
			break;

		case Json::realValue:
			{
				value.Format( L"%lf", m_kv[i][1].asDouble() );
				sql += value;
			}
			break;

		case Json::stringValue:
			{
				sql += L"'" + CStringW( m_kv[i][1].asCString() ) + L"'";
			}
			break;
		}

		if(i != size - 1) sql += L",";
	}

	if( m_usWhere != L"" ) sql += L" where " + m_usWhere;

	if(m_pDb->m_dbtype == "mysql" || m_pDb->m_dbtype == "sqlserver" || m_pDb->m_dbtype == "access")
	{
		try
		{
			m_pRsAdo->Open( (_variant_t)sql, m_pDb->m_pConnAdo.GetInterfacePtr(), adOpenStatic, adLockOptimistic, adCmdText );
		}
		catch(_com_error &e)
		{
			if( m_pDb->CheckError(505, &e, NULL, sql) ) return;
		}
	}

	if(m_pDb->m_dbtype == "sqlite")
	{
		int iReturn = 0;
		char *pErrorMsg = NULL;

		iReturn = sqlite3_exec(m_pDb->m_pConnSqlite, CT2A(sql, CP_UTF8), NULL, NULL, &pErrorMsg);
		if(iReturn != SQLITE_OK)
		{
			m_pDb->CheckError(505, NULL, pErrorMsg, sql);

			sqlite3_free(pErrorMsg);
		}
	}
}
void mCDatabase::CRs::Query(CStringW sql)
{
	if(m_pDb->m_dbtype == "mysql" || m_pDb->m_dbtype == "sqlserver" || m_pDb->m_dbtype == "access")
	{
		try	
		{
			m_pRsAdo->CursorLocation = adUseClient; // 使RecordCount可用
			m_pRsAdo->Open( (_variant_t)sql, m_pDb->m_pConnAdo.GetInterfacePtr(), adOpenStatic, adLockOptimistic, adCmdText );

			eof = m_pRsAdo->adoEOF == VARIANT_TRUE ? true : false;
		}
		catch(_com_error &e)
		{
			if( m_pDb->CheckError(506, &e, NULL, sql) ) return;
		}
	}

	if(m_pDb->m_dbtype == "sqlite")
	{
		m_bQuery = true;
		m_usSql = sql;

		int iReturn = sqlite3_prepare_v2(m_pDb->m_pConnSqlite, CT2A(sql, CP_UTF8), (int)strlen( CT2A(sql, CP_UTF8) ), &m_pRsSqlite, NULL);
		if(iReturn !=  SQLITE_OK)
		{
			if( m_pDb->CheckError(506, NULL, sqlite3_errmsg(m_pDb->m_pConnSqlite), sql) ) return;
		}

		iReturn = sqlite3_step(m_pRsSqlite);
		eof = iReturn == SQLITE_DONE ? true : false;
	}
}


void mCDatabase::CRs::Add(string TableName)
{
	string sql = "insert into " + TableName + "(";

	unsigned int size = m_kv.size();

	for(unsigned int i = 0; i < size; i++)
	{
		sql += m_kv[i][0].asCString();
		if(i != size - 1) sql += ",";
	}

	sql += ")values(";

	CStringA value;
	for(unsigned int i = 0; i < size; i++)
	{
		switch( m_kv[i][1].type() )
		{
		case Json::intValue:
			{
				value.Format( "%d", m_kv[i][1].asInt() );
				sql += value;
			}
			break;

		case Json::realValue:
			{
				value.Format( "%lf", m_kv[i][1].asDouble() );
				sql += value;
			}
			break;

		case Json::stringValue:
			{
				sql += "'" + m_kv[i][1].asString() + "'";
			}
			break;
		}

		if(i != size - 1) sql += ",";
	}

	sql += ")";

	if(m_pDb->m_dbtype == "mysql" || m_pDb->m_dbtype == "sqlserver" || m_pDb->m_dbtype == "access")
	{
		try
		{
			m_pRsAdo->Open( (_variant_t)sql.c_str(), m_pDb->m_pConnAdo.GetInterfacePtr(), adOpenStatic, adLockOptimistic, adCmdText );
		}
		catch(_com_error &e)
		{
			if( m_pDb->CheckError(503, &e, NULL, sql) ) return;
		}
	}

	if(m_pDb->m_dbtype == "sqlite")
	{
		int iReturn = 0;
		char *pErrorMsg = NULL;

		iReturn = sqlite3_exec(m_pDb->m_pConnSqlite, stou8(sql), NULL, NULL, &pErrorMsg);
		if(iReturn != SQLITE_OK)
		{
			m_pDb->CheckError(503, NULL, pErrorMsg, sql);

			sqlite3_free(pErrorMsg);
		}
	}
}
void mCDatabase::CRs::Delete(string TableName)
{
	string sql = "delete from " + TableName;

	if( m_asWhere != "" ) sql += " where " + m_asWhere;

	if(m_pDb->m_dbtype == "mysql" || m_pDb->m_dbtype == "sqlserver" || m_pDb->m_dbtype == "access")
	{
		try	
		{
			m_pRsAdo->Open( (_variant_t)sql.c_str(), m_pDb->m_pConnAdo.GetInterfacePtr(), adOpenStatic, adLockOptimistic, adCmdText );
		}
		catch(_com_error &e)
		{
			if( m_pDb->CheckError(504, &e, NULL, sql) ) return;
		}
	}

	if(m_pDb->m_dbtype == "sqlite")
	{
		int iReturn = 0;
		char *pErrorMsg = NULL;

		iReturn = sqlite3_exec(m_pDb->m_pConnSqlite, stou8(sql), NULL, NULL, &pErrorMsg);
		if(iReturn != SQLITE_OK)
		{
			m_pDb->CheckError(504, NULL, pErrorMsg, sql);

			sqlite3_free(pErrorMsg);
		}
	}
}
void mCDatabase::CRs::Modify(string TableName)
{
	string sql = "update " + TableName + " set ";

	unsigned int size = m_kv.size();
	CStringA value;
	for(unsigned int i = 0; i < size; i++)
	{
		sql += m_kv[i][0].asString() + "=";

		switch( m_kv[i][1].type() )
		{
		case Json::intValue:
			{
				value.Format( "%d", m_kv[i][1].asInt() );
				sql += value;
			}
			break;

		case Json::realValue:
			{
				value.Format( "%lf", m_kv[i][1].asDouble() );
				sql += value;
			}
			break;

		case Json::stringValue:
			{
				sql += "'" + m_kv[i][1].asString() + "'";
			}
			break;
		}

		if(i != size - 1) sql += ",";
	}

	if( m_asWhere != "" ) sql += " where " + m_asWhere;

	if(m_pDb->m_dbtype == "mysql" || m_pDb->m_dbtype == "sqlserver" || m_pDb->m_dbtype == "access")
	{
		try
		{
			m_pRsAdo->Open( (_variant_t)sql.c_str(), m_pDb->m_pConnAdo.GetInterfacePtr(), adOpenStatic, adLockOptimistic, adCmdText );
		}
		catch(_com_error &e)
		{
			if( m_pDb->CheckError(505, &e, NULL, sql) ) return;
		}
	}

	if(m_pDb->m_dbtype == "sqlite")
	{
		int iReturn = 0;
		char *pErrorMsg = NULL;

		iReturn = sqlite3_exec(m_pDb->m_pConnSqlite, stou8(sql), NULL, NULL, &pErrorMsg);
		if(iReturn != SQLITE_OK)
		{
			m_pDb->CheckError(505, NULL, pErrorMsg, sql);

			sqlite3_free(pErrorMsg);
		}
	}
}
void mCDatabase::CRs::Query(string sql)
{
	if(m_pDb->m_dbtype == "mysql" || m_pDb->m_dbtype == "sqlserver" || m_pDb->m_dbtype == "access")
	{
		try	
		{
			m_pRsAdo->CursorLocation = adUseClient; // 使RecordCount可用
			m_pRsAdo->Open( (_variant_t)sql.c_str(), m_pDb->m_pConnAdo.GetInterfacePtr(), adOpenStatic, adLockOptimistic, adCmdText );

			eof = m_pRsAdo->adoEOF == VARIANT_TRUE ? true : false;
		}
		catch(_com_error &e)
		{
			if( m_pDb->CheckError(506, &e, NULL, sql) ) return;
		}
	}

	if(m_pDb->m_dbtype == "sqlite")
	{
		m_bQuery = true;
		m_asSql = sql;

		int iReturn = sqlite3_prepare_v2(m_pDb->m_pConnSqlite, stou8(sql), (int)strlen( stou8(sql) ), &m_pRsSqlite, NULL);
		if(iReturn !=  SQLITE_OK)
		{
			if( m_pDb->CheckError(506, NULL, sqlite3_errmsg(m_pDb->m_pConnSqlite), sql) ) return;
		}

		iReturn = sqlite3_step(m_pRsSqlite);
		eof = iReturn == SQLITE_DONE ? true : false;
	}
}


void mCDatabase::CRs::MoveNext()
{
	if(m_pDb->m_dbtype == "mysql" || m_pDb->m_dbtype == "sqlserver" || m_pDb->m_dbtype == "access")
	{
		try	
		{
			m_pRsAdo->MoveNext();

			eof = m_pRsAdo->adoEOF == VARIANT_TRUE ? true : false;
		}
		catch(_com_error &e)
		{
			eof = true;

			if( m_pDb->CheckError( 507, &e, NULL, L"" ) ) return;
		}
	}

	if(m_pDb->m_dbtype == "sqlite")
	{
		int iReturn = sqlite3_step(m_pRsSqlite);
		eof = iReturn == SQLITE_DONE ? true : false;
	}
}


void mCDatabase::CRs::SetInt(CStringW key, int value)
{
	Json::Value json;
	json.append( utoa(key) );
	json.append(value);
	m_kv.append(json);
}
void mCDatabase::CRs::SetDouble(CStringW key, double value)
{
	Json::Value json;
	json.append( utoa(key) );
	json.append(value);
	m_kv.append(json);
}
void mCDatabase::CRs::SetString(CStringW key, CStringW value)
{
	Json::Value json;
	json.append( utoa(key) );
	json.append( utoa(value) );
	m_kv.append(json);
}
void mCDatabase::CRs::SetString(CStringW key, string value)
{
	Json::Value json;
	json.append( utoa(key) );
	json.append(value);
	m_kv.append(json);
}
void mCDatabase::CRs::SetDateTime(CStringW key, CStringW value, bool bIncludingMS)
{
	if( value == L"" ) ::GetDateTime(value, bIncludingMS);

	Json::Value json;
	json.append( utoa(key) );
	json.append( utoa(value) );
	m_kv.append(json);
}
void mCDatabase::CRs::SetDateTime(CStringW key, string value, bool bIncludingMS)
{
	if(value == "") ::GetDateTime(value, bIncludingMS);

	Json::Value json;
	json.append( utoa(key) );
	json.append(value);
	m_kv.append(json);
}


void mCDatabase::CRs::SetInt(string key, int value)
{
	Json::Value json;
	json.append(key);
	json.append(value);
	m_kv.append(json);
}
void mCDatabase::CRs::SetDouble(string key, double value)
{
	Json::Value json;
	json.append(key);
	json.append(value);
	m_kv.append(json);
}
void mCDatabase::CRs::SetString(string key, CStringW value)
{
	Json::Value json;
	json.append(key);
	json.append( utoa(value) );
	m_kv.append(json);
}
void mCDatabase::CRs::SetString(string key, string value)
{
	Json::Value json;
	json.append(key);
	json.append(value);
	m_kv.append(json);
}
void mCDatabase::CRs::SetDateTime(string key, CStringW value, bool bIncludingMS)
{
	if( value == L"" ) ::GetDateTime(value, bIncludingMS);

	Json::Value json;
	json.append(key);
	json.append( utoa(value) );
	m_kv.append(json);
}
void mCDatabase::CRs::SetDateTime(string key, string value, bool bIncludingMS)
{
	if(value == "") ::GetDateTime(value, bIncludingMS);

	Json::Value json;
	json.append(key);
	json.append(value);
	m_kv.append(json);
}


void mCDatabase::CRs::SetWhere(CStringW where)
{
	m_usWhere = where;
}
void mCDatabase::CRs::SetWhere(string where)
{
	m_asWhere = where;
}


int mCDatabase::CRs::KeyToIndex(CStringW key)
{
	int index = -1;

	if(m_pDb->m_dbtype == "sqlite")
	{
		int size = sqlite3_column_count(m_pRsSqlite);
		for(int i = 0; i < size; i++)
		{
			if( CStringW( sqlite3_column_name(m_pRsSqlite, i) ) == key )
			{
				index = i;
				break;
			}
		}
	}

	return index;
}
int mCDatabase::CRs::KeyToIndex(string key)
{
	int index = -1;

	if(m_pDb->m_dbtype == "sqlite")
	{
		int size = sqlite3_column_count(m_pRsSqlite);
		for(int i = 0; i < size; i++)
		{
			if( string( sqlite3_column_name(m_pRsSqlite, i) ) == key )
			{
				index = i;
				break;
			}
		}
	}

	return index;
}


unsigned int mCDatabase::CRs::GetRecordSize()
{
	if(m_pDb->m_dbtype == "mysql" || m_pDb->m_dbtype == "sqlserver" || m_pDb->m_dbtype == "access")
	{
		try	
		{
			return (unsigned int)m_pRsAdo->RecordCount;
		}
		catch(_com_error &e)
		{
			if( m_pDb->CheckError( 508, &e, NULL, L"GetRecordSize" ) ) return 0;
		}
	}

	if(m_pDb->m_dbtype == "sqlite")
	{
		if(m_bQuery && m_pRsSqlite != NULL)
		{
			unsigned int value = 1;

			while(sqlite3_step(m_pRsSqlite) != SQLITE_DONE)
			{
				value++;
			}

			m_bQuery = false;
			sqlite3_finalize(m_pRsSqlite);

			if(m_usSql != L"") Query(m_usSql);
			if(m_asSql != "") Query(m_asSql);

			return value;
		}
	}

	return 0;
}
unsigned int mCDatabase::CRs::GetColumnSize()
{
	if(m_pDb->m_dbtype == "mysql" || m_pDb->m_dbtype == "sqlserver" || m_pDb->m_dbtype == "access")
	{
		try	
		{
			return (unsigned int)m_pRsAdo->Fields->Count;
		}
		catch(_com_error &e)
		{
			if( m_pDb->CheckError( 508, &e, NULL, L"GetColumnSize" ) ) return 0;
		}
	}

	if(m_pDb->m_dbtype == "sqlite")
	{
		return sqlite3_column_count(m_pRsSqlite);
	}

	return 0;
}
void mCDatabase::CRs::GetColumnName(int index, CStringW &name)
{
	if(m_pDb->m_dbtype == "mysql" || m_pDb->m_dbtype == "sqlserver" || m_pDb->m_dbtype == "access")
	{
		try
		{
			Fields *pFields = NULL;
			m_pRsAdo->get_Fields(&pFields);
			name = (LPCTSTR)pFields->Item[ (long)index ]->GetName();
			pFields->Release();
		}
		catch(_com_error &e)
		{
			if( m_pDb->CheckError( 508, &e, NULL, L"GetColumnName" ) ) return;
		}
	}

	if(m_pDb->m_dbtype == "sqlite")
	{
		name = CStringW( sqlite3_column_name(m_pRsSqlite, index) );
	}
}
void mCDatabase::CRs::GetColumnName(int index, string &name)
{
	if(m_pDb->m_dbtype == "mysql" || m_pDb->m_dbtype == "sqlserver" || m_pDb->m_dbtype == "access")
	{
		try
		{
			Fields *pFields = NULL;
			m_pRsAdo->get_Fields(&pFields);
			name = (LPCSTR)pFields->Item[ (long)index ]->GetName();
			pFields->Release();
		}
		catch(_com_error &e)
		{
			if( m_pDb->CheckError( 508, &e, NULL, L"GetColumnName" ) ) return;
		}
	}

	if(m_pDb->m_dbtype == "sqlite")
	{
		name = string( sqlite3_column_name(m_pRsSqlite, index) );
	}
}


int mCDatabase::CRs::GetInt(CStringW key)
{
	if(m_pDb->m_dbtype == "mysql" || m_pDb->m_dbtype == "sqlserver" || m_pDb->m_dbtype == "access")
	{
		try	
		{
			_variant_t v = m_pRsAdo->GetCollect( (_variant_t)key );

			return v.vt == VT_NULL ? 0 : int(v);
		}
		catch(_com_error &e)
		{
			if( m_pDb->CheckError(508, &e, NULL, key) ) return 0;
		}
	}

	if(m_pDb->m_dbtype == "sqlite")
	{
		return sqlite3_column_int( m_pRsSqlite, KeyToIndex(key) );
	}

	return 0;
}
double mCDatabase::CRs::GetDouble(CStringW key)
{
	if(m_pDb->m_dbtype == "mysql" || m_pDb->m_dbtype == "sqlserver" || m_pDb->m_dbtype == "access")
	{
		try	
		{
			_variant_t v = m_pRsAdo->GetCollect( (_variant_t)key );

			return v.vt == VT_NULL ? 0 : double(v);
		}
		catch(_com_error &e)
		{
			if( m_pDb->CheckError(508, &e, NULL, key) ) return 0;
		}
	}

	if(m_pDb->m_dbtype == "sqlite")
	{
		return sqlite3_column_double( m_pRsSqlite, KeyToIndex(key) );
	}

	return 0;
}
void mCDatabase::CRs::GetString(CStringW key, CStringW &value)
{
	if(m_pDb->m_dbtype == "mysql" || m_pDb->m_dbtype == "sqlserver" || m_pDb->m_dbtype == "access")
	{
		try	
		{
			_variant_t v = m_pRsAdo->GetCollect( (_variant_t)key );

			value = v.vt == VT_NULL ? L"" : LPCTSTR( _bstr_t(v) );
		}
		catch(_com_error &e)
		{
			if( m_pDb->CheckError(508, &e, NULL, key) ) return;
		}
	}

	if(m_pDb->m_dbtype == "sqlite")
	{
		const unsigned char *pStr = sqlite3_column_text( m_pRsSqlite, KeyToIndex(key) );
		value = pStr == NULL ? L"" : CA2W( (char*)pStr, CP_UTF8 );
	}
}
void mCDatabase::CRs::GetString(CStringW key, string &value)
{
	if(m_pDb->m_dbtype == "mysql" || m_pDb->m_dbtype == "sqlserver" || m_pDb->m_dbtype == "access")
	{
		try	
		{
			_variant_t v = m_pRsAdo->GetCollect( (_variant_t)key );

			value = v.vt == VT_NULL ? "" : LPCSTR( LPCTSTR( _bstr_t(v) ) );
		}
		catch(_com_error &e)
		{
			if( m_pDb->CheckError(508, &e, NULL, key) ) return;
		}
	}

	if(m_pDb->m_dbtype == "sqlite")
	{
		const unsigned char *pStr = sqlite3_column_text( m_pRsSqlite, KeyToIndex(key) );
		value = pStr == NULL ? "" : CA2A( (char*)pStr, CP_UTF8 );
	}
}
void mCDatabase::CRs::GetDateTime(CStringW key, CStringW &value)
{
	if(m_pDb->m_dbtype == "mysql" || m_pDb->m_dbtype == "sqlserver" || m_pDb->m_dbtype == "access")
	{
		try	
		{
			_variant_t v = m_pRsAdo->GetCollect( (_variant_t)key );

			if(v.vt == VT_NULL)
			{
				value = L"";
			}
			else
			{
				value = LPCTSTR( _bstr_t(v) );
				int count = value.ReverseFind('.');
				value = value.Left(count == -1 ? value.GetLength() : count);

				value.Replace('-', '/');
				COleDateTime time;
				time.ParseDateTime(value);
				SYSTEMTIME systime;
				time.GetAsSystemTime(systime);
				value.Format(L"%d-%02d-%02d %02d:%02d:%02d",
					systime.wYear, systime.wMonth, systime.wDay,
					systime.wHour, systime.wMinute, systime.wSecond
					);
			}
		}
		catch(_com_error &e)
		{
			if( m_pDb->CheckError(508, &e, NULL, key) ) return;
		}
	}

	if(m_pDb->m_dbtype == "sqlite")
	{
		const unsigned char *pStr = sqlite3_column_text( m_pRsSqlite, KeyToIndex(key) );
		value = CA2W( (char*)pStr, CP_UTF8 );
		int count = value.ReverseFind('.');
		value = value.Left(count == -1 ? value.GetLength() : count);
		value.Replace('/', '-');
	}
}
void mCDatabase::CRs::GetDateTime(CStringW key, string &value)
{
	if(m_pDb->m_dbtype == "mysql" || m_pDb->m_dbtype == "sqlserver" || m_pDb->m_dbtype == "access")
	{
		try	
		{
			_variant_t v = m_pRsAdo->GetCollect( (_variant_t)key );

			if(v.vt == VT_NULL)
			{
				value = "";
			}
			else
			{
				CStringA _value;
				_value = LPCSTR( _bstr_t(v) );
				int count = _value.ReverseFind('.');
				_value = _value.Left(count == -1 ? _value.GetLength() : count);

				_value.Replace('-', '/');
				COleDateTime time;
				time.ParseDateTime( atou(_value) );
				SYSTEMTIME systime;
				time.GetAsSystemTime(systime);
				_value.Format("%d-%02d-%02d %02d:%02d:%02d",
					systime.wYear, systime.wMonth, systime.wDay,
					systime.wHour, systime.wMinute, systime.wSecond
					);

				value = _value;
			}
		}
		catch(_com_error &e)
		{
			if( m_pDb->CheckError(508, &e, NULL, key) ) return;
		}
	}

	if(m_pDb->m_dbtype == "sqlite")
	{
		const unsigned char *pStr = sqlite3_column_text( m_pRsSqlite, KeyToIndex(key) );
		CStringA _value = CA2A( (char*)pStr, CP_UTF8 );
		int count = _value.ReverseFind('.');
		_value = _value.Left(count == -1 ? _value.GetLength() : count);
		_value.Replace('/', '-');

		value = _value;
	}
}


int mCDatabase::CRs::GetInt(string key)
{
	if(m_pDb->m_dbtype == "mysql" || m_pDb->m_dbtype == "sqlserver" || m_pDb->m_dbtype == "access")
	{
		try	
		{
			_variant_t v = m_pRsAdo->GetCollect( (_variant_t)key.c_str() );

			return v.vt == VT_NULL ? 0 : int(v);
		}
		catch(_com_error &e)
		{
			if( m_pDb->CheckError(508, &e, NULL, key) ) return 0;
		}
	}

	if(m_pDb->m_dbtype == "sqlite")
	{
		return sqlite3_column_int( m_pRsSqlite, KeyToIndex(key) );
	}

	return 0;
}
double mCDatabase::CRs::GetDouble(string key)
{
	if(m_pDb->m_dbtype == "mysql" || m_pDb->m_dbtype == "sqlserver" || m_pDb->m_dbtype == "access")
	{
		try	
		{
			_variant_t v = m_pRsAdo->GetCollect( (_variant_t)key.c_str() );

			return v.vt == VT_NULL ? 0 : double(v);
		}
		catch(_com_error &e)
		{
			if( m_pDb->CheckError(508, &e, NULL, key) ) return 0;
		}
	}

	if(m_pDb->m_dbtype == "sqlite")
	{
		return sqlite3_column_double( m_pRsSqlite, KeyToIndex(key) );
	}

	return 0;
}
void mCDatabase::CRs::GetString(string key, CStringW &value)
{
	if(m_pDb->m_dbtype == "mysql" || m_pDb->m_dbtype == "sqlserver" || m_pDb->m_dbtype == "access")
	{
		try	
		{
			_variant_t v = m_pRsAdo->GetCollect( (_variant_t)key.c_str() );

			value = v.vt == VT_NULL ? L"" : atou( _bstr_t(v) );
		}
		catch(_com_error &e)
		{
			if( m_pDb->CheckError(508, &e, NULL, key) ) return;
		}
	}

	if(m_pDb->m_dbtype == "sqlite")
	{
		const unsigned char *pStr = sqlite3_column_text( m_pRsSqlite, KeyToIndex(key) );
		value = pStr == NULL ? L"" : u8tou( (char*)pStr );
	}
}
void mCDatabase::CRs::GetString(string key, string &value)
{
	if(m_pDb->m_dbtype == "mysql" || m_pDb->m_dbtype == "sqlserver" || m_pDb->m_dbtype == "access")
	{
		try	
		{
			_variant_t v = m_pRsAdo->GetCollect( (_variant_t)key.c_str() );

			value = v.vt == VT_NULL ? "" : _bstr_t(v);
		}
		catch(_com_error &e)
		{
			if( m_pDb->CheckError(508, &e, NULL, key) ) return;
		}
	}

	if(m_pDb->m_dbtype == "sqlite")
	{
		const unsigned char *pStr = sqlite3_column_text( m_pRsSqlite, KeyToIndex(key) );
		value = pStr == NULL ? "" : CA2A( (char*)pStr, CP_UTF8 );
	}
}
void mCDatabase::CRs::GetDateTime(string key, CStringW &value)
{
	if(m_pDb->m_dbtype == "mysql" || m_pDb->m_dbtype == "sqlserver" || m_pDb->m_dbtype == "access")
	{
		try	
		{
			_variant_t v = m_pRsAdo->GetCollect( (_variant_t)key.c_str() );

			if(v.vt == VT_NULL)
			{
				value = L"";
			}
			else
			{
				value = LPCTSTR( _bstr_t(v) );
				int count = value.ReverseFind('.');
				value = value.Left(count == -1 ? value.GetLength() : count);

				value.Replace('-', '/');
				COleDateTime time;
				time.ParseDateTime(value);
				SYSTEMTIME systime;
				time.GetAsSystemTime(systime);
				value.Format(L"%d-%02d-%02d %02d:%02d:%02d",
					systime.wYear, systime.wMonth, systime.wDay,
					systime.wHour, systime.wMinute, systime.wSecond
					);
			}
		}
		catch(_com_error &e)
		{
			if( m_pDb->CheckError(508, &e, NULL, key) ) return;
		}
	}

	if(m_pDb->m_dbtype == "sqlite")
	{
		const unsigned char *pStr = sqlite3_column_text( m_pRsSqlite, KeyToIndex(key) );
		value = CA2W( (char*)pStr, CP_UTF8 );
		int count = value.ReverseFind('.');
		value = value.Left(count == -1 ? value.GetLength() : count);
		value.Replace('/', '-');
	}
}
void mCDatabase::CRs::GetDateTime(string key, string &value)
{
	if(m_pDb->m_dbtype == "mysql" || m_pDb->m_dbtype == "sqlserver" || m_pDb->m_dbtype == "access")
	{
		try	
		{
			_variant_t v = m_pRsAdo->GetCollect( (_variant_t)key.c_str() );

			if(v.vt == VT_NULL)
			{
				value = "";
			}
			else
			{
				CStringA _value;
				_value = LPCSTR( _bstr_t(v) );
				int count = _value.ReverseFind('.');
				_value = _value.Left(count == -1 ? _value.GetLength() : count);

				_value.Replace('-', '/');
				COleDateTime time;
				time.ParseDateTime( atou(_value) );
				SYSTEMTIME systime;
				time.GetAsSystemTime(systime);
				_value.Format("%d-%02d-%02d %02d:%02d:%02d",
					systime.wYear, systime.wMonth, systime.wDay,
					systime.wHour, systime.wMinute, systime.wSecond
					);

				value = _value;
			}
		}
		catch(_com_error &e)
		{
			if( m_pDb->CheckError(508, &e, NULL, key) ) return;
		}
	}

	if(m_pDb->m_dbtype == "sqlite")
	{
		const unsigned char *pStr = sqlite3_column_text( m_pRsSqlite, KeyToIndex(key) );
		CStringA _value = CA2A( (char*)pStr, CP_UTF8 );
		int count = _value.ReverseFind('.');
		_value = _value.Left(count == -1 ? _value.GetLength() : count);
		_value.Replace('/', '-');

		value = _value;
	}
}


mCDatabase::mCDatabase()
{
//	::CoInitialize(NULL);

	m_pConnAdo = NULL;
	m_pConnSqlite = NULL;
}
mCDatabase::~mCDatabase()
{
//	::CoUninitialize();
}


bool mCDatabase::CheckError(int iError, _com_error *pe, const char *pErrorMsg, CStringW sql)
{
	CStringW tip = L"";

	switch(iError)
	{
	case 0:break;

	case 201: tip = L"OpenDatabase error"; break;
	case 202: tip = L"CloseDatabase error"; break;

	case 301: tip = L"NewTable error"; break;
	case 302: tip = L"DeleteTable error"; break;

	case 501: tip = L"OpenRs error"; break;
	case 502: tip = L"CloseRs error"; break;
	case 503: tip = L"Add error"; break;
	case 504: tip = L"Delete error"; break;
	case 505: tip = L"Modify error"; break;
	case 506: tip = L"Query error"; break;
	case 507: tip = L"MoveNext error"; break;
	case 508: tip = L"Get... error"; break;
	}

	CStringW sDateTime; ::GetDateTime(sDateTime);

	CStringW sErrorMsg;
	if(m_dbtype == "mysql" || m_dbtype == "sqlserver" || m_dbtype == "access")
	{
		sErrorMsg = (LPCTSTR)pe->Description();
	}
	if(m_dbtype == "sqlite")
	{
		sErrorMsg = CA2W(pErrorMsg, CP_UTF8);
	}

	Log(L"mCDatabase", L"【%s】【%s】【%s】【%s】", sDateTime, tip, sErrorMsg, sql);

	return iError != 0;
}
bool mCDatabase::CheckError(int iError, _com_error *pe, const char *pErrorMsg, string sql)
{
	return CheckError( iError, pe, pErrorMsg, CStringW( sql.c_str() ) );
}


void mCDatabase::OpenDatabase(CStringW OptionFile)
{
	Json::Value option; ::filetojson(OptionFile, option);
	OpenDatabase(option);
}
void mCDatabase::OpenDatabase(Json::Value option)
{
	m_dbtype = option["dbtype"].asCString();

	CStringW ConnectionString = L"";

	if(m_dbtype == "mysql")
	{
		ConnectionString += L"Driver={" + CStringW( option["dbdriver"].asCString() ) + L"};";
		ConnectionString += L"Server=" + CStringW( option["dblocation"].asCString() ) + L";Port=" + CStringW( option["dbport"].asCString() ) + L";";
		ConnectionString += L"UID=" + CStringW( option["dbuid"].asCString() ) + L";";
		ConnectionString += L"PWD=" + CStringW( option["dbpwd"].asCString() ) + L";";
		ConnectionString += L"Database=" + CStringW( option["dbname"].asCString() ) + L";";
	}

	if(m_dbtype == "sqlserver")
	{
		ConnectionString += L"Provider=" + CStringW( option["dbdriver"].asCString() ) +  L";";
		ConnectionString +=  L"Data Source=" + CStringW( option["dblocation"].asCString() ) + L"," + CStringW( option["dbport"].asCString() ) + L";";
		ConnectionString +=  L"UID=" + CStringW( option["dbuid"].asCString() ) +  L";";
		ConnectionString +=  L"PWD=" + CStringW( option["dbpwd"].asCString() ) +  L";";
		ConnectionString +=  L"Initial Catalog=" + CStringW( option["dbname"].asCString() ) +  L";";
	}

	if(m_dbtype == "access")
	{
		ConnectionString += L"Provider=" + CStringW( option["dbdriver"].asCString() ) +  L";";
		ConnectionString += L"Data Source=" + CStringW( option["dblocation"].asCString() ) +  L";";
	}

	if(m_dbtype == "mysql" || m_dbtype == "sqlserver" || m_dbtype == "access")
	{
		try	
		{
			m_pConnAdo.CreateInstance( __uuidof(Connection) );
			m_pConnAdo->Open( _bstr_t(ConnectionString), _bstr_t( L"" ), _bstr_t( L"" ), adModeUnknown );
		}
		catch(_com_error &e)
		{
			if( CheckError(201, &e, NULL, ConnectionString) ) return;
		}
	}

	if(m_dbtype == "sqlite")
	{
		CStringW DatabaseFile = CStringW( option["dblocation"].asCString() );

		::ToAbsolutePath(DatabaseFile);

		int iReturn = sqlite3_open( CT2A(DatabaseFile, CP_UTF8), &m_pConnSqlite );
		if(iReturn != SQLITE_OK)
		{
			m_pConnSqlite = NULL;

			if( CheckError(201, NULL, sqlite3_errmsg(m_pConnSqlite), DatabaseFile) ) return;
		}
	}
}
void mCDatabase::CloseDatabase()
{
	if(m_dbtype == "mysql" || m_dbtype == "sqlserver" || m_dbtype == "access")
	{
		if(m_pConnAdo != NULL)
		{
			try	
			{
				if(m_pConnAdo->State != adStateClosed) m_pConnAdo->Close();

				m_pConnAdo.Release();
				m_pConnAdo = NULL;
			}
			catch(_com_error &e)
			{
				if( CheckError( 202, &e, NULL, L"CloseDatabase()" ) ) return;
			}
		}
	}

	if(m_dbtype == "sqlite")
	{
		if(m_pConnSqlite != NULL)
		{
			int iReturn = sqlite3_close(m_pConnSqlite);

			m_pConnSqlite = NULL;

			if(iReturn != SQLITE_OK)
			{
				if( CheckError( 202, NULL, sqlite3_errmsg(m_pConnSqlite), L"CloseDatabase" ) ) return;
			}
		}
	}
}


void mCDatabase::NewTable(CStringW SqlFile)
{
	if(m_dbtype == "mysql" || m_dbtype == "sqlserver" || m_dbtype == "access")
	{
		CStringW sql;
		::filetostr(SqlFile, sql);

		vector<CStringW> vSql;
		int iFirst = 0;
		int iCount = 0;
		for(int i = 0; i < sql.GetLength(); i++)
		{
			if(sql[i] == ';')
			{
				iCount = i - iFirst + 1;
				vSql.push_back( sql.Mid(iFirst, iCount) );
				iFirst = i + 1;
			}
		}

		for(unsigned int i = 0; i < vSql.size(); i++)
		{
			try	
			{
				m_pConnAdo->Execute( (_bstr_t)vSql[i], NULL, adCmdText );
			}
			catch(_com_error &e)
			{
				if( CheckError(301, &e, NULL, sql) ) return;
			}
		}
	}

	if(m_dbtype == "sqlite")
	{
		string sql;
		::filetostr(SqlFile, sql);

		int iReturn = 0;
		char *pErrorMsg = NULL;

		iReturn = sqlite3_exec(m_pConnSqlite, sql.c_str(), NULL, NULL, &pErrorMsg);
		if(iReturn != SQLITE_OK)
		{
			CheckError(301, NULL, pErrorMsg, CStringW( CA2W(sql.c_str(), CP_UTF8) ));

			sqlite3_free(pErrorMsg);
		}
	}
}
void mCDatabase::DeleteTable(CStringW TableName)
{
	CStringW sql = L"drop table " + TableName;

	if(m_dbtype == "mysql" || m_dbtype == "sqlserver" || m_dbtype == "access")
	{
		try	
		{
			m_pConnAdo->Execute( (_bstr_t)sql, NULL, adCmdText );
		}
		catch(_com_error &e)
		{
			if( CheckError(302, &e, NULL, sql) ) return;
		}
	}

	if(m_dbtype == "sqlite")
	{
		int iReturn = 0;
		char *pErrorMsg = NULL;

		iReturn = sqlite3_exec(m_pConnSqlite, CT2A(sql, CP_UTF8), NULL, NULL, &pErrorMsg);
		if(iReturn != SQLITE_OK)
		{
			CheckError(302, NULL, pErrorMsg, sql);

			sqlite3_free(pErrorMsg);
		}
	}
}


void mCDatabase::DeleteTable(string TableName)
{
	string sql = "drop table " + TableName;

	if(m_dbtype == "mysql" || m_dbtype == "sqlserver" || m_dbtype == "access")
	{
		try	
		{
			m_pConnAdo->Execute( (_bstr_t)sql.c_str(), NULL, adCmdText );
		}
		catch(_com_error &e)
		{
			if( CheckError(302, &e, NULL, sql) ) return;
		}
	}

	if(m_dbtype == "sqlite")
	{
		int iReturn = 0;
		char *pErrorMsg = NULL;

		iReturn = sqlite3_exec(m_pConnSqlite, CA2A(sql.c_str(), CP_UTF8), NULL, NULL, &pErrorMsg);
		if(iReturn != SQLITE_OK)
		{
			CheckError(302, NULL, pErrorMsg, sql);

			sqlite3_free(pErrorMsg);
		}
	}
}


mCDatabase::CRs *mCDatabase::OpenRs()
{
	mCDatabase::CRs *pRs = new mCDatabase::CRs(this);

	return pRs;
}
void mCDatabase::CloseRs(CRs *pRs)
{
	if(pRs != NULL)
	{
		delete pRs;
		pRs = NULL;
	}
}
