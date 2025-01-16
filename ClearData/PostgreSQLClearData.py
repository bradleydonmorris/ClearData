from .Interfaces import *
import platform
import pythonnet
if (platform.system() == "Linux"):
	pythonnet.load("mono")
if (platform.system() == "Windows"):
	pythonnet.load("netfx")
import clr
clr.AddReference("System")
clr.AddReference("System.Data")
from System.Data import DataTable
from System.Data import DataRow
from System.Data import DataColumn
from System.Data import ParameterDirection

import pandas
import psycopg
from psycopg.rows import DictRow, dict_row
import datetime
import json
from pathlib import Path

class PostgreSQLParameter:
	def __init__(self, name:str, dataType:str, direction:ParameterDirection, value, size:int = None):
		self.Name = name
		self.Direction = direction
		self.Value = value

class PostgreSQLStoredProcedures(IStoredProcedures):
	ConnectionString:str = None

	def __init__(self, connectionString:str):
		self.ConnectionString = connectionString

	def Execute(self, schema:str, name:str, parameters:dict = None) -> None:
		exception = None
		try:
			parameterText:str = None
			if (parameters is None):
				parameterText = ""
			else:
				for key in parameters.keys():
					if (parameterText is None):
						parameterText = f"%({key})s"
					else:
						parameterText += f", %({key})s"
			commandText:str = f"SELECT * FROM \"{schema}\".\"{name}\"({parameterText}) AS \"output\""
			with psycopg.connect(self.ConnectionString) as conn:
				with conn.cursor() as cur:
					cur.execute(commandText, parameters)
		except Exception as e:
			exception = e
		if exception is not None:
			raise exception

	def ExecuteWithScalar(self, schema:str, name:str, parameters:dict = None) -> any:
		returnValue:any | None = None
		exception = None
		try:
			parameterText:str = None
			if (parameters is None):
				parameterText = ""
			else:
				for key in parameters.keys():
					if (parameterText is None):
						parameterText = f"%({key})s"
					else:
						parameterText += f", %({key})s"
			commandText:str = f"SELECT * FROM \"{schema}\".\"{name}\"({parameterText}) AS \"output\""
			with psycopg.connect(self.ConnectionString) as conn:
				with conn.cursor() as cur:
					cur.execute(commandText, parameters)
					for row in cur:
						returnValue = row[0]
						break
		except Exception as e:
			exception = e
		if exception is not None:
			raise exception
		else:
			return returnValue

	def ExecuteJSON(self,
			schema:str,
			name:str,
			inputValue:str = None
		) -> str:
		returnValue:str | None = None
		exception = None
		try:
			commandText:str = f"SELECT FROM \"{schema}\".\"{name}\"(%(Input)s) AS \"output\""
			with psycopg.connect(self.ConnectionString) as conn:
				with conn.cursor() as cur:
					cur.execute(commandText, {"Input": inputValue})
					for row in cur:
						returnValue = row[0]
		except Exception as e:
			exception = e
		if exception is not None:
			raise exception
		else:
			return returnValue

	def ExecuteJSONInputOnly(self,
				 schema:str,
				 name:str,
				 inputValue:str = None,
		) -> None:
		exception = None
		try:
			commandText:str = f"SELECT FROM \"{schema}\".\"{name}\"(%(Input)s) AS \"output\""
			with psycopg.connect(self.ConnectionString) as conn:
				with conn.cursor() as cur:
					cur.execute(commandText, {"Input": inputValue})
		except Exception as e:
			exception = e
		if exception is not None:
			raise exception

	def ExecuteJSONOutputOnly(self,
				 schema:str,
				 name:str
		) -> str:
		returnValue:str | None = None
		exception = None
		try:
			commandText:str = f"SELECT FROM \"{schema}\".\"{name}\"() AS \"output\""
			with psycopg.connect(self.ConnectionString) as conn:
				with conn.cursor() as cur:
					cur.execute(commandText)
					for row in cur:
						returnValue = row[0]
		except Exception as e:
			exception = e
		if exception is not None:
			raise exception
		else:
			return returnValue

	def ExecuteDataTable(self, schema:str, name:str, parameters:dict = None) -> DataTable:
		returnValue = None
		exception = None
		try:
			parameterText:str = None
			if (parameters is None):
				parameterText = ""
			else:
				for key in parameters.keys():
					keyValue:str = "NULL"
					if (parameters[key] is not None): keyValue = f"'{parameters[key]}'"
					keyValue = f"\"{key}\"=>{keyValue}"
					if (parameterText is None):
						parameterText = f"{keyValue}"
					else:
						parameterText += f", {keyValue}"
			commandText:str = f"SELECT * FROM \"{schema}\".\"{name}\"({parameterText}) AS \"output\""
			with psycopg.connect(self.ConnectionString) as conn:
				with conn.cursor() as cur:
					cur.execute(commandText, parameters)
					returnValue = DataTable()
					rowCount:int = 0
					for row in cur:
						rowCount += 1
						dataRow = returnValue.NewRow()
						for columnIndex, column in enumerate(cur.description):
							clrDataType:str = PostgreSQLClearData.__GetCLRDataType__(row[columnIndex])
							if (not returnValue.Columns.Contains(column.name)):
								dataColumn:DataColumn = DataColumn(column.name)
								dataColumn.DataType = System.Type.GetType(clrDataType)
								dataColumn.AllowDBNull  = True
								#returnValue.Columns.Add(column.name, System.Type.GetType(clrDataType))
								returnValue.Columns.Add(dataColumn)
							dataRow[columnIndex] = PostgreSQLClearData.__ConvertToCLRDataType__(row[columnIndex], clrDataType)
						returnValue.Rows.Add(dataRow)

		except Exception as e:
			exception = e
		if exception is not None:
			raise exception
		else:
			return returnValue

	def ExecuteDataFrame(self, schema:str, name:str, parameters = None) -> pandas.DataFrame:
		returnValue = pandas.DataFrame()
		returnValue = None
		exception = None
		try:
			parameterText:str = None
			if (parameters is None):
				parameterText = ""
			else:
				for key in parameters.keys():
					keyValue:str = "NULL"
					if (parameters[key] is not None): keyValue = f"'{parameters[key]}'"
					keyValue = f"\"{key}\"=>{keyValue}"
					if (parameterText is None):
						parameterText = f"{keyValue}"
					else:
						parameterText += f", {keyValue}"
			commandText:str = f"SELECT * FROM \"{schema}\".\"{name}\"({parameterText}) AS \"output\""
			with psycopg.connect(self.ConnectionString) as conn:
				with conn.cursor() as cur:
					cur.execute(commandText, parameters)
					data = cur.fetchall()
					returnValue = pandas.DataFrame(data, columns=[desc[0] for desc in cur.description])
		except Exception as e:
			exception = e
		if exception is not None:
			raise exception
		else:
			return returnValue

class PostgreSQLQueries(IQueries):
	ConnectionString:str = None

	def __init__(self, connectionString:str):
		self.ConnectionString = connectionString

	def Execute(self, query:str):
		exception = None
		try:
			commandText:str = query
			with psycopg.connect(self.ConnectionString) as conn:
				with conn.cursor() as cur:
					cur.execute(commandText)
		except Exception as e:
			exception = e
		if exception is not None:
			raise exception

	def ExecuteWithScalar(self, query:str):
		returnValue = None
		exception = None
		try:
			with psycopg.connect(self.ConnectionString) as conn:
				with conn.cursor() as cur:
					cur.execute(query)
					for row in cur:
						returnValue = row[0]
		except Exception as e:
			exception = e
		if exception is not None:
			raise exception
		if returnValue is not None:
			return returnValue

	def ExecuteDataTable(self, query:str) -> DataTable:
		returnValue = None
		exception = None
		try:
			with psycopg.connect(self.ConnectionString) as conn:
				with conn.cursor() as cur:
					cur.execute(query)
					returnValue = DataTable()
					rowCount:int = 0
					for row in cur:
						rowCount += 1
						dataRow = returnValue.NewRow()
						for columnIndex, column in enumerate(cur.description):
							clrDataType:str = PostgreSQLClearData.__GetCLRDataType__(row[columnIndex])
							if (not returnValue.Columns.Contains(column.name)):
								dataColumn:DataColumn = DataColumn(column.name)
								dataColumn.DataType = System.Type.GetType(clrDataType)
								dataColumn.AllowDBNull  = True
								returnValue.Columns.Add(dataColumn)
							dataRow[columnIndex] = PostgreSQLClearData.__ConvertToCLRDataType__(row[columnIndex], clrDataType)
						returnValue.Rows.Add(dataRow)

		except Exception as e:
			exception = e
		if exception is not None:
			raise exception
		else:
			return returnValue

	def ExecuteDataFrame(self, query:str) -> pandas.DataFrame:
		returnValue = pandas.DataFrame()
		returnValue = None
		exception = None
		try:
			with psycopg.connect(self.ConnectionString) as conn:
				with conn.cursor() as cur:
					cur.execute(query)
					data = cur.fetchall()
					returnValue = pandas.DataFrame(data, columns=[desc[0] for desc in cur.description])
		except Exception as e:
			exception = e
		if exception is not None:
			raise exception
		else:
			return returnValue

	def TruncateTable(self, schema:str, table:str):
		exception = None
		try:
			commandText:str = f"TRUNCATE TABLE {schema}.{table}"
			with psycopg.connect(self.ConnectionString) as conn:
				with conn.cursor() as cur:
					cur.execute(commandText)
		except Exception as e:
			exception = e
		if exception is not None:
			raise exception

	def GetRowCount(self, schema:str, table:str) -> int:
		returnValue:int = None
		exception = None
		try:
			with psycopg.connect(self.ConnectionString) as conn:
				with conn.cursor() as cur:
					cur.execute(f"SELECT COUNT(*) AS \"Count\" FROM \"{schema}\".\"{table}\"")
					returnValue = int(cur.fetchone()[0])
		except Exception as e:
			exception = e
		if exception is not None:
			raise exception
		if returnValue is not None:
			return returnValue

class PostgreSQLBulkData(IBulkData):
	ConnectionString:str = None

	def __init__(self, connectionString:str):
		self.ConnectionString = connectionString

	def GetDataTable(
		self,
		schema:str = None,
		tableOrView:str = None,
		options:dict = {"UseBinaryFormat": False, "CLRTypeOverride": {} }
	):
		returnValue = None
		exception = None
		commandText:str = f"SELECT * FROM \"{schema}\".\"{tableOrView}\""
		useBinaryFormat:bool = False
		clrTypeOverride:dict = {}
		if ("BatchSize" in options):
			useBinaryFormat = bool(options["UseBinaryFormat"])
		if ("CLRTypeOverride" in options):
			if (isinstance(options["CLRTypeOverride"], dict)):
				clrTypeOverride = options["CLRTypeOverride"]
		try:
			with psycopg.connect(self.ConnectionString) as conn:
				with conn.cursor() as cur:
					cur.execute(commandText)
					returnValue = DataTable()
					rowCount:int = 0
					for row in cur:
						rowCount += 1
						dataRow = returnValue.NewRow()
						for columnIndex, column in enumerate(cur.description):
							clrDataType:str = None
							if (column.name not in clrTypeOverride):
								clrDataType:str = PostgreSQLClearData.__GetCLRDataType__(row[columnIndex])
							else:
								clrDataType = clrTypeOverride[column.name]
							if (not returnValue.Columns.Contains(column.name)):
								dataColumn:DataColumn = DataColumn(column.name)
								dataColumn.DataType = System.Type.GetType(clrDataType)
								dataColumn.AllowDBNull  = True
								#returnValue.Columns.Add(column.name, System.Type.GetType(clrDataType))
								returnValue.Columns.Add(dataColumn)
							dataRow[columnIndex] = PostgreSQLClearData.__ConvertToCLRDataType__(row[columnIndex], clrDataType)
						returnValue.Rows.Add(dataRow)
		except Exception as e:
			exception = e
		if exception is not None:
			raise exception
		elif returnValue is not None:
			return returnValue

	def WriteDataTable(self, schema:str, table:str, dataTable:DataTable, options:dict = {"UseBinaryFormat": False}):
		exception = None
		binaryFormat:str = ""
		useBinaryFormat:bool = False
		if ("BatchSize" in options):
			useBinaryFormat = bool(options["UseBinaryFormat"])
		if (useBinaryFormat):
			binaryFormat = " (FORMAT BINARY)"
		try:
			columnList:str = None
			for column in dataTable.Columns:
				if (columnList is None):
					columnList = f"\"{column.ColumnName}\""
				else:
					columnList += f", \"{column.ColumnName}\""
			with psycopg.connect(self.ConnectionString) as conn:
				with conn.cursor() as cur:
					returnValue = DataTable()
					with cur.copy(f"COPY \"{schema}\".\"{table}\" ({columnList}) FROM STDOUT{binaryFormat}") as copy:
						for row in dataTable.Rows:
							rowData = []
							for item in row.ItemArray:
								if (isinstance(item, System.DBNull)):
									rowData.append(None)
								elif (isinstance(item, System.DateTime)):
									rowData.append(datetime.datetime.fromisoformat(item.ToString("yyyy-MM-ddTHH:mm:ss.fffffff")))
								else:
									rowData.append(item)
							copy.write_row(rowData)
		except Exception as e:
			exception = e
		if exception is not None:
			raise exception

	def GetDataFrame(self, schema:str = None, tableOrView:str = None, options:dict = None) -> pandas.DataFrame:
		raise NotImplementedError()

	def WriteDataFrame(self, schema:str, table:str, dataFrame:pandas.DataFrame, options:dict = None):
		raise NotImplementedError()

class PostgreSQLClearData:
	ConnectionString:str = None

	def __init__(self, connectionString:str):
		self.ConnectionString = connectionString
		self.StoredProcedures = PostgreSQLStoredProcedures(connectionString)
		self.Queries = PostgreSQLQueries(connectionString)
		self.BulkData = PostgreSQLBulkData(connectionString)

	def __GetTestConnectionQuery__(self) -> str:
		return """
			SELECT ROW_TO_JSON(\"Sections\")
				FROM
				(
					SELECT
						(
							SELECT ROW_TO_JSON(\"Client\")
								FROM
								(
									SELECT
										CURRENT_SETTING('application_name') AS \"ApplicationName\",
										inet_client_addr() AS \"HostAddress\",
										inet_client_port() AS \"HostPort\",
										CURRENT_ROLE AS \"Role\",
										CURRENT_USER AS \"CurrentUser\",
										USER AS \"User\"
								) AS \"Client\"
						) AS \"Client\",
						(
							SELECT ROW_TO_JSON(\"Server\")
								FROM
								(
									SELECT
										inet_server_addr() AS \"HostAddress\",
										inet_server_port() AS \"HostPort\",
										CURRENT_SETTING('SERVER_VERSION') AS \"Version\"
								) AS \"Server\"
						) AS \"Server\",
						(
							SELECT ROW_TO_JSON(\"Database\")
								FROM
								(
									SELECT
										current_database()  AS \"Name\"
								) AS \"Database\"
						) AS \"Database\"
				) AS \"Sections\"
		"""

	def TestConnection(self) -> dict:
		returnValue:dict = None
		commandText:str = self.__GetTestConnectionQuery__()
		with open(Path(__file__).parent.joinpath("PostgreSQLFunctions.TestConnection.sql"), "r") as file:
			commandText = file.read()
		exception = None
		try:
			with psycopg.connect(self.ConnectionString) as conn:
				with conn.cursor() as cur:
					cur.execute(commandText)
					result = cur.fetchone()[0]
					if (result):
						returnValue = json.loads(result)
		except Exception as e:
			exception = e
		if exception is not None:
			raise exception
		return returnValue

	@staticmethod
	def __GetDataColumn__(name:str, clrDataType:str):
		dataColumn:DataColumn = DataColumn(name)
		dataColumn = System.Type.GetType(clrDataType)
		return dataColumn

	@staticmethod
	def __GetCLRDataType__(obj):
		if (isinstance(obj, bool)):
			return "System.Boolean"
		elif (isinstance(obj, datetime.datetime)):
			return "System.DateTime"
		elif (isinstance(obj, int)):
			if (obj > 2147483647):
				return "System.Int64"
			else:
				return "System.Int32"
		else:
			return "System.String"

	@staticmethod
	def __ConvertToCLRDataType__(obj, exceptedType:str = None):
		returnValue = None
		if (obj is None):
			returnValue = System.DBNull.Value
		else:
			if (exceptedType is None):
				exceptedType = BulkData.__GetCLRDataType__(obj)
			match exceptedType:
				case "System.Boolean":
					returnValue = System.Convert.ToBoolean(obj)
				case "System.DateTime":
					returnValue = System.Convert.ToDateTime(obj.isoformat())
				case "System.Int32":
					returnValue = System.Convert.ToInt32(obj)
				case "System.Int64":
					returnValue = System.Convert.ToInt64(obj)
				case _:
					returnValue = System.Convert.ToString(obj)
		return returnValue

__all__ = ["PostgreSQLParameter", "PostgreSQLStoredProcedures", "PostgreSQLQueries", "PostgreSQLBulkData", "PostgreSQLClearData"]
