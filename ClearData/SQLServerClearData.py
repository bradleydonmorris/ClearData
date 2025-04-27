from .Interfaces import *
import platform
import pythonnet
if (platform.system() == "Linux"):
	pythonnet.load("mono")
if (platform.system() == "Windows"):
	pythonnet.load("netfx")
import clr
clr.AddReference("System.Data")
from System.Data import DataTable
from System.Data import DataRow
from System.Data import DataColumn
from System.Data import ParameterDirection
from System import DateTime
from System.Data import *
from System.Data import CommandType
from System.Data import SqlClient
from System.Data import SqlDbType
from System.Data import ParameterDirection

import pandas
import json
from pathlib import Path

class SQLServerParameter:
	def __init__(self, name:str, sqlDataType:SqlDbType, direction:ParameterDirection, value = None, size:int = None):
		self.Name = name
		self.SqlDbType = sqlDataType
		self.Direction = direction
		self.Value = value
		self.Size = size

	@staticmethod
	def CreateBitInput(name:str, value:bool):
		return SQLServerParameter(name, SqlDbType.Bit, ParameterDirection.Input, value, None)

	@staticmethod
	def CreateBitOutput(name:str):
		return SQLServerParameter(name, SqlDbType.Bit, ParameterDirection.Output, None, None)

	@staticmethod
	def CreateIntInput(name:str, value:int):
		return SQLServerParameter(name, SqlDbType.Int, ParameterDirection.Input, value, None)

	@staticmethod
	def CreateIntOutput(name:str):
		return SQLServerParameter(name, SqlDbType.Int, ParameterDirection.Output, None, None)

	@staticmethod
	def CreateBigIntInput(name:str, value:int):
		return SQLServerParameter(name, SqlDbType.BigInt, ParameterDirection.Input, value, None)

	@staticmethod
	def CreateBigIntOutput(name:str):
		return SQLServerParameter(name, SqlDbType.BigInt, ParameterDirection.Output, None, None)

	@staticmethod
	def CreateNVarCharInput(name:str, size:int, value:str):
		return SQLServerParameter(name, SqlDbType.NVarChar, ParameterDirection.Input, value, size)

	@staticmethod
	def CreateNVarCharOutput(name:str, size:int):
		return SQLServerParameter(name, SqlDbType.NVarChar, ParameterDirection.Output, None, size)

	@staticmethod
	def CreateVarCharInput(name:str, size:int, value:str):
		return SQLServerParameter(name, SqlDbType.VarChar, ParameterDirection.Input, value, size)

	@staticmethod
	def CreateVarCharOutput(name:str, size:int):
		return SQLServerParameter(name, SqlDbType.VarChar, ParameterDirection.Output, None, size)

	@staticmethod
	def CreateSysNameInput(name:str, value:str):
		return SQLServerParameter(name, SqlDbType.NVarChar, ParameterDirection.Input, value, 128)

	@staticmethod
	def CreateSysNameOutput(name:str):
		return SQLServerParameter(name, SqlDbType.NVarChar, ParameterDirection.Output, None, 128)

	@staticmethod
	def CreateDateTime2Input(name:str, size:int, value:DateTime):
		if size not in range(0, 8):
			raise ValueError('Size must be an integer between 0 and 7.')
		return SQLServerParameter(name, SqlDbType.DateTime2, ParameterDirection.Input, value, size)

	@staticmethod
	def CreateDateTime2Output(sqlCommand:SqlClient.SqlCommand, name:str, size:int):
		if size not in range(0, 8):
			raise ValueError('Size must be an integer between 0 and 7.')
		return SQLServerParameter(name, SqlDbType.DateTime2, ParameterDirection.Output, None, size)

class _SQLServerParametersHelpers:
	@staticmethod
	def GetParameter(sqlCommand:SqlClient.SqlCommand, name:str):
		returnValue = [ p for p in sqlCommand.Parameters if p.ParameterName == name ][0]
		return returnValue

	@staticmethod
	def AddParameters(sqlCommand:SqlClient.SqlCommand, parameters:list[any]):
		for parameter in parameters:
			if (type(parameter).__name__ != "SQLServerParameter"):
				raise ValueError(f"paramteres must be a list of SQLServerParameter objects. {type(parameter).__name__} is not valid.")
			match parameter.SqlDbType:
				case SqlDbType.Bit:
					if parameter.Direction == ParameterDirection.Input:
						_SQLServerParametersHelpers.AddBitInput(sqlCommand, parameter.Name, parameter.Value)
					elif parameter.Direction == ParameterDirection.Output:
						_SQLServerParametersHelpers.AddBitOutput(sqlCommand, parameter.Name)
				case SqlDbType.Int:
					if parameter.Direction == ParameterDirection.Input:
						_SQLServerParametersHelpers.AddIntInput(sqlCommand, parameter.Name, parameter.Value)
					elif parameter.Direction == ParameterDirection.Output:
						_SQLServerParametersHelpers.AddIntOutput(sqlCommand, parameter.Name)
				case SqlDbType.BigInt:
					if parameter.Direction == ParameterDirection.Input:
						_SQLServerParametersHelpers.AddBigIntInput(sqlCommand, parameter.Name, parameter.Value)
					elif parameter.Direction == ParameterDirection.Output:
						_SQLServerParametersHelpers.AddBigIntOutput(sqlCommand, parameter.Name)
				case SqlDbType.NVarChar:
					if parameter.Direction == ParameterDirection.Input:
						_SQLServerParametersHelpers.AddNVarCharInput(sqlCommand, parameter.Name, parameter.Size, parameter.Value)
					elif parameter.Direction == ParameterDirection.Output:
						_SQLServerParametersHelpers.AddNVarCharOutput(sqlCommand, parameter.Name, parameter.Size)
				case SqlDbType.VarChar:
					if parameter.Direction == ParameterDirection.Input:
						_SQLServerParametersHelpers.AddVarCharInput(sqlCommand, parameter.Name, parameter.Size, parameter.Value)
					elif parameter.Direction == ParameterDirection.Output:
						_SQLServerParametersHelpers.AddVarCharOutput(sqlCommand, parameter.Name, parameter.Size)
				case SqlDbType.DateTime2:
					if parameter.Direction == ParameterDirection.Input:
						_SQLServerParametersHelpers.AddDateTime2Input(sqlCommand, parameter.Name, parameter.Size, parameter.Value)
					elif parameter.Direction == ParameterDirection.Output:
						_SQLServerParametersHelpers.AddDateTime2Output(sqlCommand, parameter.Name, parameter.Size)
				case _:
					raise TypeError(f"Unhandled SqlDbType ({parameter.SqlDbType})")

	@staticmethod
	def AddBitInput(sqlCommand:SqlClient.SqlCommand, name:str, value:bool):
		sqlParameter = sqlCommand.CreateParameter()
		sqlParameter.ParameterName = name
		sqlParameter.SqlDbType = SqlDbType.Bit
		sqlParameter.Direction = ParameterDirection.Input
		sqlParameter.SqlValue = value
		sqlCommand.Parameters.Add(sqlParameter)

	@staticmethod
	def AddBitOutput(sqlCommand:SqlClient.SqlCommand, name:str):
		sqlParameter = sqlCommand.CreateParameter()
		sqlParameter.ParameterName = name
		sqlParameter.SqlDbType = SqlDbType.Bit
		sqlParameter.Direction = ParameterDirection.Output
		sqlCommand.Parameters.Add(sqlParameter)

	@staticmethod
	def AddIntInput(sqlCommand:SqlClient.SqlCommand, name:str, value:int):
		sqlParameter = sqlCommand.CreateParameter()
		sqlParameter.ParameterName = name
		sqlParameter.SqlDbType = SqlDbType.Int
		sqlParameter.Direction = ParameterDirection.Input
		sqlParameter.SqlValue = value
		sqlCommand.Parameters.Add(sqlParameter)

	@staticmethod
	def AddIntOutput(sqlCommand:SqlClient.SqlCommand, name:str):
		sqlParameter = sqlCommand.CreateParameter()
		sqlParameter.ParameterName = name
		sqlParameter.SqlDbType = SqlDbType.Int
		sqlParameter.Direction = ParameterDirection.Output
		sqlCommand.Parameters.Add(sqlParameter)

	@staticmethod
	def AddBigIntInput(sqlCommand:SqlClient.SqlCommand, name:str, value:int):
		sqlParameter = sqlCommand.CreateParameter()
		sqlParameter.ParameterName = name
		sqlParameter.SqlDbType = SqlDbType.BigInt
		sqlParameter.Direction = ParameterDirection.Input
		sqlParameter.SqlValue = value
		sqlCommand.Parameters.Add(sqlParameter)

	@staticmethod
	def AddBigIntOutput(sqlCommand:SqlClient.SqlCommand, name:str):
		sqlParameter = sqlCommand.CreateParameter()
		sqlParameter.ParameterName = name
		sqlParameter.SqlDbType = SqlDbType.BigInt
		sqlParameter.Direction = ParameterDirection.Output
		sqlCommand.Parameters.Add(sqlParameter)

	@staticmethod
	def AddNVarCharInput(sqlCommand:SqlClient.SqlCommand, name:str, size:int, value:str):
		sqlParameter = sqlCommand.CreateParameter()
		sqlParameter.ParameterName = name
		sqlParameter.SqlDbType = SqlDbType.NVarChar
		sqlParameter.Direction = ParameterDirection.Input
		sqlParameter.Size = size
		sqlParameter.SqlValue = value
		sqlCommand.Parameters.Add(sqlParameter)

	@staticmethod
	def AddNVarCharOutput(sqlCommand:SqlClient.SqlCommand, name:str, size:int):
		sqlParameter = sqlCommand.CreateParameter()
		sqlParameter.ParameterName = name
		sqlParameter.SqlDbType = SqlDbType.NVarChar
		sqlParameter.Direction = ParameterDirection.Output
		sqlParameter.Size = size
		sqlCommand.Parameters.Add(sqlParameter)

	@staticmethod
	def AddVarCharInput(sqlCommand:SqlClient.SqlCommand, name:str, size:int, value:str):
		sqlParameter = sqlCommand.CreateParameter()
		sqlParameter.ParameterName = name
		sqlParameter.SqlDbType = SqlDbType.VarChar
		sqlParameter.Direction = ParameterDirection.Input
		sqlParameter.Size = size
		sqlParameter.SqlValue = value
		sqlCommand.Parameters.Add(sqlParameter)

	@staticmethod
	def AddVarCharOutput(sqlCommand:SqlClient.SqlCommand, name:str, size:int):
		sqlParameter = sqlCommand.CreateParameter()
		sqlParameter.ParameterName = name
		sqlParameter.SqlDbType = SqlDbType.VarChar
		sqlParameter.Direction = ParameterDirection.Output
		sqlParameter.Size = size
		sqlCommand.Parameters.Add(sqlParameter)

	@staticmethod
	def AddSysNameInput(sqlCommand:SqlClient.SqlCommand, name:str, value:str):
		sqlParameter = sqlCommand.CreateParameter()
		sqlParameter.ParameterName = name
		sqlParameter.SqlDbType = SqlDbType.NVarChar
		sqlParameter.Direction = ParameterDirection.Input
		sqlParameter.Size = 128
		sqlParameter.SqlValue = value
		sqlCommand.Parameters.Add(sqlParameter)

	@staticmethod
	def AddSysNameOutput(sqlCommand:SqlClient.SqlCommand, name:str):
		sqlParameter = sqlCommand.CreateParameter()
		sqlParameter.ParameterName = name
		sqlParameter.SqlDbType = SqlDbType.NVarChar
		sqlParameter.Direction = ParameterDirection.Output
		sqlParameter.Size = 128
		sqlCommand.Parameters.Add(sqlParameter)

	@staticmethod
	def AddDateTime2Input(sqlCommand:SqlClient.SqlCommand, name:str, size:int, value:DateTime):
		if size not in range(0, 8):
			raise ValueError('Size must be an integer between 0 and 7.')
		sqlParameter = sqlCommand.CreateParameter()
		sqlParameter.ParameterName = name
		sqlParameter.SqlDbType = SqlDbType.DateTime2
		sqlParameter.Direction = ParameterDirection.Input
		sqlParameter.Size = size
		sqlParameter.SqlValue = value
		sqlCommand.Parameters.Add(sqlParameter)

	@staticmethod
	def AddDateTime2Output(sqlCommand:SqlClient.SqlCommand, name:str, size:int):
		if size not in range(0, 8):
			raise ValueError('Size must be an integer between 0 and 7.')
		sqlParameter = sqlCommand.CreateParameter()
		sqlParameter.ParameterName = name
		sqlParameter.SqlDbType = SqlDbType.DateTime2
		sqlParameter.Direction = ParameterDirection.Output
		sqlParameter.Size = size
		sqlCommand.Parameters.Add(sqlParameter)

class SQLServerStoredProcedures(IStoredProcedures):
	ConnectionString:str = None

	def __init__(self, connectionString:str):
		self.ConnectionString = connectionString

	def Execute(self, schema:str, name:str, parameters:list[any] = None):
		exception = None
		sqlConnection = None
		sqlCommand = None
		try:
			sqlConnection = SqlClient.SqlConnection(self.ConnectionString)
			sqlConnection.Open()
			sqlCommand = SqlClient.SqlCommand(f"[{schema}].[{name}]", sqlConnection)
			sqlCommand.CommandType = CommandType.StoredProcedure
			sqlCommand.CommandTimeout = 0
			if (parameters is not None):
				_SQLServerParametersHelpers.AddParameters(sqlCommand, parameters)
			sqlCommand.ExecuteNonQuery()
		except Exception as e:
			exception = e
		finally:
			if sqlCommand:
				sqlCommand.Dispose()
			if sqlConnection:
				sqlConnection.Close()
				sqlConnection.Dispose()
		if exception is not None:
			raise exception

	def ExecuteWithScalar(self, schema:str, name:str, parameters = None) -> any:
		returnValue = None
		exception = None
		sqlConnection = None
		sqlCommand = None
		try:
			sqlConnection = SqlClient.SqlConnection(self.ConnectionString)
			sqlConnection.Open()
			sqlCommand = SqlClient.SqlCommand(f"[{schema}].[{name}]", sqlConnection)
			sqlCommand.CommandType = CommandType.StoredProcedure
			sqlCommand.CommandTimeout = 0
			if (parameters is not None):
				_SQLServerParametersHelpers.AddParameters(sqlCommand, parameters)
			returnValue = sqlCommand.ExecuteScalar()
		except Exception as e:
			exception = e
		finally:
			if sqlCommand:
				sqlCommand.Dispose()
			if sqlConnection:
				sqlConnection.Close()
				sqlConnection.Dispose()
		if exception is not None:
			raise exception
		if returnValue is not None:
			return returnValue

	def ExecuteJSONNamed(self,
				 schema:str,
				 name:str,
				 inputParameterName:str = "Input",
				 inputValue:str = None,
				 outputParameterName:str = "Output"
		) -> str:
		returnValue:str = None
		exception = None
		sqlConnection = None
		sqlCommand = None
		try:
			sqlConnection = SqlClient.SqlConnection(self.ConnectionString)
			sqlConnection.Open()
			sqlCommand = SqlClient.SqlCommand(f"[{schema}].[{name}]", sqlConnection)
			sqlCommand.CommandType = CommandType.StoredProcedure
			sqlCommand.CommandTimeout = 0
			if (inputParameterName is not None):
				_SQLServerParametersHelpers.AddNVarCharInput(sqlCommand, inputParameterName, -1, inputValue)
			if (outputParameterName is not None):
				_SQLServerParametersHelpers.AddNVarCharOutput(sqlCommand, outputParameterName, -1)
			sqlCommand.ExecuteNonQuery()
			if (outputParameterName is not None):
				outputParameter = _SQLServerParametersHelpers.GetParameter(sqlCommand, outputParameterName)
				if (outputParameter):
					if (outputParameter.Value):
						if (type(outputParameter.Value) == str):
							returnValue = str(outputParameter.Value)
		except Exception as e:
			exception = e
		finally:
			if sqlCommand:
				sqlCommand.Dispose()
			if sqlConnection:
				sqlConnection.Close()
				sqlConnection.Dispose()
		if exception is not None:
			raise exception
		if returnValue is not None:
			return returnValue

	def ExecuteJSON(self,
			schema:str,
			name:str,
			inputValue:str = None
		) -> str:
		returnValue:str = None
		exception = None
		sqlConnection = None
		sqlCommand = None
		try:
			sqlConnection = SqlClient.SqlConnection(self.ConnectionString)
			sqlConnection.Open()
			sqlCommand = SqlClient.SqlCommand(f"[{schema}].[{name}]", sqlConnection)
			sqlCommand.CommandType = CommandType.StoredProcedure
			sqlCommand.CommandTimeout = 0
			_SQLServerParametersHelpers.AddNVarCharInput(sqlCommand, "Input", -1, inputValue)
			_SQLServerParametersHelpers.AddNVarCharOutput(sqlCommand, "Output", -1)
			sqlCommand.ExecuteNonQuery()
			outputParameter = _SQLServerParametersHelpers.GetParameter(sqlCommand, "Output")
			if (outputParameter):
				if (outputParameter.Value):
					if (type(outputParameter.Value) == str):
						returnValue = str(outputParameter.Value)
		except Exception as e:
			exception = e
		finally:
			if sqlCommand:
				sqlCommand.Dispose()
			if sqlConnection:
				sqlConnection.Close()
				sqlConnection.Dispose()
		if exception is not None:
			raise exception
		if returnValue is not None:
			return returnValue

	def ExecuteJSONInputOnly(self,
				 schema:str,
				 name:str,
				 inputValue:str = None,
		):
		exception = None
		sqlConnection = None
		sqlCommand = None
		try:
			sqlConnection = SqlClient.SqlConnection(self.ConnectionString)
			sqlConnection.Open()
			sqlCommand = SqlClient.SqlCommand(f"[{schema}].[{name}]", sqlConnection)
			sqlCommand.CommandType = CommandType.StoredProcedure
			sqlCommand.CommandTimeout = 0
			_SQLServerParametersHelpers.AddNVarCharInput(sqlCommand, "Input", -1, inputValue)
			sqlCommand.ExecuteNonQuery()
		except Exception as e:
			exception = e
		finally:
			if sqlCommand:
				sqlCommand.Dispose()
			if sqlConnection:
				sqlConnection.Close()
				sqlConnection.Dispose()
		if exception is not None:
			raise exception

	def ExecuteJSONOutputOnly(self,
				 schema:str,
				 name:str
		) -> str:
		returnValue:str = None
		exception = None
		sqlConnection = None
		sqlCommand = None
		try:
			sqlConnection = SqlClient.SqlConnection(self.ConnectionString)
			sqlConnection.Open()
			sqlCommand = SqlClient.SqlCommand(f"[{schema}].[{name}]", sqlConnection)
			sqlCommand.CommandType = CommandType.StoredProcedure
			sqlCommand.CommandTimeout = 0
			_SQLServerParametersHelpers.AddNVarCharOutput(sqlCommand, "Output", -1)
			sqlCommand.ExecuteNonQuery()
			outputParameter = _SQLServerParametersHelpers.GetParameter(sqlCommand, "Output")
			if (outputParameter):
				if (outputParameter.Value):
					if (type(outputParameter.Value) == str):
						returnValue = str(outputParameter.Value)
		except Exception as e:
			exception = e
		finally:
			if sqlCommand:
				sqlCommand.Dispose()
			if sqlConnection:
				sqlConnection.Close()
				sqlConnection.Dispose()
		if exception is not None:
			raise exception
		if returnValue is not None:
			return returnValue

	def ExecuteDataTable(self, schema:str, name:str, parameters = None) -> DataTable:
		returnValue = DataTable()
		exception = None
		sqlConnection = None
		sqlCommand = None
		try:
			sqlConnection = SqlClient.SqlConnection(self.ConnectionString)
			sqlConnection.Open()
			sqlCommand = SqlClient.SqlCommand(f"[{schema}].[{name}]", sqlConnection)
			sqlCommand.CommandType = CommandType.StoredProcedure
			sqlCommand.CommandTimeout = 0
			if (parameters is not None):
				_SQLServerParametersHelpers.AddParameters(sqlCommand, parameters)
			returnValue.Load(sqlCommand.ExecuteReader())
		except Exception as e:
			exception = e
		finally:
			if sqlCommand:
				sqlCommand.Dispose()
			if sqlConnection:
				sqlConnection.Close()
				sqlConnection.Dispose()
		if exception is not None:
			raise exception
		return returnValue

	def ExecuteDataFrame(self, schema:str, name:str, parameters = None) -> pandas.DataFrame:
		returnValue = pandas.DataFrame()
		exception = None
		sqlConnection = None
		sqlCommand = None
		data:dict = dict()
		try:
			sqlConnection = SqlClient.SqlConnection(self.ConnectionString)
			sqlConnection.Open()
			
			#sqlCommand = SqlClient.SqlCommand(f"SET ANSI_NULLS, ANSI_WARNINGS ON", sqlConnection)
			#sqlCommand.CommandType = CommandType.Text
			#sqlCommand.ExecuteNonQuery()

			sqlCommand = SqlClient.SqlCommand(f"[{schema}].[{name}]", sqlConnection)
			sqlCommand.CommandType = CommandType.StoredProcedure
			sqlCommand.CommandTimeout = 0
			if (parameters is not None):
				_SQLServerParametersHelpers.AddParameters(sqlCommand, parameters)
			sqlDataReader = sqlCommand.ExecuteReader()
			isFirstRow:bool = True
			while (sqlDataReader.Read()):
				if (isFirstRow):
					for fieldIndex in range(sqlDataReader.FieldCount):
						data.update({ sqlDataReader.GetName(fieldIndex): list() })
					isFirstRow = False
				for fieldIndex in range(sqlDataReader.FieldCount):
					data[sqlDataReader.GetName(fieldIndex)].append(sqlDataReader.GetValue(fieldIndex))
			returnValue = pandas.DataFrame(data)
		except Exception as e:
			exception = e
		finally:
			if sqlCommand:
				sqlCommand.Dispose()
			if sqlConnection:
				sqlConnection.Close()
				sqlConnection.Dispose()
		if exception is not None:
			raise exception
		return returnValue

class SQLServerQueries(IQueries):
	ConnectionString:str = None

	def __init__(self, connectionString:str):
		self.ConnectionString = connectionString

	def Execute(self, query:str, parameters:list[any] = None):
		exception = None
		sqlConnection = None
		sqlCommand = None
		try:
			sqlConnection = SqlClient.SqlConnection(self.ConnectionString)
			sqlConnection.Open()
			sqlCommand = SqlClient.SqlCommand(query, sqlConnection)
			sqlCommand.CommandType = CommandType.Text
			sqlCommand.CommandTimeout = 0
			if (parameters is not None):
				_SQLServerParametersHelpers.AddParameters(sqlCommand, parameters)
			sqlCommand.ExecuteNonQuery()
		except Exception as e:
			exception = e
		finally:
			if sqlCommand:
				sqlCommand.Dispose()
			if sqlConnection:
				sqlConnection.Close()
				sqlConnection.Dispose()
		if exception is not None:
			raise exception

	def ExecuteWithScalar(self, query:str, parameters:list[any] = None):
		returnValue = None
		exception = None
		sqlConnection = None
		sqlCommand = None
		try:
			sqlConnection = SqlClient.SqlConnection(self.ConnectionString)
			sqlConnection.Open()
			sqlCommand = SqlClient.SqlCommand(query, sqlConnection)
			sqlCommand.CommandType = CommandType.Text
			sqlCommand.CommandTimeout = 0
			if (parameters is not None):
				_SQLServerParametersHelpers.AddParameters(sqlCommand, parameters)
			returnValue = sqlCommand.ExecuteScalar()
		except Exception as e:
			exception = e
		finally:
			if sqlCommand:
				sqlCommand.Dispose()
			if sqlConnection:
				sqlConnection.Close()
				sqlConnection.Dispose()
		if returnValue is not None:
			return returnValue
		elif exception is not None:
			raise exception

	def ExecuteDataTable(self, query:str) -> DataTable:
		returnValue = DataTable()
		exception = None
		sqlConnection = None
		sqlCommand = None
		try:
			sqlConnection = SqlClient.SqlConnection(self.ConnectionString)
			sqlConnection.Open()
			sqlCommand = SqlClient.SqlCommand(query, sqlConnection)
			sqlCommand.CommandType = CommandType.Text
			sqlCommand.CommandTimeout = 0
			returnValue.Load(sqlCommand.ExecuteReader())
		except Exception as e:
			exception = e
		finally:
			if sqlCommand:
				sqlCommand.Dispose()
			if sqlConnection:
				sqlConnection.Close()
				sqlConnection.Dispose()
		if exception is not None:
			raise exception
		return returnValue

	def ExecuteDataFrame(self, query:str) -> pandas.DataFrame:
		returnValue = pandas.DataFrame()
		exception = None
		sqlConnection = None
		sqlCommand = None
		data:dict = dict()
		try:
			sqlConnection = SqlClient.SqlConnection(self.ConnectionString)
			sqlConnection.Open()
			sqlCommand = SqlClient.SqlCommand(query, sqlConnection)
			sqlCommand.CommandType = CommandType.Text
			sqlCommand.CommandTimeout = 0
			sqlDataReader = sqlCommand.ExecuteReader()
			isFirstRow:bool = True
			while (sqlDataReader.Read()):
				if (isFirstRow):
					for fieldIndex in range(sqlDataReader.FieldCount):
						data.update({ sqlDataReader.GetName(fieldIndex): list() })
					isFirstRow = False
				for fieldIndex in range(sqlDataReader.FieldCount):
					data[sqlDataReader.GetName(fieldIndex)].append(sqlDataReader.GetValue(fieldIndex))
			returnValue = pandas.DataFrame(data)
		except Exception as e:
			exception = e
		finally:
			if sqlCommand:
				sqlCommand.Dispose()
			if sqlConnection:
				sqlConnection.Close()
				sqlConnection.Dispose()
		if exception is not None:
			raise exception
		return returnValue

	def TruncateTable(self, schema:str, table:str):
		exception = None
		sqlConnection = None
		sqlCommand = None
		try:
			sqlConnection = SqlClient.SqlConnection(self.ConnectionString)
			sqlConnection.Open()
			sqlCommand = SqlClient.SqlCommand(f"TRUNCATE TABLE [{schema}].[{table}]", sqlConnection)
			sqlCommand.CommandType = CommandType.Text
			sqlCommand.CommandTimeout = 0
			sqlCommand.ExecuteNonQuery()
		except Exception as e:
			exception = e
		finally:
			if sqlCommand:
				sqlCommand.Dispose()
			if sqlConnection:
				sqlConnection.Close()
				sqlConnection.Dispose()
		if exception is not None:
			raise exception

	def GetRowCount(self, schema:str, table:str) -> int:
		returnValue:int = None
		exception = None
		sqlConnection = None
		sqlCommand = None
		try:
			sqlConnection = SqlClient.SqlConnection(self.ConnectionString)
			sqlConnection.Open()
			sqlCommand = SqlClient.SqlCommand(f"SELECT COUNT(*) AS [Count] FROM [{schema}].[{table}]", sqlConnection)
			sqlCommand.CommandType = CommandType.Text
			sqlCommand.CommandTimeout = 0
			result = sqlCommand.ExecuteScalar()
			if (isinstance(result, int)):
				returnValue = int(result)
		except Exception as e:
			exception = e
		finally:
			if sqlCommand:
				sqlCommand.Dispose()
			if sqlConnection:
				sqlConnection.Close()
				sqlConnection.Dispose()
		if exception is not None:
			raise exception
		if returnValue is not None:
			return returnValue

class SQLServerBulkData(IBulkData):
	ConnectionString:str = None

	def __init__(self, connectionString:str):
		self.ConnectionString = connectionString

	def GetDataTable(self, schema:str, tableOrView:str, options:dict = None) -> DataTable:
		returnValue = DataTable()
		exception = None
		sqlConnection = None
		sqlCommand = None
		commandText:str = f"SELECT * FROM [{schema}].[{tableOrView}]"
		try:
			sqlConnection = SqlClient.SqlConnection(self.ConnectionString)
			sqlConnection.Open()
			sqlCommand = SqlClient.SqlCommand(commandText, sqlConnection)
			sqlCommand.CommandType = CommandType.Text
			sqlCommand.CommandTimeout = 0
			returnValue.Load(sqlCommand.ExecuteReader())
		except Exception as e:
			exception = e
		finally:
			if sqlCommand:
				sqlCommand.Dispose()
			if sqlConnection:
				sqlConnection.Close()
				sqlConnection.Dispose()
		if returnValue is not None:
			return returnValue
		elif exception is not None:
			raise exception

	def WriteDataTable(self, schema:str, table:str, dataTable:DataTable, options:dict = { "BatchSize": 1000 }):
		exception = None
		sqlConnection = None
		sqlBulkCopy = None
		try:
			batchSize:int = 1000
			if ("BatchSize" in options):
				batchSize = int(options["BatchSize"])
			sqlConnection = SqlClient.SqlConnection(self.ConnectionString)
			sqlConnection.Open()
			sqlBulkCopy = SqlClient.SqlBulkCopy(sqlConnection)
			sqlBulkCopy.BulkCopyTimeout = 0
			sqlBulkCopy.EnableStreaming = True
			sqlBulkCopy.BatchSize = batchSize
			sqlBulkCopy.DestinationTableName = f"[{schema}].[{table}]"
			for column in dataTable.Columns:
				sqlBulkCopy.ColumnMappings.Add(SqlClient.SqlBulkCopyColumnMapping(column.ColumnName, f"[{column.ColumnName}]"))
			sqlBulkCopy.WriteToServer(dataTable)
		except Exception as e:
			exception = e
		finally:
			if sqlBulkCopy:
				sqlBulkCopy.Close()
			if sqlConnection:
				sqlConnection.Close()
				sqlConnection.Dispose()
		if exception is not None:
			raise exception

	def GetDataFrame(self, schema:str = None, tableOrView:str = None, options:dict = None) -> pandas.DataFrame:
		returnValue = pandas.DataFrame()
		exception = None
		sqlConnection = None
		sqlCommand = None
		commandText:str = f"SELECT * FROM [{schema}].[{tableOrView}]"
		data:dict = dict()
		try:
			sqlConnection = SqlClient.SqlConnection(self.ConnectionString)
			sqlConnection.Open()
			sqlCommand = SqlClient.SqlCommand(commandText, sqlConnection)
			sqlCommand.CommandType = CommandType.Text
			sqlCommand.CommandTimeout = 0
			sqlDataReader = sqlCommand.ExecuteReader()
			isFirstRow:bool = True
			while (sqlDataReader.Read()):
				if (isFirstRow):
					for fieldIndex in range(sqlDataReader.FieldCount):
						data.update({ sqlDataReader.GetName(fieldIndex): list() })
					isFirstRow = False
				for fieldIndex in range(sqlDataReader.FieldCount):
					data[sqlDataReader.GetName(fieldIndex)].append(sqlDataReader.GetValue(fieldIndex))
			returnValue = pandas.DataFrame(data)
		except Exception as e:
			exception = e
		finally:
			if sqlCommand:
				sqlCommand.Dispose()
			if sqlConnection:
				sqlConnection.Close()
				sqlConnection.Dispose()
		if returnValue is not None:
			return returnValue
		elif exception is not None:
			raise exception

	def WriteDataFrame(self, schema:str, table:str, dataFrame:pandas.DataFrame, options:dict = None):
		raise NotImplementedError()


class SQLServerClearData(IClearData):
	ConnectionString:str = None

	def __init__(self, connectionString:str):
		self.ConnectionString = connectionString
		self.StoredProcedures = SQLServerStoredProcedures(connectionString)
		self.Queries = SQLServerQueries(connectionString)
		self.BulkData = SQLServerBulkData(connectionString)

	@staticmethod
	def ParseParameters(parameters):
		for parameter in parameters:
			print(f"{ParameterDirection.Input} [{parameter.SqlDbType}] -- Name: {parameter.Name} Direction: {parameter.Direction} Value: {parameter.Value}")

	def __GetTestConnectionQuery__(self) -> str:
		return """
			SET NOCOUNT ON
			DECLARE @TimeZone VARCHAR(50)
			EXEC [master].[dbo].[xp_regread]
				N'HKEY_LOCAL_MACHINE','SYSTEM\\CurrentControlSet\\Control\\TimeZoneInformation',
				N'TimeZoneKeyName',
				@TimeZone OUTPUT
			SET NOCOUNT OFF
			SELECT
				CONVERT
				(
					[nvarchar](MAX),
					(
						SELECT
							JSON_QUERY((
								SELECT
									HOST_NAME() AS [HostName],
									APP_NAME() AS [ApplicationName]
									FOR JSON PATH, WITHOUT_ARRAY_WRAPPER, INCLUDE_NULL_VALUES
							)) AS [Client],
							JSON_QUERY((
								SELECT
									@@VERSION AS [Version],
									SERVERPROPERTY(N'ProductVersion') AS [ProductVersion],
									SERVERPROPERTY(N'ServerName') AS [Name],
									SERVERPROPERTY(N'InstanceName') AS [InstanceName],
									SUSER_SNAME() AS [LoginName],
									SERVERPROPERTY(N'Collation') AS [Collation],
									SERVERPROPERTY(N'Edition') AS [Edition],
									SERVERPROPERTY(N'InstanceDefaultBackupPath') AS [InstanceDefaultBackupPath],
									SERVERPROPERTY(N'InstanceDefaultDataPath') AS [InstanceDefaultDataPath],
									SERVERPROPERTY(N'InstanceDefaultLogPath') AS [InstanceDefaultLogPath],
									CASE TRY_CAST(SERVERPROPERTY(N'FilestreamConfiguredLevel') AS [int])
										WHEN 0 THEN N'Disabled'
										WHEN 1 THEN N'Enabled for Transact-SQL access'
										WHEN 2 THEN N'Enabled for Transact-SQL and local Win32 streaming access'
										WHEN 3 THEN N'Enabled for Transact-SQL and both local and remote Win32 streaming access'
									END AS [FilestreamConfiguredLevel],
									CASE TRY_CAST(SERVERPROPERTY(N'FilestreamEffectiveLevel') AS [int])
										WHEN 0 THEN N'Disabled'
										WHEN 1 THEN N'Enabled for Transact-SQL access'
										WHEN 2 THEN N'Enabled for Transact-SQL and local Win32 streaming access'
										WHEN 3 THEN N'Enabled for Transact-SQL and both local and remote Win32 streaming access'
									END AS [FilestreamEffectiveLevel],
									SERVERPROPERTY(N'FilestreamShareName') AS [FilestreamShareName],
									DEFAULT_DOMAIN() AS [DefaultDomain],
									@TimeZone AS [ServerTimeZone]	
									FOR JSON PATH, WITHOUT_ARRAY_WRAPPER, INCLUDE_NULL_VALUES
							)) AS [Server],
							JSON_QUERY((
								SELECT
									DB_NAME() AS [Name],
									USER_NAME() AS [UserName],
									DATABASEPROPERTYEX(DB_NAME(), N'Collation') AS [Collation],
									DATABASEPROPERTYEX(DB_NAME(), N'Edition') AS [Edition],
									DATABASEPROPERTYEX(DB_NAME(), N'Recovery') AS [Recovery],
									DATABASEPROPERTYEX(DB_NAME(), N'Status') AS [Status],
									DATABASEPROPERTYEX(DB_NAME(), N'UserAccess') AS [UserAccess],
									DATABASEPROPERTYEX(DB_NAME(), N'Version') AS [Version]
									FOR JSON PATH, WITHOUT_ARRAY_WRAPPER, INCLUDE_NULL_VALUES
							)) AS [Databse]
							FOR JSON PATH, WITHOUT_ARRAY_WRAPPER, INCLUDE_NULL_VALUES
					),
					0
				) AS [JSON]
		"""

	def TestConnection(self) -> dict:
		returnValue:dict = None
		commandText:str = self.__GetTestConnectionQuery__()
		exception = None
		sqlConnection = None
		sqlCommand = None
		try:
			sqlConnection = SqlClient.SqlConnection(self.ConnectionString)
			sqlConnection.Open()
			sqlCommand = SqlClient.SqlCommand(commandText, sqlConnection)
			sqlCommand.CommandType = CommandType.Text
			sqlCommand.CommandTimeout = 0
			result = sqlCommand.ExecuteScalar()
			if (result):
				returnValue = json.loads(result)
		except Exception as e:
			exception = e
		finally:
			if sqlCommand:
				sqlCommand.Dispose()
			if sqlConnection:
				sqlConnection.Close()
				sqlConnection.Dispose()
		if exception is not None:
			raise exception
		return returnValue

__all__ = ["SQLServerParameter", "SQLServerStoredProcedures", "SQLServerQueries", "SQLServerBulkData", "SQLServerClearData"]