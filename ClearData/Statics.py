import pandas
import datetime
import platform
import pythonnet
if (platform.system() == "Linux"):
	pythonnet.load("mono")
if (platform.system() == "Windows"):
	pythonnet.load("netfx")
import clr
clr.AddReference("System")
import System
from System.Data import DataTable
from System.Data import DataRow
from System.Data import DataColumn

class Statics:
	@staticmethod
	def GetCLRDataType(obj):
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
	def ConvertToCLRDataType(obj, exceptedType:str = None):
		returnValue = None
		if (obj is None):
			returnValue = System.DBNull.Value
		else:
			if (exceptedType is None):
				exceptedType = Statics.GetCLRDataType(obj)
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

	@staticmethod
	def DataFrameToDataTable(dataFrame:pandas.DataFrame) -> DataTable:
		returnValue = DataTable()
		for index, row in dataFrame.iterrows():
			if (index == 0):
				for column in dataFrame.columns:
					dataColumn:DataColumn = DataColumn(column)
					dataColumn.DataType = System.Type.GetType(Statics.GetCLRDataType(row[column]))
					dataColumn.AllowDBNull  = True
					returnValue.Columns.Add(dataColumn)
			returnValue.Rows.Add(row.values.tolist())
		return returnValue

	@staticmethod
	def DataTableToDataFrame(dataTable:DataTable) -> pandas.DataFrame:
		returnValue:pandas.DataFrame = pandas.DataFrame()
		data:dict = dict()
		isFirstRow:bool = True
		for row in dataTable.Rows:
			if (isFirstRow):
				for column in dataTable.Columns:
					data.update({ column.ColumnName: list() })
				isFirstRow = False
			for index, column in enumerate(dataTable.Columns):
				data[column.ColumnName].append(row[index])
		returnValue = pandas.DataFrame(data)
		return returnValue

__all__ = ["Statics"]
