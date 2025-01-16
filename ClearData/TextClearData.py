from .Interfaces import *
from enum import Enum
from pathlib import Path
import platform
import stat
import datetime
import pandas
import clr
import csv
clr.AddReference('System.Data')
import System
from System.Data import DataTable
from System.Data import DataRow
from System.Data import DataColumn


class TextFileFormat(Enum):
	CommaWithHeader = 1
	CommaWithoutHeader = 2
	TabWithHeader = 3
	TabWithoutHeader = 4
	PipeWithHeader = 5
	PipeWithoutHeader = 6
	Prettify = 7

class TextQueries(IQueries):
	FilePath:Path = None
	Format:TextFileFormat = TextFileFormat.CommaWithHeader

	def __init__(self, filePath:Path, format:TextFileFormat = TextFileFormat.CommaWithHeader):
		self.FilePath = filePath
		self.Format = format

	def Execute(self, query:str, parameters = None):
		raise NotImplementedError()

	def ExecuteWithScalar(self, query:str, parameters = None):
		raise NotImplementedError()

	def ExecuteDataTable(self, query:str, parameters = None):
		raise NotImplementedError()

	def ExecuteDataFrame(self, query:str, parameters = None):
		raise NotImplementedError()

	def TruncateTable(self):
		header:str = None
		contents:str = self.FilePath.read_text()
		match self.Format:
			case TextFileFormat.CommaWithHeader:
				header = contents.split("\n")[0]
			case TextFileFormat.CommaWithoutHeader:
				header = ""

			case TextFileFormat.TabWithHeader:
				header = contents.split("\n")[0]
			case TextFileFormat.TabWithoutHeader:
				header = ""

			case TextFileFormat.PipeWithHeader:
				header = contents.split("\n")[0]
			case TextFileFormat.PipeWithoutHeader:
				header = ""
		self.FilePath.write_text(header)

	def GetRowCount(self) -> int:
		returnValue:int = None
		if (self.Format != TextFileFormat.Prettify):
			contents:str = self.FilePath.read_text()
			returnValue = len(contents.split("\n"))
			returnValue -= 1
			match self.Format:
				case TextFileFormat.CommaWithHeader:
					returnValue -= 1
				case TextFileFormat.TabWithHeader:
					returnValue -= 1
				case TextFileFormat.PipeWithHeader:
					returnValue -= 1
		return returnValue

class TextBulkData:
	FilePath:Path = None
	Format:TextFileFormat = TextFileFormat.CommaWithHeader

	def __init__(self, filePath:Path, format:TextFileFormat = TextFileFormat.CommaWithHeader):
		self.FilePath = filePath
		self.Format = format

	def __GetDataTableDelimited__(self, rowDelimiter:str, fieldDelimiter:str, includeHeader:bool) -> DataTable:
		returnValue:DataTable = None
		with open(self.FilePath, newline='') as file:
			returnValue = DataTable()
			reader = csv.reader(file, delimiter=fieldDelimiter, lineterminator=rowDelimiter)
			rowNumber:int = 0
			for reader in reader:
				rowNumber += 1
				if (rowNumber == 1):
					columns:list = []
					if (includeHeader):
						for item in reader:
							columnName:str = item
							dupeIndex:int = 0
							while (columns.count(columnName) > 0):
								dupeIndex += 1
								columnName = f"{columnName}_{dupeIndex}"
							columns.append(columnName)
					else:
						columns = ["Field_{}".format(x, y) for (x, y) in enumerate(reader)]
					for columnName in columns:
						dataColumn:DataColumn = DataColumn(columnName)
						dataColumn.DataType = System.Type.GetType("System.String")
						dataColumn.AllowDBNull  = True
						returnValue.Columns.Add(dataColumn)
					if (not includeHeader):
						dataRow:DataRow = returnValue.NewRow()
						for index, item in enumerate(reader):
							dataRow[index] = item
						returnValue.Rows.Add(dataRow)
				else:
					dataRow:DataRow = returnValue.NewRow()
					for index, item in enumerate(reader):
						dataRow[index] = item
					returnValue.Rows.Add(dataRow)
		return returnValue

	def __WriteDataTableDelimited__(self, dataTable:DataTable, rowDelimiter:str, fieldDelimiter:str, includeHeader:bool):
		returnValue:str = None
		exception = None
		try:
			lines:list = []
			line:str = None
			line = ""
			if (includeHeader):
				for dataColumn in dataTable.Columns:
					line += f"{dataColumn.ColumnName}{fieldDelimiter}"
				if (line.endswith(fieldDelimiter)):
					line = line[:-len(fieldDelimiter)]
				lines.append(line)
				line = ""
			for dataRow in dataTable.Rows:
				line = ""
				for dataColumn in dataTable.Columns:
					if (str(dataColumn.DataType) in [
						"System.Byte",
						"System.Int16",
						"System.Int32",
						"System.Int64",
						"System.Decimal"]):
						line += f"{str(dataRow[dataColumn.ColumnName])}{fieldDelimiter}"
					else:
						line += f"{str(dataRow[dataColumn.ColumnName])}{fieldDelimiter}"
				if (line.endswith(fieldDelimiter)):
					line = line[:-len(fieldDelimiter)]
				lines.append(line)
			returnValue = ""
			for ln in lines:
				returnValue += f"{ln}{rowDelimiter}"
		except Exception as e:
			exception = e
		if exception is not None:
			raise exception
		elif returnValue is not None:
			return returnValue

	def __WriteDataTablePrettify__(self, dataTable:DataTable):
		returnValue:str = None
		exception = None
		try:
			totalColumnWidth:int = 0
			columnWidth:dict = {}
			lines:list = []
			line:str = None
			allLines:str = ""
			for dataRow in dataTable.Rows:
				for dataColumn in dataTable.Columns:
					if (dataColumn.ColumnName not in columnWidth):
						columnWidth[dataColumn.ColumnName] = len(dataColumn.ColumnName)
					if (columnWidth[dataColumn.ColumnName] < len(str(dataRow[dataColumn]))):
						columnWidth[dataColumn.ColumnName] = len(str(dataRow[dataColumn]))
			for columnName in columnWidth:
				totalColumnWidth += columnWidth[columnName]
			lines.append(" -" + ("-"*totalColumnWidth) + ("--"*len(columnWidth)) + "- ")
			line = ""
			for columnName in columnWidth:
				line += f" {columnName.ljust(columnWidth[columnName])} "
			lines.append(f"| {line} |")
			line = ""
			for columnName in columnWidth:
				headBreak:str = "-"*columnWidth[columnName]
				line += f" {headBreak} "
			lines.append(f"| {line} |")
			for dataRow in dataTable.Rows:
				line = ""
				for dataColumn in dataTable.Columns:
					if (str(dataColumn.DataType) in [
						"System.Byte",
						"System.Int16",
						"System.Int32",
						"System.Int64",
						"System.Decimal"]):
						line += f" {str(dataRow[dataColumn.ColumnName]).rjust(columnWidth[dataColumn.ColumnName])} "
					else:
						line += f" {str(dataRow[dataColumn.ColumnName]).ljust(columnWidth[dataColumn.ColumnName])} "
				lines.append(f"| {line} |")
			lines.append(" -" + ("-"*totalColumnWidth) + ("--"*len(columnWidth)) + "- ")
			returnValue = ""
			for ln in lines:
				returnValue += f"{ln}\n"
		except Exception as e:
			exception = e
		if exception is not None:
			raise exception
		elif returnValue is not None:
			return returnValue

	def GetDataTable(self) -> DataTable:
		returnValue:DataTable | None = None
		match self.Format:
			case TextFileFormat.CommaWithHeader:
				returnValue = self.__GetDataTableDelimited__("\n", ",", True)
			case TextFileFormat.CommaWithoutHeader:
				returnValue = self.__GetDataTableDelimited__("\n", ",", False)

			case TextFileFormat.TabWithHeader:
				returnValue = self.__GetDataTableDelimited__("\n", "\t", True)
			case TextFileFormat.TabWithoutHeader:
				returnValue = self.__GetDataTableDelimited__("\n", "\t", False)

			case TextFileFormat.PipeWithHeader:
				returnValue = self.__GetDataTableDelimited__("\n", "|", True)
			case TextFileFormat.PipeWithoutHeader:
				returnValue = self.__GetDataTableDelimited__("\n", "|", False)
		return returnValue

	def WriteDataTable(self, dataTable:DataTable):
		exception = None
		try:
			text:str = None
			match self.Format:
				case TextFileFormat.CommaWithHeader:
					text = self.__WriteDataTableDelimited__(dataTable, "\n", ",", True)
				case TextFileFormat.CommaWithoutHeader:
					text = self.__WriteDataTableDelimited__(dataTable, "\n", ",", False)
				case TextFileFormat.TabWithHeader:
					text = self.__WriteDataTableDelimited__(dataTable, "\n", "\t", True)
				case TextFileFormat.TabWithoutHeader:
					text = self.__WriteDataTableDelimited__(dataTable, "\n", "\t", False)
				case TextFileFormat.PipeWithHeader:
					text = self.__WriteDataTableDelimited__(dataTable, "\n", "|", True)
				case TextFileFormat.PipeWithoutHeader:
					text = self.__WriteDataTableDelimited__(dataTable, "\n", "|", False)
				case TextFileFormat.Prettify:
					text = self.__WriteDataTablePrettify__(dataTable)

			if (text is not None):
				with open(self.FilePath, "w") as file:
					file.write(text)
		except Exception as e:
			exception = e
		if exception is not None:
			raise exception

	def GetDataFrame(self, schema:str = None, tableOrView:str = None, options:dict = None) -> pandas.DataFrame:
		raise NotImplementedError()

	def WriteDataFrame(self, schema:str, table:str, dataFrame:pandas.DataFrame, options:dict = None):
		raise NotImplementedError()

class TextClearData:
	FilePath:Path = None
	Format:TextFileFormat = TextFileFormat.CommaWithHeader

	@property
	def StoredProcedures(self):
		raise NotImplementedError("TextClearData does not implement IStoredProcedures")

	def __init__(self, filePath:Path, format:TextFileFormat = TextFileFormat.CommaWithHeader):
		self.FilePath = filePath
		self.Format = format
		self.Queries = TextQueries(filePath, format)
		self.BulkData = TextBulkData(filePath, format)

	def TestConnection(self) -> dict:
		returnValue:dict = dict()
		if (self.FilePath.exists()):
			filestat = self.FilePath.stat()
			returnValue.update({
					"Size": filestat.st_size,
					"CreateTime": datetime.datetime.fromtimestamp(filestat.st_birthtime),
					"LastAccessTime": datetime.datetime.fromtimestamp(filestat.st_atime),
					"LastModifiedTime": datetime.datetime.fromtimestamp(filestat.st_mtime),
					"CreateTime": datetime.datetime.fromtimestamp(filestat.st_birthtime)
				})
			if (platform.system() == "Linux"):
				returnValue.update({
					"FileSystem": filestat.st_fstype,
					"Type": filestat.st_type,
					"Creator": filestat.st_creator
				})
			if (platform.system() == "Windows"):
				returnValue.update({ "FileAttributes": filestat.st_file_attributes })
				returnValue.update({ "IsArchive": ((filestat.st_file_attributes & stat.FILE_ATTRIBUTE_ARCHIVE) == stat.FILE_ATTRIBUTE_ARCHIVE) })
				returnValue.update({ "IsCompressed": ((filestat.st_file_attributes & stat.FILE_ATTRIBUTE_COMPRESSED) == stat.FILE_ATTRIBUTE_COMPRESSED) })
				returnValue.update({ "IsDecive": ((filestat.st_file_attributes & stat.FILE_ATTRIBUTE_DEVICE) == stat.FILE_ATTRIBUTE_DEVICE) })
				returnValue.update({ "IsDirectory": ((filestat.st_file_attributes & stat.FILE_ATTRIBUTE_DIRECTORY) == stat.FILE_ATTRIBUTE_DIRECTORY) })
				returnValue.update({ "IsEncrypted": ((filestat.st_file_attributes & stat.FILE_ATTRIBUTE_ENCRYPTED) == stat.FILE_ATTRIBUTE_ENCRYPTED) })
				returnValue.update({ "IsHidden": ((filestat.st_file_attributes & stat.FILE_ATTRIBUTE_HIDDEN) == stat.FILE_ATTRIBUTE_HIDDEN) })
				returnValue.update({ "IsIntegrityStream": ((filestat.st_file_attributes & stat.FILE_ATTRIBUTE_INTEGRITY_STREAM) == stat.FILE_ATTRIBUTE_INTEGRITY_STREAM) })
				returnValue.update({ "IsNormal": ((filestat.st_file_attributes & stat.FILE_ATTRIBUTE_NORMAL) == stat.FILE_ATTRIBUTE_NORMAL) })
				returnValue.update({ "IsContentNotIndexed": ((filestat.st_file_attributes & stat.FILE_ATTRIBUTE_NOT_CONTENT_INDEXED) == stat.FILE_ATTRIBUTE_NOT_CONTENT_INDEXED) })
				returnValue.update({ "IsNoScrubData": ((filestat.st_file_attributes & stat.FILE_ATTRIBUTE_NO_SCRUB_DATA) == stat.FILE_ATTRIBUTE_NO_SCRUB_DATA) })
				returnValue.update({ "IsOffline": ((filestat.st_file_attributes & stat.FILE_ATTRIBUTE_OFFLINE) == stat.FILE_ATTRIBUTE_OFFLINE) })
				returnValue.update({ "IsReadOnly": ((filestat.st_file_attributes & stat.FILE_ATTRIBUTE_READONLY) == stat.FILE_ATTRIBUTE_READONLY) })
				returnValue.update({ "IsReparsePoint": ((filestat.st_file_attributes & stat.FILE_ATTRIBUTE_REPARSE_POINT) == stat.FILE_ATTRIBUTE_REPARSE_POINT) })
				returnValue.update({ "IsSpareFile": ((filestat.st_file_attributes & stat.FILE_ATTRIBUTE_SPARSE_FILE) == stat.FILE_ATTRIBUTE_SPARSE_FILE) })
				returnValue.update({ "IsSystem": ((filestat.st_file_attributes & stat.FILE_ATTRIBUTE_SYSTEM) == stat.FILE_ATTRIBUTE_SYSTEM) })
				returnValue.update({ "IsTemporary": ((filestat.st_file_attributes & stat.FILE_ATTRIBUTE_TEMPORARY) == stat.FILE_ATTRIBUTE_TEMPORARY) })
				returnValue.update({ "IsVirtual": ((filestat.st_file_attributes & stat.FILE_ATTRIBUTE_VIRTUAL) == stat.FILE_ATTRIBUTE_VIRTUAL) })

__all__ = ["TextFileFormat", "TextBulkData", "TextQueries", "TextClearData"]
