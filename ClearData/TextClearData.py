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


class FixedWidthJustification(Enum):
	Left = 1
	Right = 2

class TextFileFormat(Enum):
	Delimited = 1
	Prettify = 2
	FixedWidth = 3

class TextFileQuoting(Enum):
	Minimal = 0
	All = 1
	NonNumberic = 2
	NoQuotes = 3
	Strings = 4
	NotNull = 5

	def ToCSVConstant(self) -> int|None:
		returnValue:int|None = None
		match (self):
			case TextFileQuoting.Minimal: returnValue = csv.QUOTE_MINIMAL
			case TextFileQuoting.All: returnValue = csv.QUOTE_ALL
			case TextFileQuoting.NonNumberic: returnValue = csv.QUOTE_NONNUMERIC
			case TextFileQuoting.NoQuotes: returnValue = csv.QUOTE_NONE
			case TextFileQuoting.Strings: returnValue = csv.QUOTE_STRINGS
			case TextFileQuoting.NotNull: returnValue = csv.QUOTE_NOTNULL
			case _: raise ValueError("Unknown")
		return returnValue

class DelimitedFileConfig:
	Quoting:TextFileQuoting|None = None
	FieldDelimiter:str = ","
	RowDelimiter:str = "\n"
	HasHeader:bool = False,
	QuoteCharacter:str = '"'

	def __init__(self, quoting:TextFileQuoting, fieldDelimiter:str=",", rowDelimiter:str="\n", hasHeader:bool=False):
		self.Quoting = quoting
		self.FieldDelimiter = fieldDelimiter
		self.RowDelimiter = rowDelimiter
		self.HasHeader = hasHeader

class FixedWidthFileColumn:
	Ordinal:int|None = None
	Name:str|None=None
	BeginPosition:int|None = None
	EndPosition:int|None = None
	Justification:FixedWidthJustification = FixedWidthJustification.Left
	JustificationCharacter:str|None = " "
	Length:int = 0
	TrimJustificationCharacters:bool = False

	def __init__(self,
					ordinal:int|None=None, name:int|None=None,
					beginPosition:int|None=None, endPosition:int|None=None,
					justification:FixedWidthJustification = FixedWidthJustification.Left,
					justificationCharacter:str|None=" ",
					trimJustificationCharacters:bool=False):
		self.Ordinal = ordinal
		self.Name = name
		self.BeginPosition = beginPosition
		self.EndPosition = endPosition
		self.Justification = justification
		self.JustificationCharacter = justificationCharacter
		self.TrimJustificationCharacters = trimJustificationCharacters
		self.Length = (self.EndPosition - self.BeginPosition) + 1

class FixedWidthFileColumns(list):
	LastUsedOrdinal:int = 0

	def __len__(self):
		return super().__len__()

	def __iter__(self):
		return super().__iter__()

	def __getitem__(self, item):
		return super()[item]

	def __init__(self) -> None:
		super().__init__()

	def append(self, object:FixedWidthFileColumn):
		if (object.Ordinal is None):
			self.LastUsedOrdinal += 1
			object.Ordinal = self.LastUsedOrdinal
		elif (object.Ordinal == 0):
			self.LastUsedOrdinal += 1
			object.Ordinal = self.LastUsedOrdinal
		super().append(object)

	def append(self, object:FixedWidthFileColumn):
		if (object.Ordinal is None):
			self.LastUsedOrdinal += 1
			object.Ordinal = self.LastUsedOrdinal
		elif (object.Ordinal == 0):
			self.LastUsedOrdinal += 1
			object.Ordinal = self.LastUsedOrdinal
		super().append(object)

	def GetColumn(name:str) -> FixedWidthFileColumn|None:
		return next(filter(lambda x: x.Name == name,  super()), None)

class TextQueries(IQueries):
	FilePath:Path = None
	Format:TextFileFormat = TextFileFormat.Delimited
	Columns:FixedWidthFileColumns|None = None
	DelimitedConfig:DelimitedFileConfig|None = None

	def __init__(self, filePath:Path, format:TextFileFormat = TextFileFormat.Delimited, delimitedConfig:DelimitedFileConfig|None = None, fixedWidthColumns:FixedWidthFileColumns|None = None):
		self.FilePath = filePath
		self.Format = format
		self.Columns = fixedWidthColumns
		self.DelimitedConfig = delimitedConfig

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
		if (self.Format == TextFileFormat.Delimited
			and self.DelimitedConfig.HasHeader):
			header = self.FilePath.read_text().split(self.DelimitedConfig.RowDelimiter)[0]
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
	Format:TextFileFormat = TextFileFormat.Delimited
	FixedWidthColumns:FixedWidthFileColumns|None = None
	DelimitedConfig:DelimitedFileConfig|None = None

	def __init__(self, filePath:Path, format:TextFileFormat = TextFileFormat.Delimited, delimitedConfig:DelimitedFileConfig|None = None, fixedWidthColumns:FixedWidthFileColumns|None = None):
		self.FilePath = filePath
		self.Format = format
		self.FixedWidthColumns = fixedWidthColumns
		self.DelimitedConfig = delimitedConfig

	def __GetDataTableDelimited__(self) -> DataTable:
		returnValue:DataTable = None
		with open(self.FilePath, newline='', encoding="UTF-8") as file:
			returnValue = DataTable()
			reader = csv.reader(file,
						delimiter=self.DelimitedConfig.FieldDelimiter,
						lineterminator=self.DelimitedConfig.RowDelimiter,
						quotechar=self.DelimitedConfig.QuoteCharacter,
						quoting=self.DelimitedConfig.Quoting.ToCSVConstant())
			rowNumber:int = 0
			for row in reader:
				rowNumber += 1
				if (rowNumber == 1):
					columns:list = []
					if (self.DelimitedConfig.HasHeader):
						for item in row:
							columnName:str = item
							dupeIndex:int = 0
							while (columns.count(columnName) > 0):
								dupeIndex += 1
								columnName = f"{columnName}_{dupeIndex}"
							columns.append(columnName)
					else:
						columns = ["Field_{}".format(x, y) for (x, y) in enumerate(row)]
					for columnName in columns:
						dataColumn:DataColumn = DataColumn(columnName)
						dataColumn.DataType = System.Type.GetType("System.String")
						dataColumn.AllowDBNull  = True
						returnValue.Columns.Add(dataColumn)
					if (not self.DelimitedConfig.HasHeader):
						dataRow:DataRow = returnValue.NewRow()
						for index, item in enumerate(row):
							dataRow[index] = item
						returnValue.Rows.Add(dataRow)
				else:
					dataRow:DataRow = returnValue.NewRow()
					for index, item in enumerate(row):
						dataRow[index] = item
					returnValue.Rows.Add(dataRow)
		return returnValue

	def __GetDataTableFixedWidth__(self) -> DataTable:
		returnValue:DataTable = DataTable()
		for fixedWidthColumn in self.FixedWidthColumns:
			dataColumn:DataColumn = DataColumn(fixedWidthColumn.Name)
			dataColumn.DataType = System.Type.GetType("System.String")
			dataColumn.AllowDBNull  = True
			returnValue.Columns.Add(dataColumn)
		for lineIndex, lineText in enumerate(self.FilePath.read_text().splitlines()):
			values:list[str] = list[str]()
			for fixedWidthColumn in self.FixedWidthColumns:
				value:str = lineText[(fixedWidthColumn.BeginPosition-1):fixedWidthColumn.EndPosition]
				if (fixedWidthColumn.TrimJustificationCharacters):
					if (fixedWidthColumn.Justification == FixedWidthJustification.Right):
						value = value.rstrip(fixedWidthColumn.JustificationCharacter)
					elif (fixedWidthColumn.Justification == FixedWidthJustification.Left):
						value = value.lstrip(fixedWidthColumn.JustificationCharacter)
				values.append(value)
			returnValue.Rows.Add(values)
		return returnValue

	def __WriteDataTableDelimited__(self, dataTable:DataTable):
		with open(self.FilePath, newline='', encoding="UTF-8") as file:
			writer = csv.writer(file,
					delimiter=self.DelimitedConfig.FieldDelimiter,
					lineterminator=self.DelimitedConfig.RowDelimiter,
					quotechar=self.DelimitedConfig.QuoteCharacter,
					quoting=self.DelimitedConfig.Quoting.ToCSVConstant())
			if (self.DelimitedConfig.HasHeader):
				header:list[str] = list[str]()
				for dataColumn in dataTable.Columns:
					header.append(dataColumn.ColumnName)
				writer.writerow(header)
			for dataRow in dataTable.Rows:
				values:list[any] = list[any]()
				for dataColumn in dataTable.Columns:
					values.append(dataRow[dataColumn.ColumnName])
				writer.writerow(values)

	def __WriteDataTableFixedWidth__(self, dataTable:DataTable):
		rowDelimiter:str = "\n"
		lines:list = []
		line:str = None
		line = ""
		for dataRow in dataTable.Rows:
			line = ""
			for dataColumn in dataTable.Columns:
				fixedWidthFileColumn:FixedWidthFileColumn|None = self.FixedWidthColumns.GetColumn(dataColumn.ColumnName)
				if (fixedWidthFileColumn is not None):
					if (fixedWidthFileColumn.Justification == FixedWidthJustification.Left):
						line += str(dataRow[dataColumn.ColumnName]).ljust(fixedWidthFileColumn.Length, fixedWidthFileColumn.JustificationCharacter)
					if (fixedWidthFileColumn.Justification == FixedWidthJustification.Right):
						line += str(dataRow[dataColumn.ColumnName]).rjust(fixedWidthFileColumn.Length, fixedWidthFileColumn.JustificationCharacter)
			lines.append(line)
		self.FilePath.write_text(rowDelimiter.join(lines))

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

	def GetDataTable(self) -> DataTable|None:
		returnValue:DataTable|None = None
		match (self.Format):
			case TextFileFormat.Delimited:
				returnValue = self.__GetDataTableDelimited__()
			case TextFileFormat.FixedWidth:
				returnValue = self.__GetDataTableFixedWidth__()
			case _:
				returnValue = None
		return returnValue

	def WriteDataTable(self, dataTable:DataTable):
		match (self.Format):
			case TextFileFormat.Delimited:
				returnValue = self.__WriteDataTableDelimited__(dataTable)
			case TextFileFormat.FixedWidth:
				returnValue = self.__WriteDataTableFixedWidth__(dataTable)
			case _:
				returnValue = None

	def GetDataFrame(self, schema:str = None, tableOrView:str = None, options:dict = None) -> pandas.DataFrame:
		raise NotImplementedError()

	def WriteDataFrame(self, schema:str, table:str, dataFrame:pandas.DataFrame, options:dict = None):
		raise NotImplementedError()

class TextClearData(IClearData):
	FilePath:Path = None
	Format:TextFileFormat = TextFileFormat.Delimited

	@property
	def StoredProcedures(self):
		raise NotImplementedError("TextClearData does not implement IStoredProcedures")

	def __init__(self, filePath:Path, format:TextFileFormat = TextFileFormat.Delimited, delimitedConfig:DelimitedFileConfig|None = None, fixedWidthColumns:FixedWidthFileColumns|None = None):
		if (format == TextFileFormat.Delimited
			and delimitedConfig is None):
			raise ValueError("If format is delimited, then delimitedConfig must be provided")
		if (format == TextFileFormat.FixedWidth
			and fixedWidthColumns is None):
			raise ValueError("If format is fixed wicth, then fixedWidthColumns must be provided")
		self.FilePath = filePath
		self.Format = format
		self.Queries = TextQueries(filePath, format, delimitedConfig, fixedWidthColumns)
		self.BulkData = TextBulkData(filePath, format, delimitedConfig, fixedWidthColumns)

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

	@staticmethod
	def GetTextFileFormatConfig(format:TextFileFormat = TextFileFormat.Delimited):
		fieldDelimiter:str = ","
		rowDelimiter:str = "\n"
		includeHeader:bool = True
		match format:
			case TextFileFormat.CommaWithHeader:
				fieldDelimiter = ","
				rowDelimiter = "\n"
				includeHeader = True
			case TextFileFormat.CommaWithoutHeader:
				fieldDelimiter = ","
				rowDelimiter = "\n"
				includeHeader = False
			case TextFileFormat.TabWithHeader:
				fieldDelimiter = "\t"
				rowDelimiter = "\n"
				includeHeader = True
			case TextFileFormat.TabWithoutHeader:
				fieldDelimiter = "\t"
				rowDelimiter = "\n"
				includeHeader = False
			case TextFileFormat.PipeWithHeader:
				fieldDelimiter = "|"
				rowDelimiter = "\n"
				includeHeader = True
			case TextFileFormat.PipeWithoutHeader:
				fieldDelimiter = "|"
				rowDelimiter = "\n"
				includeHeader = False
			case TextFileFormat.FixedWidth:
				fieldDelimiter = None
				rowDelimiter = "\n"
				includeHeader = False
		return fieldDelimiter, rowDelimiter, includeHeader

	@staticmethod
	def ConvertFixedWidthToDelimited(
			fixedWidthFilePath:Path, fixedWidthColumns:FixedWidthFileColumns,
			delimitedFilePath:Path, delimitedConfig:DelimitedFileConfig):
		fixedBulkData = TextBulkData(filePath=fixedWidthFilePath, format=TextFileFormat.FixedWidth, delimitedConfig=None, fixedWidthColumns=fixedWidthColumns)
		delimitedBulkData = TextBulkData(filePath=delimitedFilePath, format=TextFileFormat.Delimited, delimitedConfig=delimitedConfig, fixedWidthColumns=None)
		delimitedBulkData.WriteDataTable(fixedBulkData.GetDataTable())

	@staticmethod
	def ConvertDelimitedToFixedWidth(
			fixedWidthFilePath:Path, fixedWidthColumns:FixedWidthFileColumns,
			delimitedFilePath:Path, delimitedConfig:DelimitedFileConfig):
		fixedBulkData = TextBulkData(filePath=fixedWidthFilePath, format=TextFileFormat.FixedWidth, delimitedConfig=None, fixedWidthColumns=fixedWidthColumns)
		delimitedBulkData = TextBulkData(filePath=delimitedFilePath, format=TextFileFormat.Delimited, delimitedConfig=delimitedConfig, fixedWidthColumns=None)
		fixedBulkData.WriteDataTable(delimitedBulkData.GetDataTable())

__all__ = ["FixedWidthJustification", "TextFileFormat", "TextFileQuoting",
			"DelimitedFileConfig", "FixedWidthFileColumn", "FixedWidthFileColumns",
			"TextQueries", "TextBulkData", "TextClearData"]
