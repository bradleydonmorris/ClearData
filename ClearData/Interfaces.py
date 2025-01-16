import pandas
import platform
import pythonnet
if (platform.system() == "Linux"):
	pythonnet.load("mono")
if (platform.system() == "Windows"):
	pythonnet.load("netfx")
import clr
clr.AddReference("System.Data")
from System.Data import DataTable

class IStoredProcedures:
	"""
	Interface class for all classes that wish to represent themselves as IStoredProcedure compliant classes.
	"""

	def Execute(self, schema:str, name:str, parameters:list[any] = None) -> None:
		"""
		Execute
		Executes a procedure

		Parameters
		----------
		schema : str
			Name of the schema in which the procedure lives.
		
		name : str
			Name of the procedure.

		paramters : list
			Collection of parameters to pass to the stored procedure.
		"""
		raise NotImplementedError()

	def ExecuteWithScalar(self, schema:str, name:str, parameters:list[any] = None) -> any:
		"""
		ExecuteWithScalar
		Executes a procedure and returns the value of first field in the first row.

		Parameters
		----------
		schema : str
			Name of the schema in which the procedure lives.
		
		name : str
			Name of the procedure.

		paramters : list
			Collection of parameters to pass to the stored procedure.
		"""
		raise NotImplementedError()

	def ExecuteJSON(self,
			schema:str,
			name:str,
			inputValue:str = None
		) -> str:
		"""
		ExecuteJSON
		Executes a JSON styled procedure which expects an input parameter named Input and an output parameter named Output.

		Parameters
		----------
		schema : str
			Name of the schema in which the procedure lives.
		
		name : str
			Name of the procedure.

		inputParamterName : str
			The name of the input paramter for the procedure.

		inputValue : str
			The json string to pass as input.

		outputParamterName : str
			The name of the output paramter for the procedure.
		"""
		raise NotImplementedError()

	def ExecuteJSONInputOnly(self,
				 schema:str,
				 name:str,
				 inputValue:str = None,
		) -> None:
		"""
		ExecuteJSON
		Executes a JSON styled procedure which expects only an input parameter named Input and will have no output.

		Parameters
		----------
		schema : str
			Name of the schema in which the procedure lives.
		
		name : str
			Name of the procedure.

		inputValue : str
			The json string to pass as input.
		"""
		raise NotImplementedError()

	def ExecuteJSONOutputOnly(self,
				 schema:str,
				 name:str
		) -> str:
		"""
		ExecuteJSON
		Executes a JSON styled procedure which expects only an output parameter named Output and will not have any inputs.

		Parameters
		----------
		schema : str
			Name of the schema in which the procedure lives.
		
		name : str
			Name of the procedure.
		"""
		raise NotImplementedError()

	def ExecuteDataTable(self, schema:str, name:str, parameters:list[any] = None) -> DataTable:
		"""
		ExecuteDataTable
		Executes a procedure and returns the results as a System.Data.DataTable.

		Parameters
		----------
		schema : str
			Name of the schema in which the procedure lives.
		
		name : str
			Name of the procedure.

		paramters : list
			Collection of parameters to pass to the stored procedure.
		"""
		raise NotImplementedError()

	def ExecuteDataFrame(self, schema:str, name:str, parameters:list[any] = None) -> pandas.DataFrame:
		"""
		ExecuteDataFrame
		Executes a procedure and returns the results as a pandas.DataFrame.

		Parameters
		----------
		schema : str
			Name of the schema in which the procedure lives.
		
		name : str
			Name of the procedure.

		paramters : list
			Collection of parameters to pass to the stored procedure.
		"""
		raise NotImplementedError()

class IQueries:
	"""
	Interface class for all classes that wish to represent themselves as IQueries compliant classes.
	"""

	def Execute(self, query:str, parameters = None):
		"""
		Execute
		Executes a query

		Parameters
		----------
		query : str
			The query to execute.

		paramters : list
			Collection of parameters to pass to the stored procedure.
		"""
		raise NotImplementedError()

	def ExecuteWithScalar(self, query:str, parameters = None):
		"""
		ExecuteWithScalar
		Executes a query and returns the value of first field in the first row.

		Parameters
		----------
		query : str
			The query to execute.

		paramters : list
			Collection of parameters to pass to the stored procedure.
		"""
		raise NotImplementedError()

	def TruncateTable(self, schema:str, name:str):
		"""
		TruncateTable
		Truncates a table.

		Parameters
		----------
		schema : str
			Name of the schema in which the procedure lives.
		
		name : str
			Name of the procedure.
		"""
		raise NotImplementedError()

	def GetRowCount(self, schema:str, name:str) -> int:
		"""
		GetRowCount
		Returnes the row count of a table.

		Parameters
		----------
		schema : str
			Name of the schema in which the procedure lives.
		
		name : str
			Name of the table or view.
		"""
		raise NotImplementedError()

class IBulkData:
	def GetDataTable(self, schema:str = None, tableOrView:str = None, query:str = None, options:dict = None) -> DataTable:
		"""
		GetDataTable
		Returns a System.Data.DataTable from either a Schema and Table or from a query.

		Parameters
		----------
		schema : str
			Name of the schema in which the table or view lives.
		
		tableOrView : str
			Name of the table or view.

		query : str
			The query to execute.

		config : dict
			A dictionary of database system specific options.
		"""
		raise NotImplementedError()

	def WriteDataTable(self, schema:str, table:str, dataTable:DataTable, options:dict = None):
		"""
		WriteDataTable
		Writes the contents of a System.Data.DataTable to a table.

		Parameters
		----------
		schema : str
			Name of the schema in which the table or view lives.
		
		table : str
			Name of the table.

		dataTable : DataTable
			The System.Data.DataTable to write.

		config : dict
			A dictionary of database system specific options.
		"""
		raise NotImplementedError()

class IClearData:
	"""
	Interface class for all classes that wish to represent themselves as ICleanData compliant classes.
	"""

	def TestConnection(self) -> dict:
		"""
		TestConnection
		Makes a connection and executes a query that will return some data to prove the connection was successful.
		"""
		raise NotImplementedError()

__all__ = ["IStoredProcedures", "IQueries", "IBulkData", "IClearData"]