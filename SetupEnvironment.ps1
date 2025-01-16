[String] $VEnvPath = [IO.Path]::Combine([IO.Path]::GetDirectoryName($PSCommandPath), ".venv")
[String] $SourcePath =  [IO.Path]::GetDirectoryName($PSCommandPath)
$SourcePath += "\."

& "C:/Program Files/Python312/python.exe" -m venv $VEnvPath

[String] $VEnvPythonPath = [IO.Path]::Combine([IO.Path]::GetDirectoryName($PSCommandPath), ".venv", "Scripts/python.exe")
& $VEnvPythonPath -m pip install --upgrade pip
& $VEnvPythonPath -m pip install build
& $VEnvPythonPath -m pip install twine
& $VEnvPythonPath -m pip install pythonnet
& $VEnvPythonPath -m pip install pyodbc
& $VEnvPythonPath -m pip install pandas
& $VEnvPythonPath -m pip install "psycopg[binary,pool]"

& $VEnvPythonPath -m pip install $SourcePath
