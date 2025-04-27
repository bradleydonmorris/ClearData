[String] $VEnvPythonPath = [IO.Path]::Combine([IO.Path]::GetDirectoryName($PSCommandPath), ".venv", "Scripts/python.exe")
& $VEnvPythonPath -m build 
& $VEnvPythonPath -m twine upload --skip-existing --repository pypi dist/*
