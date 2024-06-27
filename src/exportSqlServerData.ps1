# Carregando a biblioteca SMO, antes disso precisa instalar a biblioteca usando o comando: Install-Module -Name SqlServer
Import-Module SqlServer

# Obtem o caminho do diretório onde o script está sendo executado
$scriptDirectory = $PSScriptRoot

$configFile = Join-Path -Path $scriptDirectory -ChildPath "config.cfg"

if (Test-Path $configFile) {
    $config = Get-Content $configFile | ForEach-Object {
        # Separar o nome da variável e o valor usando o sinal de igual (=)
        $name, $value = $_ -split '\s*=\s*', 2
        Set-Variable -Name $name -Value $value -Scope Global
    }
} else {
    Write-Host "Arquivo de configuração não encontrado: $configFile"
    exit
}

# Criar a string de conexão para o Azure SQL Database
$connectionString = "Server=tcp:$serverName,1433;Database=$databaseName;User ID=$username;Password=$password;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;"
Write-Host "String de conexÃ£o: $connectionString"

# Criar objeto de conexão SMO para o Azure SQL Database
$server = New-Object Microsoft.SqlServer.Management.Smo.Server($serverName)
$server.ConnectionContext.ConnectionString = $connectionString

# Selecionar o banco de dados
$database = $server.Databases[$databaseName]
if ($database -eq $null) {
    throw "NÃ£o foi possÃ­vel conectar ao banco de dados $databaseName"
}
else{
    Write-Host "Selecionou a DataBase: $database"
}

# Definir as opções de script
$options = New-Object Microsoft.SqlServer.Management.Smo.ScriptingOptions
$options.ScriptData = $true  # Incluir dados (INSERTs) no script
$options.ScriptSchema = $false  # Não incluir estrutura da tabela
$options.IncludeHeaders = $true  # Incluir cabeçãlhos (INSERT INTO)
Write-Host "Definiu as opções de exportação."

# Buscar as datas que precisam ser exportadas
$sqlQuerySelectDays = "
    select convert(date, CreationDate) as date, 
        datediff(day, CreationDate, getdate()) as lifetime
    from StoredEvent 
    where datediff(day, CreationDate, getdate()) > 90
    group by CONVERT(date, CreationDate), DATEDIFF(day, CreationDate, GETDATE())
    order by date desc
"

$resultSet = $database.ExecuteWithResults($sqlQuerySelectDays)   
$totalRecords = $resultSet.Tables[0].Rows.Count 
Write-Host "Existe(m) $totalRecords dia(s) para ser(em) exportado(s)"

$cont = 1

foreach ($row in $resultSet.Tables[0].Rows) {
    Write-Host "Exportando o registro: $cont de $totalRecords"

    $date = $row["date"].ToString("yyyy-MM-dd")
    $lifetime = $row["lifetime"]

    $tableNameSufix = $row["date"].ToString("ddMMyyyy")    
    $tableName = "${tableNamePrefix}${tableNameSufix}"

    Write-Host "Gerando a tabela temporaria: $tableName"
    $sqlQueryGenerateTempTable = "
        if object_id('$databaseName.dbo.$tableName', 'U') is not null
            begin
                drop table $tableName;
            end
        
        select * into $tableName
        from StoredEvent
        where convert(date, CreationDate) = '$date'
    "

    Invoke-SqlCmd -ServerInstance $serverName -Database $databaseName -Username $username -Password $password -Query $sqlQueryGenerateTempTable -Debug
    Write-Host "Tabela temporaria gerada: $tableName"

    Write-Host "Exportando dados da tabela: $tableName"
    $table = $database.Tables[$tableName]
    if ($table -eq $null) {
        throw "A tabela $tableName nÃ£o foi encontrada no banco de dados $databaseName"
    }

    $scripter = New-Object Microsoft.SqlServer.Management.Smo.Scripter($server)
    $scripter.Options = $options
    #$scripter.EnumScript($table) | Out-File "$filePath\dbo.$tableName.Table.sql"
    $scripterContents = $scripter.EnumScript($table)
    $replaceFrom = "INSERT [dbo].[$tableName]"
    $replaceTo = "INSERT [dbo].[$tableNamePrefix]"
    $scripterContents = $scripterContents.Replace($replaceFrom, $replaceTo)
    $scripterContents | Out-File "$filePath\dbo.$tableName.Table.sql"

    Write-Host "Tabela: $tableName exportada com sucesso"

    Write-Host "Excluindo a tabela: $tableName"
    $dropTableCommand = "DROP TABLE [dbo].[$tableName]"
    $server.ConnectionContext.ExecuteNonQuery($dropTableCommand)
    Write-Host "Tabela: $tableName excluída"

    $cont ++
}

Write-Host "Script de dados gerado com sucesso: $filePath\DataOnlyScript.sql"