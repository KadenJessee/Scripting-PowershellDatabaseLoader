#!/usr/bin/env pwsh
# Kaden Jessee
# Lab 8 - PowerShell Database Loader
# CS 3030 - Scripting Languages

#take in parameters
param (
    [string]$inputCSV,
    [string]$outputDB
)

#had to change from $args.Length -ne 2
if ([string]::IsNullOrEmpty($inputCSV) -or [string]::IsNullOrEmpty($outputDB)) {
    Write-Host "Usage: dbload.ps1 INPUTCSV OUTPUTDB"
    exit 1
}


try {
    #use for connection to the file path
    Add-Type -Path "dlls/System.Data.SQLite.dll"
    $con = New-Object -TypeName System.Data.SQLite.SQLiteConnection
    $con.ConnectionString = "Data Source=$outputDB"
    
    # Attempt to open the database connection
    $con.Open()
}
catch {
    Write-Host ("Error opening database file: $_")
    exit 1
}

try {
    #delimiter line mentioned from lab instructions
    $csv = Import-Csv -Path $inputCSV -Delimiter ","
}
catch {
    Write-Host ("Error opening CSV file: $_")
    # Close the database connection if it was opened successfully
    if ($con.State -eq [System.Data.ConnectionState]::Open) {
        $con.Close()
    }
    exit 1
}

$transaction = $con.BeginTransaction("create")

# Drop existing tables if they exist
$sql = $con.CreateCommand()
$sql.CommandText = 'DROP TABLE IF EXISTS people'
[void]$sql.ExecuteNonQuery()

$sql = $con.CreateCommand()
$sql.CommandText = 'DROP TABLE IF EXISTS courses'
[void]$sql.ExecuteNonQuery()

# Create tables
$sql.CommandText = 'CREATE TABLE courses (id text, subjcode text, coursenumber text, termcode text);'
[void]$sql.ExecuteNonQuery()

$sql = $con.CreateCommand()
$sql.CommandText = 'CREATE TABLE people (id text PRIMARY KEY UNIQUE, lastname text, firstname text, email text, major text, city text, state text, zip text);'
[void]$sql.ExecuteNonQuery()

[void]$transaction.Commit()

foreach ($row in $csv) {
    # Begin a transaction for adding a person
    $transaction = $con.BeginTransaction("addpersontransaction")
    
    $sql.CommandText = "INSERT OR REPLACE INTO people (id, firstname, lastname, email, major, city, state, zip)
        VALUES (@id, @firstname, @lastname, @email, @major, @city, @state, @zip);"

    # Bind parameters
    [void]$sql.Parameters.AddWithValue("@id", $row.wnumber)
    [void]$sql.Parameters.AddWithValue("@firstname", $row.firstname)
    [void]$sql.Parameters.AddWithValue("@lastname", $row.lastname)
    [void]$sql.Parameters.AddWithValue("@email", $row.email)
    [void]$sql.Parameters.AddWithValue("@major", $row.major)
    [void]$sql.Parameters.AddWithValue("@city", $row.city)
    [void]$sql.Parameters.AddWithValue("@state", $row.state)
    [void]$sql.Parameters.AddWithValue("@zip", $row.zip)

    # Execute the SQL command
    [void]$sql.ExecuteNonQuery()

    # Commit the transaction
    [void]$transaction.Commit()
    
    # Begin a transaction for adding a course
    $transaction = $con.BeginTransaction("addcoursetransaction")
    $coursedata = $row.course -split " "
    $subjcode = $coursedata[0]
    $coursenumber = $coursedata[1]

    $sql.CommandText = "INSERT INTO courses (id, subjcode, coursenumber, termcode)
        VALUES (@id, @subjcode, @coursenumber, @termcode);"

    # Bind parameters
    [void]$sql.Parameters.AddWithValue("@id", $row.wnumber)
    [void]$sql.Parameters.AddWithValue("@subjcode", $subjcode)
    [void]$sql.Parameters.AddWithValue("@coursenumber", $coursenumber)
    [void]$sql.Parameters.AddWithValue("@termcode", $row.termcode)

    # Execute the SQL command
    [void]$sql.ExecuteNonQuery()

    # Commit the transaction
    [void]$transaction.Commit()
}

# Close the database connection
$con.Close()
Write-Host "Database created successfully."
exit 0
