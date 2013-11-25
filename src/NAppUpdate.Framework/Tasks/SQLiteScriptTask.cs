using System;
using NAppUpdate.Framework.Tasks;
using NAppUpdate.Framework.Common;
using NAppUpdate.Framework.Sources;
using Mono.Data.Sqlite;
using System.IO;

namespace NAppUpdate.Framework
{
	[UpdateTaskAlias("sqliteScript")]
	public class SQLiteScriptTask : UpdateTaskBase
	{
		[NauField("scriptFile","Script filename",true)]
		public string ScriptFile { get; set;}
		[NauField("dbFile","SQLite databse filename",true)]
		public string DbFile { get; set;}
		[NauField("pwd","SQLite databse password. Need if database is encrypted", false)]
		public string Password { get; set;}

		[NauField("sha256-checksum", "SHA-256 checksum to validate the file after download (optional)", false)]
		public string Sha256Checksum { get; set; }

		private string _dbFile, _tempScript, _connectionString;

		public override void Prepare (IUpdateSource source)
		{
			if (string.IsNullOrEmpty(ScriptFile))
			{
				UpdateManager.Instance.Logger.Log(Logger.SeverityLevel.Warning, "SQLiteScriptTask: ScriptFile is empty, task is a noop");
				ExecutionStatus = TaskExecutionStatus.FailedToPrepare;
				return;
			}

			if (string.IsNullOrEmpty(DbFile))
			{
				UpdateManager.Instance.Logger.Log(Logger.SeverityLevel.Warning, "SQLiteScriptTask: DbFile is empty, task is a noop");
				ExecutionStatus = TaskExecutionStatus.FailedToPrepare;
				return;
			}

			_dbFile = Path.Combine(Path.GetDirectoryName(UpdateManager.Instance.ApplicationPath), DbFile);
			SqliteConnectionStringBuilder b = new SqliteConnectionStringBuilder ();

			_connectionString = String.Format ("Data Source={0};Version=3;{1}",_dbFile, string.IsNullOrEmpty(Password) ? string.Empty : String.Format("Password={0};",Password));

			if (!tryToOpenDatabase(_connectionString))
				throw new UpdateProcessFailedException(String.Format("SQLiteScriptTask: Failed to open sqlite database {0}", _dbFile));

			_tempScript = null;

			string baseUrl = UpdateManager.Instance.BaseUrl;
			string tempFileLocal = Path.Combine(UpdateManager.Instance.Config.TempFolder, Guid.NewGuid().ToString());

			UpdateManager.Instance.Logger.Log("SQLiteScriptTask: Downloading {0} with BaseUrl of {1} to {2}", fileName, baseUrl, tempFileLocal);

			if (!source.GetData (ScriptFile, baseUrl, OnProgress, ref tempFileLocal)) {
				ExecutionStatus = TaskExecutionStatus.FailedToPrepare;
				throw new UpdateProcessFailedException ("SQLiteScriptTask: Failed to get file from source");
			}

			_tempScript = tempFileLocal;
			if (_tempScript == null)
				throw new UpdateProcessFailedException("SQLiteScriptTask: Failed to get file from source");

			if (!string.IsNullOrEmpty(Sha256Checksum))
			{
				string checksum = Utils.FileChecksum.GetSHA256Checksum(_tempScript);
				if (!checksum.Equals(Sha256Checksum))
					throw new UpdateProcessFailedException(string.Format("SQLiteScriptTask: Checksums do not match; expected {0} but got {1}", Sha256Checksum, checksum));
			}
			UpdateManager.Instance.Logger.Log("SQLiteScriptTask: Prepared successfully; database to patch: {0}", _dbFile);
			ExecutionStatus = TaskExecutionStatus.Prepared;
		}

		public override TaskExecutionStatus Execute (bool coldRun)
		{
			if (string.IsNullOrEmpty(ScriptFile))
			{
				UpdateManager.Instance.Logger.Log(Logger.SeverityLevel.Warning, "SQLiteScriptTask: ScriptFile is empty, task is a noop");
				return;
			}

			if (string.IsNullOrEmpty(DbFile))
			{
				UpdateManager.Instance.Logger.Log(Logger.SeverityLevel.Warning, "SQLiteScriptTask: DbFile is empty, task is a noop");
				return;
			}

			using (SqliteConnection connection = new SqliteConnection (_connectionString)) {
				try {
					connection.Open();
					var transaction = connection.BeginTransaction ();
					var commands = File.ReadAllLines (_tempScript);
					using (SqliteCommand sqlCommand = new SqliteCommand (connection)) {
						sqlCommand.Transaction = transaction;
							sqlCommand.CommandTimeout = 5000;
						try {
							ExecutionStatus = TaskExecutionStatus.Pending;
							foreach (var commandText in commands) {
								sqlCommand.CommandText = commandText;
								sqlCommand.ExecuteNonQuery();
							}
							connection.Commit ();
						} catch (SqliteException ex) {
							UpdateManager.Instance.Logger.Log (Logger.SeverityLevel.Error, "SQLiteScriptTask: Execute; Commands execution failed with code {0}. Command: {1}", ex.ErrorCode, sqlCommand.CommandText);
							connection.RollBack ();
							return TaskExecutionStatus.Failed;
						} finally {
							connection.Close ();
						}

					}
				} catch (Exception ex) {
					UpdateManager.Instance.Logger.Log(Logger.SeverityLevel.Error,"SQLiteScriptTask: Failed to open sqlite database {0}", _dbFile);
					return TaskExecutionStatus.Failed;
				}
			}
			return TaskExecutionStatus.Successful;
		}

		public override bool Rollback ()
		{
			return true;
		}

		bool tryToOpenDatabase (string connectionString)
		{
			SqliteConnection c = new SqliteConnection(connectionString);
			try {
				c.Open();
				c.Close();
			} catch (Exception ex) {
				return false;
			}
			return true;

		}
	}
}

