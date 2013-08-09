using SqlFetcher.Models;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading;


namespace SqlFetcher
{
    class SqlFetcher
    {
        private readonly string _connectionString;
        private readonly ManualResetEvent _finishEvent;
        private FetcherContext _mainContext;
        const int GetTaskTimout = 1000;

        public SqlFetcher(string connectionString, ManualResetEvent finishEvent)
        {
            this._connectionString = connectionString;
            this._finishEvent = finishEvent;
            DoneEvent = new ManualResetEvent(false);
        }

        public ManualResetEvent DoneEvent { get; private set;}
        public void Run()
        {
            ThreadPool.QueueUserWorkItem(InternalLoop, this);
        }

        private void ForceFetchTaskTable()
        {
            const string CreateTable = 
@"IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[FetchTask]') AND type in (N'U'))
CREATE TABLE [dbo].[FetchTask](
    [id] [int] IDENTITY(1,1) NOT NULL,
    [Query] [nvarchar](max) NOT NULL,
    [OutputTable] [nvarchar](50) NULL,
    [Status] [nvarchar](500) NULL,
    [StartAt] [datetime] NULL,
    [FinishAt] [datetime] NULL
)";
            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = CreateTable;
                    command.ExecuteNonQuery();
                }
            }
        }

        private static void InternalLoop(Object obj)
        {
            var fetcher = (SqlFetcher)obj;

            //цикл получения и обработки задач
            try
            {
                Console.WriteLine("Start {0}", fetcher._connectionString);
                while (true)
                {
                    try
                    {
                        fetcher.Execute();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.ToString());
                    }
                    if (fetcher._finishEvent.WaitOne(GetTaskTimout))
                        break;
                }
            }
            finally 
            {
                Console.WriteLine("Finished {0}", fetcher._connectionString);
                fetcher.DoneEvent.Set();
            }
        }

        private void Execute()
        {
            if (_mainContext == null)
            {
                ForceFetchTaskTable();
                _mainContext = new FetcherContext(_connectionString);
            }

            try
            {
                var tasksQuery =
                    from task in _mainContext.FetchTasks.AsNoTracking()
                    where task.Status.Equals(null)
                    select task;

                var tasks = tasksQuery.ToList();
                foreach (var t in tasks)
                {
                    t.SqlFetcher = this;
                    ThreadPool.QueueUserWorkItem(ProcessTaskSink, t);
                }
            }
            catch(Exception)
            {
                if (_mainContext != null)
                {
                    _mainContext.Dispose();
                   _mainContext = null;
                }
                throw;
            }
        }

        private static void ProcessTaskSink(object obj)
        {
            var fetchTask = (FetchTask)obj;
            fetchTask.SqlFetcher.ProcessTask(fetchTask);
        }

        private void ProcessTask(FetchTask fetchTask)
        {
            Console.WriteLine("{2}>{0}->{1}", fetchTask.Query, fetchTask.OutputTable, DateTime.Now);

            using (var context = new FetcherContext(_connectionString))
            {
                var dbTask =
                    (
                    from task in context.FetchTasks
                    where task.Id == fetchTask.Id
                    select task
                    ).Single();
                dbTask.StartAt = DateTime.Now;
                dbTask.Status = "run";
                context.SaveChanges();
                try
                {
                    using (var connection = new SqlConnection(_connectionString))
                    {
                        connection.Open();

                        if (String.IsNullOrEmpty(fetchTask.OutputTable))
                            ProcessNonQuery(connection, fetchTask);
                        else
                            ProcessQuery(connection, fetchTask);
                    }
                    dbTask.Status = "done";
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                    dbTask.Status = ex.Message;
                }
                finally
                {
                    dbTask.FinishAt = DateTime.Now;
                    context.SaveChanges();
                }
            }
        }

        private void ProcessNonQuery(SqlConnection connection, FetchTask fetchTask)
        {
            var command = new SqlCommand(fetchTask.Query, connection);
            command.ExecuteNonQuery();
        }

        private void ProcessQuery(SqlConnection connection, FetchTask fetchTask)
        {
            var command = new SqlCommand(fetchTask.Query, connection);
            using (var reader = command.ExecuteReader())
            {
                using (var outConnection = new SqlConnection(_connectionString))
                {
                    outConnection.Open();

                    MakeOutputTable(outConnection, reader, fetchTask.OutputTable);
                    var insertCmd = MakeInsertQuery(outConnection, reader, fetchTask.OutputTable);
                    insertCmd.Transaction = outConnection.BeginTransaction();
                    while (reader.Read())
                    {
                        for (int i = 0, n = reader.FieldCount; i < n; ++i)
                        {
                            insertCmd.Parameters[i].Value = reader[i];
                        }
                        insertCmd.ExecuteNonQuery();
                    }
                    insertCmd.Transaction.Commit();
                    reader.Close();
                }
            }
        }

        private static void MakeOutputTable(SqlConnection connection, SqlDataReader reader, string tableName)
        {
            var table = reader.GetSchemaTable();
            var queryBuilder = new StringBuilder();
            queryBuilder.AppendFormat("CREATE TABLE {0} (", tableName);
            int i = 0;
            foreach (DataRow row in table.Rows)
            {
                var columnName = row["ColumnName"];
                var dataType = row["DataType"];
                var columnSize = row["ColumnSize"];
                var allowDbNull = row["AllowDbNull"];
                var sqlDbType = (SqlDbType)(int)row["ProviderType"];
                string dbType = sqlDbType.ToString();

                switch (sqlDbType)
                {
                    case SqlDbType.Binary:
                    case SqlDbType.Char:
                    case SqlDbType.NChar:
                    case SqlDbType.NVarChar:
                    case SqlDbType.VarBinary:
                    case SqlDbType.VarChar:
                        dbType += string.Format(
                            "({0})",
                            ((int)columnSize == 2147483647) ? "MAX" : string.Format("{0}", columnSize) 
                            );
                        break;
                }

                queryBuilder.AppendFormat(
                    "{0}\n[{1}] {2} {3}",
                    i == 0 ? "" : ",",
                    columnName,
                    dbType,
                    allowDbNull.Equals(true) ? "NULL" : "NOT NULL"
                    );
                ++i;
            }
            queryBuilder.AppendFormat("\n)");

            var dropSql = string.Format("drop table {0}", tableName);
            try
            {
                using (var command = new SqlCommand(dropSql, connection))
                {
                    //Console.WriteLine(dropSql);
                    command.ExecuteNonQuery();
                }
            }
            catch (SqlException)
            { }

            var createSql = queryBuilder.ToString();
            //Console.WriteLine(createSql);
            using (var command = new SqlCommand(createSql, connection))
            {
                command.ExecuteNonQuery();
            }

        }

        private SqlCommand MakeInsertQuery(SqlConnection connection, SqlDataReader src, string tableName)
        {
            var fields1 = new StringBuilder();
            var fields2 = new StringBuilder();
            for (int i = 0; i < src.FieldCount; i++)
            {
                fields1.AppendFormat("{0}[{1}]", (i == 0) ? "" : ", ", src.GetName(i));
                fields2.AppendFormat("{0}@{1}", (i == 0) ? "" : ", ", i);
            }
            var query = String.Format("insert into {0} ({1}) values ({2})", tableName, fields1, fields2);
            //Console.WriteLine(query);
            var result = new SqlCommand(query, connection);
            var schema = src.GetSchemaTable();
            for (var i = 0; i < src.FieldCount; i++)
            {
                var dbType = (SqlDbType)(int)schema.Rows[i]["ProviderType"];
                result.Parameters.Add(string.Format("@{0}", i), dbType);
            }

            return result;
        }
    }
}
