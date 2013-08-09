using SqlFetcher.Models;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;

namespace SqlFetcher
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Фоновый исполнитель SQL запросов.");
            Console.WriteLine("Строки подключения настраиваются в файле connections.txt, строк может быть несколько.");
            Console.WriteLine("Специально для dr.Ace от dr.Basic 22.04.2013");

            ThreadPool.SetMaxThreads(100, 100);
            var finishEvent = new ManualResetEvent(false);
            var sqlFetchers = new List<SqlFetcher>();
            foreach (var connectionString in File.ReadLines(@"connections.txt"))
            {
                if (connectionString.StartsWith(@"//"))
                    continue;
                
                try
                {
                    var f = new SqlFetcher(connectionString, finishEvent);
                    sqlFetchers.Add(f);
                    f.Run();
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                }
            }

            Console.ReadLine();
            Console.WriteLine("waiting for background threads...");
            finishEvent.Set();

            foreach (var sqlFetcher in sqlFetchers)
                sqlFetcher.DoneEvent.WaitOne();

        }
    }
}
