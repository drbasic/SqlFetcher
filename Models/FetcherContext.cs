using System.Data.Entity;
using System.Data.Entity.Infrastructure;

namespace SqlFetcher.Models
{
    public partial class FetcherContext : DbContext
    {
        static FetcherContext()
        {
            //Database.SetInitializer<FetcherContext>(new CreateDatabaseIfNotExists<FetcherContext>());
            Database.SetInitializer<FetcherContext>(null);
        }

        public FetcherContext(string nameOrConnectionString)
            : base(nameOrConnectionString)
        {
        }

        public DbSet<FetchTask> FetchTasks { get; set; }
    }
}
