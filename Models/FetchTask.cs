using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace SqlFetcher.Models
{
    [Table("FetchTask")]
    public partial class FetchTask
    {
        public int Id { get; set; }
        [MaxLength(int.MaxValue)]
        public string Query { get; set; }
        [MaxLength(250)]
        public string OutputTable { get; set; }
        [MaxLength(500)]
        public string Status { get; set; }
        public DateTime? StartAt { get; set; }
        public DateTime? FinishAt { get; set; }

        [NotMapped]
        internal SqlFetcher SqlFetcher { get; set; }
    }
}
