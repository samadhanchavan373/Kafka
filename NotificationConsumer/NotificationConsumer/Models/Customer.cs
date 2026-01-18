using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NotificationConsumer1.Models
{
    public class Customer
    {
        public int Id { get; set; } // will be identity in DB
        public string Name { get; set; }
        public string Email { get; set; }
        public string Gender { get; set; }
    }
}
