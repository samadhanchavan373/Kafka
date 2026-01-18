namespace CustomerAPI.Models
{
    public class Customer
    {
        public int Id { get; set; } // will be identity in DB
        public string Name { get; set; }
        public string Email { get; set; }
        public string Gender { get; set; }
    }
}
