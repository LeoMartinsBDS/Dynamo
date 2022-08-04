using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Microsoft.AspNetCore.Mvc;
using System.Text.Json;

namespace MovieManager.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class MovieController : ControllerBase
    {
        private readonly IDynamoDBContext _context;
        private readonly IAmazonDynamoDB _db;
        public MovieController(IDynamoDBContext context, IAmazonDynamoDB db)
        {
            _context = context;
            _db = db;
        }


        [HttpPost("/createTable")]
        public async Task<IActionResult> CreateTable()
        {
            string tableName = "Movies";

            Console.WriteLine("Getting list of tables");
            var currentTables = await _db.ListTablesAsync();

            var listCurrentTable = currentTables.TableNames;

            Console.WriteLine("Number of tables: " + listCurrentTable.Count);
            if (!listCurrentTable.Contains(tableName))
            {
                try
                {
                    var request = new CreateTableRequest
                    {
                        TableName = tableName,
                        AttributeDefinitions = new List<AttributeDefinition>
                        {
                            new AttributeDefinition
                            {
                                AttributeName = "year",
                                AttributeType = ScalarAttributeType.N
                            },
                            new AttributeDefinition
                            {
                                AttributeName = "title",
                                AttributeType = ScalarAttributeType.S
                            }
                        },
                        KeySchema = new List<KeySchemaElement>
                        {
                             new KeySchemaElement
                            {
                              AttributeName = "year",
                              KeyType = KeyType.HASH
                            },
                            new KeySchemaElement
                            {
                              AttributeName = "title",
                              KeyType = KeyType.RANGE
                            }
                        },
                        BillingMode = BillingMode.PROVISIONED,
                        ProvisionedThroughput = new ProvisionedThroughput
                        {
                            ReadCapacityUnits = 10,
                            WriteCapacityUnits = 10
                        }

                    };

                    await _db.CreateTableAsync(request);

                    return Ok();
                }
                catch (Exception)
                {
                    return BadRequest();
                }
            }

            return NotFound();
        }

        [HttpPost("/loadData")]
        public async Task<IActionResult> LoadData()
        {
            var options = new JsonDocumentOptions
            {
                AllowTrailingCommas = true,
                CommentHandling = JsonCommentHandling.Skip
            };

            var sr = new FileStream(@"moviedata.txt", FileMode.Open, FileAccess.Read);
            var jsonDocument = JsonDocument.Parse(sr, options);
            var table = Table.LoadTable(_db, "Movies");
            foreach (JsonElement je in jsonDocument.RootElement.EnumerateArray())
            {
                var item = new Document();
                foreach (JsonProperty je2 in je.EnumerateObject())
                {
                    if (je2.Name == "year")
                    {
                        item[je2.Name] = je2.Value.GetInt32();
                    }
                    else
                    {
                        item[je2.Name] = je2.Value.ToString();
                    }

                }

                await table.PutItemAsync(item);
            }

            return Ok();
        }

        [HttpGet("{year}")]
        public async Task<IActionResult> GetAllMoviesFromYear(int year)
        {
            var movies = await _db.QueryAsync(new QueryRequest
            {
                TableName = "Movies",
                ExpressionAttributeNames = new Dictionary<string, string>
                {
                    { "#yr","year" }
                },
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    {":qYr", new AttributeValue {N = year.ToString()} }
                },
                KeyConditionExpression = "#yr = :qYr",
                ProjectionExpression = "#yr, title"
            });

            return Ok(movies);
        }

        [HttpGet("{from}/{to}")]
        public async Task<IActionResult> ScanMovies(int from, int to)
        {
            var movies = await _db.ScanAsync(new ScanRequest
            {
                TableName = "Movies",
                ExpressionAttributeNames = new Dictionary<string, string>
                {
                    { "#yr", "year" }
                },
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    { ":yFromYear", new AttributeValue { N = from.ToString() } },
                    { ":yToYear", new AttributeValue { N = to.ToString() } },
                },
                FilterExpression = "#yr between :yFromYear and :yToYear",
                ProjectionExpression = "#yr, title"
            });

            return Ok(movies);
        }

    }
}
