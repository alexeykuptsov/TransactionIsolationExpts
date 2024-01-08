using Npgsql;

namespace TransactionIsolationExpts;

public class Tests
{
    private int myWorkspaceId;
    
    [SetUp]
    public async Task SetUp()
    {
        await using var connection = new NpgsqlConnection(
            "Host=localhost;Port=5433;Database=transaction_isolation_expts;Username=transaction_isolation_expts_app;Password=transaction_isolation_expts_app");
        await connection.OpenAsync();

        await using var transaction = await connection.BeginTransactionAsync();
        await ExecuteReader(connection, "select value from key_int_value where key = 'next_workspace_id' for update", reader =>
        {
            reader.Read();
            myWorkspaceId = (int)reader["value"];
        });
        await ExecuteNonQuery(connection, $"update key_int_value set value = {myWorkspaceId + 1} where key = 'next_workspace_id'");
        await transaction.CommitAsync();
        
        await using var transaction2 = await connection.BeginTransactionAsync();
        await ExecuteNonQuery(connection, $"insert into balance(workspace_id, name, money) values ({myWorkspaceId}, 'Alice', 1000)");
        await ExecuteNonQuery(connection, $"insert into balance(workspace_id, name, money) values ({myWorkspaceId}, 'Bob', 0)");
        await transaction2.CommitAsync();
    }
    
    [Test]
    public async Task SingleTransaction()
    {
        await using var connection = new NpgsqlConnection(
            "Host=localhost;Port=5433;Database=transaction_isolation_expts;Username=transaction_isolation_expts_app;Password=transaction_isolation_expts_app");
        await connection.OpenAsync();

        await using var transaction = await connection.BeginTransactionAsync();
        await ExecuteNonQuery(connection, $"update balance set money = money - 10 where name = 'Alice' and workspace_id = {myWorkspaceId}");
        await ExecuteNonQuery(connection, $"update balance set money = money + 10 where name = 'Bob' and workspace_id = {myWorkspaceId}");
        await transaction.CommitAsync();
        
        var balances = await SelectFromBalances();
        Assert.That(balances, Is.EquivalentTo(new [] {(name: "Alice", money: 990m), (name: "Bob", money: 10m)}));
    }
    
    [Test]
    public async Task LostUpdate()
    {
        async Task Action()
        {
            await using var connection = new NpgsqlConnection(
                "Host=localhost;Port=5433;Database=transaction_isolation_expts;Username=transaction_isolation_expts_app;Password=transaction_isolation_expts_app");
            await connection.OpenAsync();
            
            await using var transaction = await connection.BeginTransactionAsync();
            decimal? aliceMoney = null;
            await ExecuteReader(connection, $"select money from balance where name = 'Alice' and workspace_id = {myWorkspaceId}", reader =>
            {
                reader.Read();
                aliceMoney = (decimal)reader["money"];
            });
            await ExecuteNonQuery(connection, $"update balance set money = {aliceMoney + 10} where name = 'Alice' and workspace_id = {myWorkspaceId}");
            await transaction.CommitAsync();
        };

        var tasks = new Task[30];

        for (int i = 0; i < tasks.Length; i++)
        {
            tasks[i] = Action();
        }

        await Task.WhenAll(tasks);

        var balances = await SelectFromBalances();
        Assert.That(balances, Is.EquivalentTo(new [] {(name: "Alice", money: 1300m), (name: "Bob", money: 0m)}));
    }
    
    [Test]
    public async Task LostUpdate_Fixed()
    {
        async Task Action()
        {
            await using var connection = new NpgsqlConnection(
                "Host=localhost;Port=5433;Database=transaction_isolation_expts;Username=transaction_isolation_expts_app;Password=transaction_isolation_expts_app");
            await connection.OpenAsync();
            
            await using var transaction = await connection.BeginTransactionAsync();
            decimal? aliceMoney = null;
            await ExecuteReader(connection, $"select money from balance where name = 'Alice' and workspace_id = {myWorkspaceId} for update", reader =>
            {
                reader.Read();
                aliceMoney = (decimal)reader["money"];
            });
            await ExecuteNonQuery(connection, $"update balance set money = {aliceMoney + 10} where name = 'Alice' and workspace_id = {myWorkspaceId}");
            await transaction.CommitAsync();
        };

        var tasks = new Task[30];

        for (int i = 0; i < tasks.Length; i++)
        {
            tasks[i] = Action();
        }

        await Task.WhenAll(tasks);

        var balances = await SelectFromBalances();
        Assert.That(balances, Is.EquivalentTo(new [] {(name: "Alice", money: 1300m), (name: "Bob", money: 0m)}));
    }

    private async Task<List<(string name, decimal money)>> SelectFromBalances()
    {
        await using var connection = new NpgsqlConnection(
            "Host=localhost;Port=5433;Database=transaction_isolation_expts;Username=transaction_isolation_expts_app;Password=transaction_isolation_expts_app");
        await connection.OpenAsync();
        
        var balances = new List<(string name, decimal money)>();
        await ExecuteReader(connection, $"select name, money from balance where workspace_id = {myWorkspaceId}", reader =>
        {
            while (reader.Read())
            {
                balances.Add(((string)reader["name"], (decimal)reader["money"]));
            }
        });
        return balances;
    }

    private static async Task ExecuteReader(NpgsqlConnection connection, string commandText, Action<NpgsqlDataReader> action)
    {
        await using var command3 = connection.CreateCommand();
        command3.CommandText = commandText;
        await using var reader = await command3.ExecuteReaderAsync();
        action(reader);
    }

    private static async Task ExecuteNonQuery(NpgsqlConnection connection, string commandText)
    {
        await using var command1 = connection.CreateCommand();
        command1.CommandText = commandText;
        await command1.ExecuteNonQueryAsync();
    }
}