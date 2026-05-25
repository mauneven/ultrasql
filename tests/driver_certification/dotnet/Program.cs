using Npgsql;

static void Fail(string context, string message)
{
    throw new InvalidOperationException($"{context}: {message}");
}

static void AssertRows(string context, List<(int Id, string Text)> actual, params (int Id, string Text)[] expected)
{
    if (actual.Count != expected.Length)
    {
        Fail(context, $"expected {expected.Length} rows, got {actual.Count}");
    }

    for (var i = 0; i < expected.Length; i++)
    {
        if (actual[i] != expected[i])
        {
            Fail(context, $"row {i} expected {expected[i]}, got {actual[i]}");
        }
    }
}

static async Task<List<(int Id, string Text)>> ReadRows(NpgsqlCommand command)
{
    var rows = new List<(int Id, string Text)>();
    await using var reader = await command.ExecuteReaderAsync();
    while (await reader.ReadAsync())
    {
        rows.Add((reader.GetInt32(0), reader.GetString(1)));
    }

    return rows;
}

if (args.Length != 1)
{
    Console.Error.WriteLine("usage: dotnet run -- DSN");
    return 2;
}

var dsn = args[0];
await using var conn = new NpgsqlConnection(dsn);
await conn.OpenAsync();

await using (var command = new NpgsqlCommand("SELECT id, name FROM users WHERE id = @id", conn))
{
    command.Parameters.AddWithValue("id", 2);
    var rows = await ReadRows(command);
    AssertRows("Npgsql parameterized SELECT", rows, (2, "Grace"));
}

await using (var command = new NpgsqlCommand("CREATE TABLE npgsql_cert (id INT NOT NULL, label TEXT)", conn))
{
    await command.ExecuteNonQueryAsync();
}

await using (var command = new NpgsqlCommand("INSERT INTO npgsql_cert VALUES (@id, @label)", conn))
{
    var id = command.Parameters.Add("id", NpgsqlTypes.NpgsqlDbType.Integer);
    var label = command.Parameters.Add("label", NpgsqlTypes.NpgsqlDbType.Text);
    id.Value = 1;
    label.Value = "alpha";
    await command.ExecuteNonQueryAsync();
    id.Value = 2;
    label.Value = "beta";
    await command.ExecuteNonQueryAsync();
}

await using (var command = new NpgsqlCommand("SELECT id, label FROM npgsql_cert ORDER BY id", conn))
{
    var rows = await ReadRows(command);
    AssertRows("Npgsql parameterized INSERT", rows, (1, "alpha"), (2, "beta"));
}

await using (var tx = await conn.BeginTransactionAsync())
{
    await using var command = new NpgsqlCommand("INSERT INTO npgsql_cert VALUES (@id, @label)", conn, tx);
    command.Parameters.AddWithValue("id", 3);
    command.Parameters.AddWithValue("label", "rollback");
    await command.ExecuteNonQueryAsync();
    await tx.RollbackAsync();
}

await using (var command = new NpgsqlCommand("SELECT COUNT(*) FROM npgsql_cert", conn))
{
    var count = Convert.ToInt32(await command.ExecuteScalarAsync());
    if (count != 2)
    {
        Fail("Npgsql explicit transaction rollback", $"expected 2, got {count}");
    }
}

await using (var tx = await conn.BeginTransactionAsync())
{
    try
    {
        await using var command = new NpgsqlCommand("SELECT missing_column FROM npgsql_cert", conn, tx);
        await command.ExecuteNonQueryAsync();
        Fail("Npgsql failed transaction", "expected missing-column failure");
    }
    catch (PostgresException)
    {
        await tx.RollbackAsync();
    }
}

await using (var command = new NpgsqlCommand("SELECT id, label FROM npgsql_cert ORDER BY id", conn))
{
    var rows = await ReadRows(command);
    AssertRows("Npgsql recovery after error", rows, (1, "alpha"), (2, "beta"));
}

return 0;
