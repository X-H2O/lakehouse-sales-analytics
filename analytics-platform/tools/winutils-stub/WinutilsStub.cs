using System;

public static class WinutilsStub
{
    public static int Main(string[] args)
    {
        if (args.Length == 0)
        {
            return 0;
        }

        var command = args[0].ToLowerInvariant();
        switch (command)
        {
            case "chmod":
            case "chown":
            case "groups":
            case "ls":
                return 0;
            default:
                Console.Error.WriteLine("winutils stub ignored command: " + command);
                return 0;
        }
    }
}
