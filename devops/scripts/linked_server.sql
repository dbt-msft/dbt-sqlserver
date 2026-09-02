-- Loopback linked server for the OPENQUERY tests (tests/functional/adapter/
-- mssql/test_openquery.py), run at instance bootstrap by init_db.sh. This is
-- the only thing that creates it: the tests read it and skip when it is
-- absent, they never provision it.
--
-- That is the point of the file. A linked server is instance-wide, so a test
-- fixture that created and dropped its own raced every other caller on the
-- instance - whoever dropped last pulled the server out from under a run
-- still using it (Msg 7202). Setting it up once, with the instance, is what
-- removes the shared state from the tests entirely.
--
-- It is separate from init.sql because it configures the instance rather than
-- TestDB's users, and because the tests name it when they skip.
--
-- Two settings vary by engine version, keyed off the running instance so one
-- script serves every server-<version> image:
--
--   Provider: MSOLEDBSQL only became a valid linked-server provider on Linux
--   in 2019; 2017 (major 14) rejects it with Msg 7222 and needs SQLNCLI. SQL
--   Server 2017 leaves extended support on 2027-10-12; drop that branch along
--   with the 2017 CI leg after that date.
--
--   Cert trust: from 2025 (major 17) the provider negotiates encryption by
--   default and the loopback presents the instance's self-signed certificate,
--   so the engine's outbound handshake fails ("SSL Provider: The handle
--   specified is invalid") without it. Sent only where needed, so 2019 and
--   2022 configure identically.
--
-- @useself = 'true' maps the local login to the same-named remote login, so no
-- password is embedded here. The IF NOT EXISTS guard is load-bearing rather
-- than decorative: init_db.sh re-runs this file until it exits 0.
IF NOT EXISTS (SELECT 1 FROM sys.servers WHERE name = 'LOCALLOOP')
BEGIN
    DECLARE @major int = CAST(SERVERPROPERTY('ProductMajorVersion') AS int);
    DECLARE @provider sysname =
        CASE WHEN @major <= 14 THEN 'SQLNCLI' ELSE 'MSOLEDBSQL' END;
    DECLARE @provstr nvarchar(4000) =
        CASE WHEN @major >= 17 THEN 'TrustServerCertificate=Yes' END;

    EXEC sp_addlinkedserver
        @server = 'LOCALLOOP',
        @srvproduct = '',
        @provider = @provider,
        @provstr = @provstr,
        @datasrc = '127.0.0.1,1433';

    EXEC sp_addlinkedsrvlogin
        @rmtsrvname = 'LOCALLOOP',
        @useself = 'true',
        @locallogin = NULL,
        @rmtuser = NULL,
        @rmtpassword = NULL;

    EXEC sp_serveroption 'LOCALLOOP', 'rpc out', true;
END
