SELECT 'SELECT COUNT(*) FROM "' || schemaname || '"."' || tablename || '";' AS sql
FROM pg_catalog.pg_tables
WHERE schemaname IN ('pg_catalog')
ORDER BY tablename;
