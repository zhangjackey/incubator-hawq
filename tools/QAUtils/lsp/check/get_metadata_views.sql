SELECT 'SELECT COUNT(*) FROM "' || schemaname || '"."' || viewname || '";' AS sql
FROM pg_catalog.pg_views
WHERE schemaname IN ('pg_catalog')
ORDER BY viewname;
