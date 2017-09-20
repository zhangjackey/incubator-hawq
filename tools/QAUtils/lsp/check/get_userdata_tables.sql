SELECT 'SELECT COUNT(*) FROM "' || schemaname || '"."' || tablename || '";' AS sql
FROM pg_catalog.pg_tables
WHERE schemaname NOT IN ('hawq_toolkit', 'pg_catalog', 'information_schema') AND
      tablename IN (
          SELECT relname
          FROM pg_catalog.pg_class
          WHERE relstorage NOT IN ('x') AND relkind ='r' AND relname not like '%prt_p%')
ORDER BY tablename;
