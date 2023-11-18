from dbt.tests.adapter.persist_docs.test_persist_docs import (
    BasePersistDocs,
    BasePersistDocsColumnMissing,
    BasePersistDocsCommentOnQuotedColumn,
)


class TestPersistDocsSQLServer(BasePersistDocs):
    pass


class TestPersistDocsColumnMissingSQLServer(BasePersistDocsColumnMissing):
    pass


class TestPersistDocsCommentOnQuotedColumnSQLServer(
    BasePersistDocsCommentOnQuotedColumn
):
    pass
