from dbt.tests.adapter.query_comment.test_query_comment import (
    BaseEmptyQueryComments,
    BaseMacroArgsQueryComments,
    BaseMacroInvalidQueryComments,
    BaseMacroQueryComments,
    BaseNullQueryComments,
    BaseQueryComments,
)


class TestQueryCommentsSQLServer(BaseQueryComments):
    pass


class TestMacroQueryCommentsSQLServer(BaseMacroQueryComments):
    pass


class TestMacroArgsQueryCommentsSQLServer(BaseMacroArgsQueryComments):
    pass


class TestMacroInvalidQueryCommentsSQLServer(BaseMacroInvalidQueryComments):
    pass


class TestNullQueryCommentsSQLServer(BaseNullQueryComments):
    pass


class TestEmptyQueryCommentsSQLServer(BaseEmptyQueryComments):
    pass
