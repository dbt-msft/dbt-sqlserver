from dbt.tests.adapter.query_comment.test_query_comment import (
    BaseEmptyQueryComments,
    BaseMacroArgsQueryComments,
    BaseMacroInvalidQueryComments,
    BaseMacroQueryComments,
    BaseNullQueryComments,
    BaseQueryComments,
)


class TestQueryCommentsFabric(BaseQueryComments):
    pass


class TestMacroQueryCommentsFabric(BaseMacroQueryComments):
    pass


class TestMacroArgsQueryCommentsFabric(BaseMacroArgsQueryComments):
    pass


class TestMacroInvalidQueryCommentsFabric(BaseMacroInvalidQueryComments):
    pass


class TestNullQueryCommentsFabric(BaseNullQueryComments):
    pass


class TestEmptyQueryCommentsFabric(BaseEmptyQueryComments):
    pass
