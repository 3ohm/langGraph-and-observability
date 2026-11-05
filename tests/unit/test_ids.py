from langgraph_and_observability.ids import new_id


def test_uuid7_time_ordered():
    ids = [new_id() for _ in range(200)]
    assert all(i.version == 7 for i in ids) and ids == sorted(ids)
    assert len(set(ids)) == len(ids)
