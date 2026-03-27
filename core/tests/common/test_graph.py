from sensorkit.common.graph import StateElection


def test_election():
    graph = {"a": []}
    s = StateElection[str](graph)

    result = s.evaluate()
    assert result == {}

    s.vote(subject="a", vote=True)
    result = s.evaluate()
    assert result == {"a": True}

    s.vote(subject="a", vote=None)
    result = s.evaluate()
    assert result == {}

    # Downvote overrides multiple upvotes.
    s.vote(source="u1", subject="a", vote=True)
    s.vote(source="u2", subject="a", vote=True)
    s.vote(source="d1", subject="a", vote=False)

    result = s.evaluate()
    assert result == {"a": False}


def test_election_dependencies():
    graph = {"a": ["b"], "b": ["c"], "c": []}
    s = StateElection[str](graph)

    s.vote(subject="a", vote=True)
    result = s.evaluate()

    # Output must be in topological order.
    assert list(result.keys()) == ["c", "b", "a"]
    assert result == {"c": True, "b": True, "a": True}

    s.vote(subject="b", vote=False)

    result = s.evaluate()
    assert list(result.keys()) == ["b", "a"]
    assert result == {"b": False, "a": False}
