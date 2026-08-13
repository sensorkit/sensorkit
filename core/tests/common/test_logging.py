# SPDX-License-Identifier: Apache-2.0
import inspect
import itertools

from loguru import logger

from sensorkit.common.logging import NULL_LOGGER, limited_logger

subjects = (f"test-subject-{i}" for i in itertools.count())


def test_first_call_at_a_site_emits():
    assert limited_logger(interval=60.0) is logger


def test_repeats_at_one_site_are_discarded():
    results = [limited_logger(interval=60.0) for _ in range(3)]

    assert results == [logger, NULL_LOGGER, NULL_LOGGER]


def test_elapsed_window_emits_again():
    results = [limited_logger(interval=0.0) for _ in range(2)]

    assert results == [logger, logger]


def test_adjacent_sites_are_limited_independently():
    assert limited_logger(interval=60.0) is logger
    assert limited_logger(interval=60.0) is logger


def test_subjects_at_one_site_are_limited_independently():
    first, second = next(subjects), next(subjects)

    results = [limited_logger(s, interval=60.0) for s in (first, second, first)]

    assert results == [logger, logger, NULL_LOGGER]


def test_one_subject_is_limited_across_sites_independently():
    subject = next(subjects)

    assert limited_logger(subject, interval=60.0) is logger
    assert limited_logger(subject, interval=60.0) is logger


def test_a_subject_does_not_share_the_window_of_its_bare_site():
    subject = next(subjects)

    results = [limited_logger(s, interval=60.0) for s in (None, subject, None)]

    assert results == [logger, logger, NULL_LOGGER]


def test_interval_is_read_on_every_call():
    subject = next(subjects)

    results = [limited_logger(subject, interval=i) for i in (0.0, 60.0, 60.0)]

    assert results == [logger, logger, NULL_LOGGER]


def test_missing_frame_support_emits_rather_than_sharing_one_key(monkeypatch):
    monkeypatch.setattr(inspect, "currentframe", lambda: None)

    results = [limited_logger(interval=60.0) for _ in range(2)]

    assert results == [logger, logger]


def test_discarded_logger_accepts_chained_calls():
    discarded = [limited_logger(interval=60.0) for _ in range(2)][1]

    assert discarded is NULL_LOGGER

    discarded.opt(exception=ValueError("boom")).warning("{x}", x=1)
    discarded.bind(a=1).info("ignored")
